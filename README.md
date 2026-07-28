# ICU Replay Simulator

<img width="1917" height="1012" alt="grafik" src="https://github.com/user-attachments/assets/368e1241-14e5-4eb0-bdc0-fd80584ab966" />



A configurable, multimodal MIMIC-IV replay simulator. It streams a curated ICU
patient's real vitals, labs, medications, and ECG (waveform + machine
measurements) as a live, time-controllable feed, for testing downstream systems
such as **SASI** (secure, accent-robust voice assistance / ICU safety layer). (https://www.uniklinik-duesseldorf.de/patienten-besucher/klinikeninstitutezentren/digital-health-lab-duesseldorf/projekte/sasi)

The simulator is a **data source**. It replays real MIMIC-IV
data and emits raw signals plus threshold-based alarm flags. Any clinical
interpretation is the job of the consuming system, not the simulator.

---

## 1. Quick start

```bash
# from the project root, with the venv active
python check_setup.py     # verifies data files + ECG path resolution
python run_sim.py          # reads ./config.json, serves http://127.0.0.1:8000
```

Open <http://127.0.0.1:8000> for the bedside monitor. The **12-Lead ECG** button
opens a full-screen clinical 12-lead view (`/ecg12`).

Requirements: `fastapi`, `uvicorn`, `pandas`, `numpy`, `pyarrow`. Empty
`src/__init__.py` and `src/engine/__init__.py` must exist.

---

## 2. Architecture

```
config.json ──► run_sim.py ──► src/server.py (FastAPI)
                                   │
                                   ├─ SimulationEngine (engine.py)  
                                   │     ├─ SimClock        (clock.py)          sim vs wall time, speed
                                   │     ├─ TimelineSource  (timeline_source.py) reads parquet, LOCF, alarms
                                   │     └─ ECGPlayer       (ecg_player.py)      real .npy waveform + fallback
                                   │
                                   ├─ WebSocket  ws://…/stream   ── live feed (browser AND SASI)
                                   └─ JSONL log  data/output/session_*.jsonl ── replayable record
```

Data is produced offline by four notebooks and consumed at runtime by the server:

| Notebook | Produces |
|---|---|
| `01_curate_candidates` | scored candidate cohort (ICU × ECG windowed match, cardiac filter) |
| `02_verify_and_select_finalists` | the final curated patient list |
| `03_build_timelines` | `data/processed/patient_timelines/patient_<subject_id>.parquet` (hourly-gridded wide timeline: vitals, labs, meds-as-JSON, ECG columns) |
| `04_extract_ecg_waveforms` | `ecg_waveforms/p<sid>_s<study>_lead2.npy` + `_all12.npy`, and `ecg_index/ecg_index_<sid>.json` |

The runtime never touches raw MIMIC, only these processed artifacts.

---

## 3. Configuration (`config.json`)

Everything tunable lives in one file. No rebuild is needed to change thresholds,
plausible ranges, drug classes, or the stream flags.

| Section | What it controls |
|---|---|
| `paths` | the four processed-data folders (relative to project root) |
| `patients` | which curated patients to load, and their display labels |
| `stream.emit_alarms` | **whether `alarm` messages are sent/logged at all** (see §5) |
| `stream.log_ecg_chunks` | whether high-frequency `ecg_chunk` messages are written to the JSONL log (default `false` to keep logs small; ECG still streams live over WebSocket) |
| `alarms` | each alarm = `{signal, op, threshold, critical}`. Computed **live** from the raw signal at runtime, so changing a threshold takes effect on restart with no rebuild |
| `markers` | scrubber marker behaviour: `critical_only`, `min_duration_hours`, `merge_gap_hours` (navigation only — never sent to SASI) |
| `plausible_ranges` | `[min, max]` per signal. Values outside are set to `NaN` (see §6) |
| `drug_classes` | class → list of name substrings; used to tag drugs (vasoactive, antiarrhythmic, fluid, …) |

---

## 4. How a client receives the data

The engine broadcasts to **all** connected WebSocket clients simultaneously. The browser monitor and the client can listen at the same time. Two integration modes:

**A. Live (recommended).** Open a WebSocket to `ws://127.0.0.1:8000/stream` and
read JSON messages:

```python
import asyncio, json, websockets

async def main():
    async with websockets.connect("ws://127.0.0.1:8000/stream") as ws:
        async for raw in ws:
            msg = json.loads(raw)
            if msg["type"] == "vitals":
                hr, mapv = msg.get("heart_rate"), msg.get("map")
                # ... SASI applies its own logic here ...
            elif msg["type"] == "alarm":
                print(msg["condition"], msg["active"])   # only if emit_alarms=true

asyncio.run(main())
```

**B. Replay (offline).** Read `data/output/session_<timestamp>.jsonl` line by
line. One JSON message per line, same schema. Good for reproducible tests.

### Message schema

| `type` | Key fields |
|---|---|
| `session_start` | `patient_id`, `label`, `patients[]`, `duration_hours`, `speed` |
| `vitals` (≈1 Hz sim) | `heart_rate`, `map`, `spo2`, `respiratory_rate`, `temperature`, `sbp`, `dbp`, `lab_*` (creatinine, glucose, lactate, troponin, potassium, sodium, hemoglobin, wbc), `ecg_rhythm`, `ecg_study_id`, `hours_since_admission`, `t` |
| `ecg_chunk` (10 Hz) | `samples` (50 Lead-II values), `fs` (500), `t` |
| `drug_event` (on change) | `active_drugs[]` = `{drug, rate, rate_unit? , class}` |
| `alarm` (on change) | `condition`, `active` — **omitted entirely if `emit_alarms=false`** |
| `seeked` | sent after a scrubber jump (UI uses it to clear the ECG canvas) |

REST control endpoints: `POST /control/patient/{idx}`, `/control/speed/{x}`,
`/control/pause`, `/control/seek/{hours}`; `GET /control/status`, `/patients`,
`/events` (scrubber markers), `/ecg/current12` (12-lead snapshot).

---

## 5. Sending alarms to the client. on or off

`stream.emit_alarms` decides whether the simulator emits `alarm` messages at all.

- `true` — the simulator sends its threshold-based `alarm` flags in the stream (and
  JSONL). Useful when you want the client to also see the "ground-truth" threshold events.
- `false` — **no `alarm` messages are sent or logged.** The client then receives only the
  raw vitals/labs/ECG and must derive everything itself. This is the honest test of
  whether the client detects deterioration on its own, without the simulator hinting.

The scrubber markers in the browser are unaffected by this flag, they are a
navigation aid for the human operator, computed separately, and never sent to the client.

---

## 6. Outliers and implausible values

Two different things, handled differently on purpose:

- **Physiologically plausible extremes** (MAP 45, HR 160, lactate 8) are **kept**.
  Real ICU streams contain them, and a safety layer must be tested against them.
- **Technically impossible values** (e.g. the `-30000 ms` PR interval MIMIC emits
  when no P-wave is detected, or a `0` from a dropped SpO₂ sensor) are set to
  `NaN` via `plausible_ranges`, because a real device shows "no reading", not a
  sentinel. This cleaning happens at load time, so no parquet rebuild is required.

Tune the bounds in `config.json → plausible_ranges`. Widen them to keep more, or
remove a key to disable cleaning for that signal.

---

## 7. Defining a new use case (another team, another cohort)

The simulator is not SASI-specific. To point it at a different cohort:

1. **Curate patients.** Run notebooks `01`–`02` with your inclusion criteria
   (care units, ECG availability, timeline length). Notebooks `00`/`01` also let
   you pick the MIMIC item-ids (vitals/labs/meds) for your domain.
2. **Build timelines.** Run `03` and `04` for your `subject_id`s. This writes the
   `patient_<sid>.parquet`, ECG `.npy` files, and `ecg_index_<sid>.json`.
3. **Edit `config.json`.** Add your patients to `patients`; adjust `alarms`,
   `plausible_ranges`, and `drug_classes` for your domain; set the stream flags.
4. **Run.** `python run_sim.py your_config.json` — no code changes.

Keep the timeline schema stable (the columns `timeline_source.py` reads: the
vitals/labs listed in §4, `active_medications` as a JSON string, `ecg_*` columns,
`hours_since_admission`, `timestamp`). New modalities can be added as extra
columns and surfaced by extending `timeline_source` + the client.

---

## 8. Known follow-ups

- The browser keeps a small display-only copy of `drug_classes` for chip colours;
  the authoritative class SASI receives comes from `config.json` via the engine.
- The full-screen 12-lead shows the study at open / on **↻ Refresh** (a 12-lead is
  a snapshot, not a live sweep). Seek in the monitor, then refresh.
- `surgery_phase` is not derived (all patients `non_surgical`); real phase
  segmentation needs an OR/surgery-time proxy and is out of scope for v0.1.

