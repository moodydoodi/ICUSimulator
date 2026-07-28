"""SimulationEngine — replays a curated patient timeline as a live stream.

Tick loop (every TICK_S wall-clock seconds):
  - ECG chunk  — 50 samples @ 500 Hz  ->  10 messages/sec
  - Vitals     — once per simulated second
  - Drug events — on change only
  - Alarms     — on change only (only if config.stream.emit_alarms is true)

The engine is transport-agnostic: it puts dicts into `broadcast_queue`.
The server picks them up and forwards to WebSocket clients + JSONL log.
"""

import asyncio
import logging
from pathlib import Path

from .clock import SimClock
from .ecg_player import ECGPlayer
from .timeline_source import TimelineSource

log = logging.getLogger(__name__)

TICK_S          = 0.1     # wall-clock seconds between ticks
ECG_CHUNK_N     = 50      # samples per ECG message  (50 @ 500 Hz = 100 ms)
VITALS_EVERY_S  = 1.0     # emit vitals every N simulated seconds


class SimulationEngine:

    LEAD_ORDER = ["I", "II", "III", "aVR", "aVL", "aVF",
                  "V1", "V2", "V3", "V4", "V5", "V6"]

    def __init__(self, config: dict, broadcast_queue: asyncio.Queue):
        self.config          = config
        self.broadcast_queue = broadcast_queue
        self.project_root    = Path(config["project_root"])
        self.timeline_dir    = Path(config["timeline_dir"])
        self.ecg_index_dir   = Path(config["ecg_index_dir"])
        self.patients        = config["patients"]   # list[{subject_id, label}]

        # Mutable state
        self.current_idx: int   = 0
        self.speed: float       = 1.0
        self.paused: bool       = False
        self.loop: bool         = True   # loop at end of stay

        self._clock   = SimClock(speed=1.0)
        self._player  = ECGPlayer()
        self._source  = None

        # Change-detection state
        self._last_vitals_t: float = -999.0
        self._last_ecg_file        = None
        self._last_drugs: list     = []
        self._last_alarms: dict    = {}
        self._pending_seek: bool   = False

    # -- Control API (called from REST endpoints) ------------------------------

    def switch_patient(self, idx: int):
        self.current_idx = idx % len(self.patients)
        self._load_patient()

    def set_speed(self, speed: float):
        self.speed = max(0.25, min(300.0, float(speed)))
        self._clock.speed = self.speed

    def event_summary(self):
        if self._source:
            return self._source.event_summary()
        return {"duration_hours": 0, "alarms": [], "onsets": [], "ecgs": []}

    def current_ecg_12lead(self):
        import numpy as np
        if self._source is None:
            return None
        hours = self._clock.sim_time / 3600.0
        rel = self._source.all12_rel_at(hours)
        if not rel:
            return None
        rel = rel.replace(".0_all12.npy", "_all12.npy")
        waveform_dir = Path(self.config["waveform_dir"])
        p = waveform_dir.parent / rel
        if not p.exists():
            p = self.project_root / rel
        if not p.exists():
            return None
        try:
            arr = np.load(str(p))
        except Exception:
            return None
        if arr.ndim != 2:
            return None
        n = arr.shape[0]
        names = self.LEAD_ORDER if n == 12 else [f"lead_{i}" for i in range(n)]
        ds = 2   # downsample 500 -> 250 Hz to keep the payload small
        leads = {names[i]: [round(float(x), 3) for x in arr[i, ::ds]] for i in range(n)}
        meta = self._source.ecg_meta_at(hours)
        return {"order": names, "leads": leads, "fs": 500 // ds,
                "study_id": meta["study_id"], "rhythm": meta["rhythm"],
                "hr": meta["hr"], "qtc": meta["qtc"]}

    def _drug_class(self, name: str) -> str:
        """Map a drug name to a class from config.drug_classes (for the UI + SASI)."""
        n = (name or "").lower()
        for cls, pats in self.config.get("drug_classes", {}).items():
            if any(p.lower() in n for p in pats):
                return cls
        return "other"

    # -- Internal --------------------------------------------------------------

    def _load_patient(self):
        p   = self.patients[self.current_idx]
        sid = p["subject_id"]
        log.info(f"Loading patient {sid}: {p['label']}")
        try:
            self._source = TimelineSource(sid, self.timeline_dir, self.ecg_index_dir, self.config)
        except FileNotFoundError as e:
            log.error(f"Timeline not found: {e}")
            self._source = None
        self._clock.reset()
        self._last_vitals_t  = -999.0
        self._last_ecg_file  = None
        self._last_drugs     = []
        self._last_alarms    = {}
        self._pending_seek   = False
        self._player._current_path = None   # force ECG reload

    async def _emit(self, msg: dict):
        await self.broadcast_queue.put(msg)

    def _resolve_ecg(self, rel):
        """Resolve the relative ecg_lead2_file path to an absolute path.
        Parquet stores 'ecg_waveforms/p{sid}_s{study}_lead2.npy'; files live at
        data/processed/ecg_waveforms/... (= waveform_dir.parent / rel)."""
        if not rel:
            return None
        rel = rel.replace(".0_lead2.npy", "_lead2.npy").replace(".0_all12.npy", "_all12.npy")
        waveform_dir = Path(self.config["waveform_dir"])
        p = waveform_dir.parent / rel
        if p.exists():
            return str(p)
        p2 = self.project_root / rel
        return str(p2) if p2.exists() else None

    def seek(self, hours: float):
        """Jump the sim clock to an absolute hour in the stay."""
        if self._source is None:
            return
        hours = max(0.0, min(float(hours), self._source.duration_hours))
        self._clock._sim_time = hours * 3600.0
        self._clock._last_wall = __import__("time").monotonic()
        self._last_vitals_t = -999.0     # force an immediate re-emit at the new time
        self._last_ecg_file = None
        self._pending_seek = True

    # -- Main loop -------------------------------------------------------------

    async def run(self):
        self._load_patient()
        p = self.patients[self.current_idx]
        await self._emit({
            "type": "session_start",
            "patient_id": p["subject_id"],
            "label": p["label"],
            "patients": self.patients,
            "schema_version": "1.0",
            "t": 0.0,
            "duration_hours": (self._source.duration_hours if self._source else 0.0),
            "speed": self.speed,
        })

        emit_alarms = self.config.get("stream", {}).get("emit_alarms", True)

        while True:
            if self.paused or self._source is None:
                await asyncio.sleep(TICK_S)
                continue

            sim_t = self._clock.tick()
            hours = sim_t / 3600.0

            if self._pending_seek:
                self._pending_seek = False
                await self._emit({"type": "seeked", "t": round(sim_t, 3)})

            # Loop at end of stay
            if hours > self._source.duration_hours:
                if self.loop:
                    self._clock.reset()
                    sim_t, hours = 0.0, 0.0
                else:
                    await asyncio.sleep(TICK_S)
                    continue

            # -- ECG chunk --------------------------------------------------
            ecg_file = self._source.ecg_file_at(hours)
            if ecg_file != self._last_ecg_file:
                hr = (self._source.vitals_at(hours).get("heart_rate") or 75.0)
                self._player.load(self._resolve_ecg(ecg_file), hr=hr)
                self._last_ecg_file = ecg_file

            chunk = self._player.get_chunk(ECG_CHUNK_N)
            await self._emit({
                "type": "ecg_chunk",
                "t": round(sim_t, 3),
                "fs": ECGPlayer.FS,
                "samples": chunk,
            })

            # -- Vitals (1 Hz) ----------------------------------------------
            if sim_t - self._last_vitals_t >= VITALS_EVERY_S:
                self._last_vitals_t = sim_t
                vitals = self._source.vitals_at(hours)
                labs   = self._source.labs_at(hours)
                rhythm = self._source.machine_rhythm_at(hours)
                p_meta = self.patients[self.current_idx]

                await self._emit({
                    "type": "vitals",
                    "patient_id": p_meta["subject_id"],
                    "t": round(sim_t, 3),
                    "hours_since_admission": round(hours, 4),
                    "ecg_rhythm": rhythm,
                    "ecg_study_id": self._source.ecg_meta_at(hours)["study_id"],
                    **vitals,
                    **{f"lab_{k}": v for k, v in labs.items()},
                })

                # Drug events on change (each tagged with its config class)
                drugs = self._source.drugs_at(hours)
                for d in drugs:
                    d["class"] = self._drug_class(d.get("drug", ""))
                if drugs != self._last_drugs:
                    await self._emit({
                        "type": "drug_event",
                        "patient_id": p_meta["subject_id"],
                        "t": round(sim_t, 3),
                        "active_drugs": drugs,
                    })
                    self._last_drugs = drugs

                # Alarm events on change — only if enabled in config
                if emit_alarms:
                    alarms = self._source.alarms_at(hours)
                    for alarm, active in alarms.items():
                        if active != self._last_alarms.get(alarm, False):
                            await self._emit({
                                "type": "alarm",
                                "patient_id": p_meta["subject_id"],
                                "t": round(sim_t, 3),
                                "condition": alarm,
                                "active": bool(active),
                            })
                    self._last_alarms = alarms

            await asyncio.sleep(TICK_S)
