"""TimelineSource — reads a patient timeline parquet/csv and serves values
at any simulated hour using last-observation-carried-forward (LOCF).

The timeline files are the output of notebook 03. Behaviour (alarm thresholds,
plausible ranges) is driven by the `config` dict passed in from run_sim.py, so
another team can retune it without touching code or rebuilding parquets.
"""

import json
import math
import pandas as pd
import numpy as np
from pathlib import Path


class TimelineSource:

    def __init__(self, subject_id: int, timeline_dir: Path, ecg_index_dir: Path,
                 config: dict | None = None):
        self.subject_id = subject_id
        config = config or {}
        self._alarm_specs = config.get("alarms", {})          # name -> {signal, op, threshold, critical}
        self._marker_cfg  = config.get("markers", {})
        self._plausible   = config.get("plausible_ranges", {})

        self.df = self._load(subject_id, timeline_dir)
        self.df = self.df.sort_values("hours_since_admission").reset_index(drop=True)
        self._apply_plausible_ranges()                        # drop technically-impossible values -> NaN
        self.duration_hours = float(self.df["hours_since_admission"].max())
        self._hours_arr = self.df["hours_since_admission"].to_numpy(dtype="float64")

        # Admission wall-time (for placing ECG index times on the hours axis)
        self.admission_time = None
        if "timestamp" in self.df.columns and len(self.df):
            self.admission_time = self.df["timestamp"].iloc[0] - pd.Timedelta(
                hours=float(self._hours_arr[0]))

        # ECG index
        ecg_path = ecg_index_dir / f"ecg_index_{subject_id}.json"
        self.ecg_index: list[dict] = []
        if ecg_path.exists():
            with open(ecg_path) as f:
                self.ecg_index = json.load(f)

    # -- Loaders ---------------------------------------------------------------

    @staticmethod
    def _load(sid: int, timeline_dir: Path) -> pd.DataFrame:
        for ext, reader in [(".parquet", pd.read_parquet), (".csv", pd.read_csv)]:
            p = timeline_dir / f"patient_{sid}{ext}"
            if p.exists():
                df = reader(p)
                if "timestamp" in df.columns:
                    df["timestamp"] = pd.to_datetime(df["timestamp"])
                return df
        raise FileNotFoundError(f"No timeline file for subject {sid} in {timeline_dir}")

    def _apply_plausible_ranges(self):
        """Set technically-impossible values (sentinels, dead sensors) to NaN.
        Physiologically plausible extremes are kept on purpose -- they are exactly
        the kind of real-world outlier a safety layer like SASI should be tested on."""
        for col, rng in self._plausible.items():
            if col in self.df.columns and isinstance(rng, (list, tuple)) and len(rng) == 2:
                lo, hi = rng
                s = pd.to_numeric(self.df[col], errors="coerce")
                self.df[col] = s.where((s >= lo) & (s <= hi))

    # -- Row lookup (LOCF) -----------------------------------------------------

    def _row_at(self, hours: float) -> pd.Series:
        idx = int(np.searchsorted(self._hours_arr, hours, side="right")) - 1
        idx = max(0, min(idx, len(self.df) - 1))
        return self.df.iloc[idx]

    # -- Public API ------------------------------------------------------------

    def vitals_at(self, hours: float) -> dict:
        row = self._row_at(hours)
        return {k: _f(row, k) for k in
                ("heart_rate", "map", "spo2", "respiratory_rate", "temperature", "sbp", "dbp")}

    def labs_at(self, hours: float) -> dict:
        row = self._row_at(hours)
        return {k: _f(row, k) for k in
                ("creatinine", "glucose", "lactate", "troponin",
                 "potassium", "sodium", "hemoglobin", "wbc")}

    def drugs_at(self, hours: float) -> list[dict]:
        row = self._row_at(hours)
        raw = row.get("active_medications", "[]")
        if not isinstance(raw, str) or not raw:
            return []
        try:
            return json.loads(raw) or []
        except Exception:
            return []

    def alarms_at(self, hours: float) -> dict:
        """Alarms computed live from config thresholds on the raw signals.
        Falls back to pre-baked alarm_* columns if no config is provided."""
        row = self._row_at(hours)
        if self._alarm_specs:
            return {name: _cmp(row.get(spec["signal"]), spec["op"], spec["threshold"])
                    for name, spec in self._alarm_specs.items()}
        return {c.replace("alarm_", ""): bool(row.get(c, False))
                for c in self.df.columns if c.startswith("alarm_")}

    def ecg_file_at(self, hours: float):
        row = self._row_at(hours)
        val = row.get("ecg_lead2_file")
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return None
        return str(val)

    def all12_rel_at(self, hours: float):
        rel = self.ecg_file_at(hours)
        return rel.replace("_lead2", "_all12") if rel else None

    def machine_rhythm_at(self, hours: float):
        val = self._row_at(hours).get("ecg_machine_rhythm")
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return None
        return str(val)

    def ecg_meta_at(self, hours: float) -> dict:
        row = self._row_at(hours)
        sid = row.get("ecg_study_id")
        try:
            sid = None if (sid is None or (isinstance(sid, float) and math.isnan(sid))) else str(int(float(sid)))
        except Exception:
            sid = None if sid is None else str(sid)
        return {"study_id": sid, "rhythm": self.machine_rhythm_at(hours),
                "hr": _f(row, "ecg_machine_hr"), "qtc": _f(row, "ecg_machine_qtc_ms"),
                "qrs": _f(row, "ecg_machine_qrs_ms")}

    # -- Event summary for the UI scrubber (navigation only, not clinical) -----

    def _windows_from_mask(self, mask, min_gap: float, min_dur: float):
        h = self._hours_arr
        m = np.asarray(mask, dtype=bool)
        raw, start = [], None
        for i, v in enumerate(m):
            if v and start is None:
                start = h[i]
            elif not v and start is not None:
                raw.append([start, h[i]]); start = None
        if start is not None:
            raw.append([start, h[-1]])
        merged = []
        for w in raw:
            if merged and w[0] - merged[-1][1] < min_gap:
                merged[-1][1] = w[1]
            else:
                merged.append([w[0], w[1]])
        return [w for w in merged if (w[1] - w[0]) >= min_dur]

    def _col_mask(self, col: str, op: str, thr: float):
        if col not in self.df.columns:
            return np.zeros(len(self.df), dtype=bool)
        s = pd.to_numeric(self.df[col], errors="coerce")
        cmp = (s < thr) if op == "<" else (s > thr)
        return cmp.fillna(False).to_numpy()

    def event_summary(self) -> dict:
        """Slider markers (navigation only). Coloured onset ticks come from the
        `critical` alarms in config; cyan ticks are real 12-lead recordings."""
        crit_only = self._marker_cfg.get("critical_only", True)
        min_dur   = self._marker_cfg.get("min_duration_hours", 1.0)
        gap       = self._marker_cfg.get("merge_gap_hours", 2.0)
        alarms, onsets = [], []
        for name, spec in self._alarm_specs.items():
            if crit_only and not spec.get("critical", False):
                continue
            mask = self._col_mask(spec["signal"], spec["op"], spec["threshold"])
            for a, b in self._windows_from_mask(mask, gap, min_dur):
                alarms.append({"type": name, "start": round(float(a), 3), "end": round(float(b), 3)})
                onsets.append({"type": name, "hour": round(float(a), 3)})
        ecgs = []
        if self.admission_time is not None:
            for e in self.ecg_index:
                try:
                    t = pd.to_datetime(e.get("ecg_time"))
                    hr = (t - self.admission_time).total_seconds() / 3600.0
                except Exception:
                    continue
                if hr > self.duration_hours:
                    continue
                qtc, mhr = e.get("machine_qtc_ms"), e.get("machine_hr")
                ecgs.append({"hour": round(max(0.0, hr), 3),
                             "study_id": str(e.get("study_id", "")),
                             "qtc": (round(float(qtc)) if qtc not in (None, "") else None),
                             "hr":  (round(float(mhr)) if mhr not in (None, "") else None),
                             "rhythm": (e.get("machine_rhythm") or "").split(";")[0][:60]})
        ecgs.sort(key=lambda x: x["hour"])
        onsets.sort(key=lambda x: x["hour"])
        return {"duration_hours": round(self.duration_hours, 3),
                "alarms": alarms, "onsets": onsets, "ecgs": ecgs}


# -- Helpers -------------------------------------------------------------------

def _f(row: pd.Series, col: str):
    val = row.get(col)
    if val is None:
        return None
    try:
        f = float(val)
        return None if math.isnan(f) else round(f, 2)
    except (TypeError, ValueError):
        return None


def _cmp(v, op: str, thr: float) -> bool:
    try:
        v = float(v)
        if math.isnan(v):
            return False
    except (TypeError, ValueError):
        return False
    return v < thr if op == "<" else v > thr