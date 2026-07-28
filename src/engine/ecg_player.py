"""ECGPlayer — serves a continuous stream of Lead II samples.

Loads a real .npy waveform extracted in notebook 04.
Falls back to a synthetic waveform if the file is absent or unreadable.
Loops the 10-second recording seamlessly.
"""

import numpy as np
from pathlib import Path


class ECGPlayer:
    FS = 500  # MIMIC-IV-ECG sampling rate

    def __init__(self):
        self._waveform: np.ndarray | None = None  # (N,) float32
        self._pos: int = 0
        self._current_path: str | None = None

    # ── Public API ────────────────────────────────────────────────────────────

    def load(self, npy_path: str | None, hr: float = 75.0):
        """Load a real ECG waveform or generate a synthetic one.

        npy_path : absolute path to the .npy file, or None for synthetic.
        hr       : heart rate used only for synthetic fallback.
        """
        if npy_path and npy_path == self._current_path:
            return  # already loaded, keep playback position

        self._current_path = npy_path
        self._pos = 0

        if npy_path:
            path = Path(npy_path)
            if path.exists():
                try:
                    arr = np.load(str(path)).astype("float32")
                    if not np.all(np.isnan(arr)) and len(arr) > 0:
                        self._waveform = arr
                        return
                except Exception:
                    pass

        # Fallback to synthetic
        self._waveform = self._make_synthetic(hr)

    def get_chunk(self, n_samples: int) -> list[float]:
        """Return the next n_samples (loops the waveform)."""
        if self._waveform is None or len(self._waveform) == 0:
            return [0.0] * n_samples

        n = len(self._waveform)
        idx = (self._pos + np.arange(n_samples)) % n
        chunk = self._waveform[idx]
        self._pos = int((self._pos + n_samples) % n)
        return chunk.tolist()

    # ── Synthetic fallback ────────────────────────────────────────────────────

    def _make_synthetic(self, hr: float, duration_s: float = 10.0) -> np.ndarray:
        """Pure-numpy synthetic Lead II ECG (no external library needed).

        Produces a morphologically plausible P-QRS-T complex template
        stretched to the desired heart rate.
        """
        try:
            import neurokit2 as nk
            sig = nk.ecg_simulate(
                duration=duration_s,
                sampling_rate=self.FS,
                heart_rate=hr,
                method="ecgsyn",
            )
            return np.array(sig, dtype="float32")
        except ImportError:
            pass

        # Pure-numpy PQRST template
        n = int(duration_s * self.FS)
        sig = np.zeros(n, dtype="float32")
        period = int(self.FS * 60.0 / max(hr, 20.0))

        def _template(p: int) -> np.ndarray:
            t = np.linspace(0, 1, p)
            # P wave
            pw = 0.12 * np.exp(-((t - 0.16) ** 2) / (2 * 0.03 ** 2))
            # Q
            q  = -0.10 * np.exp(-((t - 0.30) ** 2) / (2 * 0.008 ** 2))
            # R
            r  =  1.00 * np.exp(-((t - 0.33) ** 2) / (2 * 0.006 ** 2))
            # S
            s  = -0.18 * np.exp(-((t - 0.36) ** 2) / (2 * 0.008 ** 2))
            # T wave
            tw =  0.28 * np.exp(-((t - 0.56) ** 2) / (2 * 0.06 ** 2))
            return (pw + q + r + s + tw).astype("float32")

        tmpl = _template(period)
        for start in range(0, n - period, period):
            end = min(start + period, n)
            sig[start:end] = tmpl[: end - start]

        return sig
