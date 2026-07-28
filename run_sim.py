"""run_sim.py — launch the ICU replay simulator from config.json.

    python run_sim.py                 # uses ./config.json
    python run_sim.py my_config.json  # uses a different config
"""
import sys
import json
import traceback
from pathlib import Path

print(">>> run_sim.py is executing", flush=True)

PROJECT_ROOT = Path(__file__).resolve().parent
CONFIG_PATH = PROJECT_ROOT / (sys.argv[1] if len(sys.argv) > 1 else "config.json")


def load_config() -> dict:
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    p = cfg["paths"]
    # resolve the four data paths to absolute, relative to the project root
    cfg["project_root"] = str(PROJECT_ROOT)
    cfg["timeline_dir"]  = str(PROJECT_ROOT / p["timeline_dir"])
    cfg["ecg_index_dir"] = str(PROJECT_ROOT / p["ecg_index_dir"])
    cfg["waveform_dir"]  = str(PROJECT_ROOT / p["waveform_dir"])
    cfg["output_dir"]    = str(PROJECT_ROOT / p["output_dir"])
    return cfg


if __name__ == "__main__":
    try:
        from src.server import run
        cfg = load_config()
        print(f">>> loaded {CONFIG_PATH.name}: {len(cfg['patients'])} patients, "
              f"emit_alarms={cfg.get('stream', {}).get('emit_alarms')}", flush=True)
        print(">>> server starting → http://127.0.0.1:8000   (Ctrl+C to stop)", flush=True)
        run(cfg, host="127.0.0.1", port=8000)
    except Exception:
        traceback.print_exc()
        sys.exit(1)