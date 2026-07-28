"""check_setup.py — verify everything is in place before launching.  python check_setup.py"""
from pathlib import Path
import sys, pandas as pd

ROOT = Path(__file__).resolve().parent
PROC = ROOT / "data" / "processed"
TL, IDX, WAV = PROC/"patient_timelines", PROC/"ecg_index", PROC/"ecg_waveforms"
SUBJECTS = [16179553, 17520318, 13269747, 13408370, 15906662]
ok = True
def chk(cond, msg):
    global ok; ok = ok and cond
    print(("  OK   " if cond else " FAIL  ") + msg)

print("1) packages")
try:
    import fastapi, uvicorn; chk(True, "fastapi + uvicorn")
except Exception as e:
    chk(False, f"fastapi/uvicorn missing ({e}) -> pip install fastapi uvicorn")

print("2) package layout")
chk((ROOT/"src"/"server.py").exists(),            "src/server.py")
chk((ROOT/"src"/"engine"/"engine.py").exists(),   "src/engine/engine.py")
chk((ROOT/"src"/"static"/"monitor.html").exists(),"src/static/monitor.html")

print("3) per-patient data + ECG path resolution")
for sid in SUBJECTS:
    p, j = TL/f"patient_{sid}.parquet", IDX/f"ecg_index_{sid}.json"
    chk(p.exists(), f"patient_{sid}.parquet")
    chk(j.exists(), f"ecg_index_{sid}.json")
    if p.exists():
        files = pd.read_parquet(p, columns=["ecg_lead2_file"])["ecg_lead2_file"].dropna().unique()
        if len(files):
            clean = files[0].replace(".0_lead2.npy", "_lead2.npy")   # mirror engine guard
            resolved = WAV.parent / clean
            chk(resolved.exists(), f"  {sid}: sample ECG resolves ({files[0]})")
        else:
            chk(False, f"  {sid}: no ecg_lead2_file entries")

print("\n" + ("ALL PASSED → run:  python run_sim.py" if ok else "FIX THE FAILURES ABOVE FIRST"))
sys.exit(0 if ok else 1)