"""src/server.py — FastAPI application.

Responsibilities:
  • Serve monitor.html at GET /
  • Broadcast the engine stream over WebSocket at ws://…/stream
  • Write every message to a JSONL session log
  • Expose REST endpoints for patient switching, speed, pause
"""

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

from .engine.engine import SimulationEngine

log = logging.getLogger(__name__)


def create_app(config: dict) -> FastAPI:
    app = FastAPI(title="ICU Simulator", docs_url=None, redoc_url=None)

    broadcast_queue: asyncio.Queue = asyncio.Queue(maxsize=2048)
    engine = SimulationEngine(config, broadcast_queue)

    # JSONL session log
    log_dir = Path(config.get("output_dir", "data/output"))
    log_dir.mkdir(parents=True, exist_ok=True)
    session_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    jsonl_path = log_dir / f"session_{session_ts}.jsonl"
    _jsonl = open(jsonl_path, "w", encoding="utf-8")
    log.info(f"JSONL log → {jsonl_path}")

    connected: set[WebSocket] = set()

    # ── Broadcaster ───────────────────────────────────────────────────────────

    async def _broadcaster():
        while True:
            msg = await broadcast_queue.get()

            # Write to JSONL
            _jsonl.write(json.dumps(msg, default=str) + "\n")
            _jsonl.flush()

            # Forward to all connected WebSocket clients
            dead: set[WebSocket] = set()
            for ws in list(connected):
                try:
                    await ws.send_json(msg)
                except Exception:
                    dead.add(ws)
            connected.difference_update(dead)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    @app.on_event("startup")
    async def _startup():
        asyncio.create_task(_broadcaster())
        asyncio.create_task(engine.run())
        log.info("Engine and broadcaster started.")

    @app.on_event("shutdown")
    async def _shutdown():
        _jsonl.close()

    # ── WebSocket endpoint ────────────────────────────────────────────────────

    @app.websocket("/stream")
    async def _stream(ws: WebSocket):
        await ws.accept()
        connected.add(ws)
        # Immediately send current patient context so late-joining clients sync
        p = engine.patients[engine.current_idx]
        try:
            await ws.send_json({
                "type": "session_start",
                "patient_id": p["subject_id"],
                "label": p["label"],
                "patients": engine.patients,
                "speed": engine.speed,
                "paused": engine.paused,
                "schema_version": "1.0",
                "t": 0.0,
                "duration_hours": (engine._source.duration_hours if engine._source else 0.0),
                "speed": engine.speed,
            })
            # Keep-alive: just wait (engine pushes, client only receives)
            while True:
                await ws.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            connected.discard(ws)



    # ── Control endpoints ─────────────────────────────────────────────────────

    @app.post("/control/patient/{idx}")
    async def _set_patient(idx: int):
        engine.switch_patient(idx)
        p = engine.patients[engine.current_idx]
        await broadcast_queue.put({
            "type": "session_start",
            "patient_id": p["subject_id"],
            "label": p["label"],
            "patients": engine.patients,
            "speed": engine.speed,
            "paused": engine.paused,
            "schema_version": "1.0",
            "t": 0.0,
            "duration_hours": (engine._source.duration_hours if engine._source else 0.0),
            "speed": engine.speed,
        })
        return JSONResponse({"ok": True, "patient": p})
    '''
    @app.post("/control/speed/{speed}")
    async def _set_speed(speed: float):
        engine.set_speed(speed)
        return JSONResponse({"ok": True, "speed": engine.speed})
    '''
    @app.post("/control/seek/{hours}")
    async def _seek(hours: float):
        engine.seek(hours)
        return JSONResponse({"ok": True, "hours": hours})
    
    @app.post("/control/pause")
    async def _toggle_pause():
        engine.paused = not engine.paused
        return JSONResponse({"ok": True, "paused": engine.paused})

    @app.get("/control/status")
    async def _status():
        p = engine.patients[engine.current_idx]
        return JSONResponse({
            "patient_id":    p["subject_id"],
            "patient_label": p["label"],
            "patient_idx":   engine.current_idx,
            "speed":         engine.speed,
            "paused":        engine.paused,
            "connected_clients": len(connected),
        })

    @app.get("/patients")
    async def _patients():
        return JSONResponse({"patients": engine.patients})

    @app.get("/events")
    async def _events():
        return JSONResponse(engine.event_summary())

    @app.get("/ecg/current12")
    async def _ecg12():
        data = engine.current_ecg_12lead()
        return JSONResponse(data if data else {})

    # ── Serve monitor.html ────────────────────────────────────────────────────

    _static_dir = Path(__file__).parent / "static"

    @app.get("/ecg12")
    async def _ecg12_page():
        html = (_static_dir / "ecg12.html").read_text(encoding="utf-8")
        return HTMLResponse(html)

    @app.get("/")
    async def _root():
        html = (_static_dir / "monitor.html").read_text(encoding="utf-8")
        return HTMLResponse(html)

    return app



def run(config: dict, host: str = "0.0.0.0", port: int = 8000):
    app = create_app(config)
    uvicorn.run(app, host=host, port=port, log_level="warning")
