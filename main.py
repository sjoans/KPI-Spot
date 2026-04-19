import os, json, sqlite3
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from typing import Any

# ── Config ──────────────────────────────────────────────────────────
# En Railway: agrega una variable de entorno DB_PATH=/data/snapshots.db
# y monta un Volume en /data para que los datos persistan entre deploys.
DB_PATH = os.environ.get("DB_PATH", "./data/snapshots.db")

# ── DB helpers ──────────────────────────────────────────────────────
def get_db():
    os.makedirs(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id      TEXT PRIMARY KEY,
            label   TEXT NOT NULL,
            date    TEXT NOT NULL,
            faena   TEXT NOT NULL,
            payload TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

# ── App ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(title="MEL Acreditaciones API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── API routes ──────────────────────────────────────────────────────
@app.get("/api/snapshots")
def list_snapshots():
    conn = get_db()
    rows = conn.execute("SELECT payload FROM snapshots ORDER BY date ASC").fetchall()
    conn.close()
    return [json.loads(r["payload"]) for r in rows]

@app.post("/api/snapshots", status_code=201)
def create_snapshot(snap: Any = Body(...)):
    if not snap.get("id") or not snap.get("label"):
        raise HTTPException(400, "Snapshot inválido: falta id o label")
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO snapshots (id,label,date,faena,payload) VALUES (?,?,?,?,?)",
            [snap["id"], snap["label"], snap["date"], snap.get("faena","MEL"), json.dumps(snap)]
        )
        conn.commit()
    except sqlite3.IntegrityError:
        raise HTTPException(409, "Ya existe un snapshot con ese id")
    finally:
        conn.close()
    return {"ok": True, "id": snap["id"]}

@app.delete("/api/snapshots/{snap_id}")
def delete_snapshot(snap_id: str):
    conn = get_db()
    cur = conn.execute("DELETE FROM snapshots WHERE id=?", [snap_id])
    conn.commit()
    conn.close()
    if cur.rowcount == 0:
        raise HTTPException(404, "Snapshot no encontrado")
    return {"ok": True}

# ── Sirve el dashboard HTML ──────────────────────────────────────────
@app.get("/")
def root():
    return FileResponse("dashboard.html")

# Para archivos estáticos futuros (CSS, JS separados, etc.)
# app.mount("/static", StaticFiles(directory="static"), name="static")
