import os, json, sqlite3, io
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Body, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import pandas as pd
import datetime, unicodedata, re

DB_PATH = os.environ.get("DB_PATH", "./data/snapshots.db")

def get_db():
    os.makedirs(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id TEXT PRIMARY KEY, label TEXT NOT NULL,
            date TEXT NOT NULL, faena TEXT NOT NULL, payload TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS current_data (
            id INTEGER PRIMARY KEY CHECK (id=1),
            filename TEXT, uploaded_at TEXT, payload TEXT NOT NULL
        );
    """)
    conn.commit(); conn.close()

def norm(s):
    s = str(s or '').strip()
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return re.sub(r'[\s\n\r]+', ' ', s).upper().strip()

def find_col(cols, target):
    t = norm(target)
    for c in cols:
        if norm(c) == t: return c
    for c in cols:
        if t in norm(c) or norm(c) in t: return c
    return None

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(title="MEL Acreditaciones API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.post("/api/upload")
async def upload_excel(file: UploadFile = File(...)):
    content = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(content))
        sheet_name = next((s for s in xl.sheet_names if s.strip().upper() == 'SPOT'), None)
        if not sheet_name:
            raise HTTPException(400, "No se encontró la hoja SPOT en el archivo")
        df = xl.parse(sheet_name, dtype=str).fillna('')
        cols = list(df.columns)

        PROG_MAP = [
            ('CERO DANO', 'Cero Daño'),
            ('CERTIFICADO DE ANTECEDENTES', 'Cert. Antecedentes'),
            ('FICHA MEL', 'Ficha MEL'),
            ('REGLAMENTO DE TRANSPORTE MEL', 'Regl. Transporte'),
            ('EXAMEN ALTURA GEOGRAFICA + DROGAS Y ALCOHOL', 'Exam. Altura+Drogas'),
            ('ANEXO VINCULO', 'Anexo Vínculo'),
        ]

        rows = []
        for _, row in df.iterrows():
            def g(name):
                c = find_col(cols, name)
                return str(row[c]).strip() if c and str(row[c]).strip() not in ('', 'nan') else ''

            cargo = g('CARGO')
            if not cargo or cargo == 'nan': continue

            nombres = g('NOMBRES'); ap = g('APELLIDO PATERNO'); am = g('APELLIDO MATERNO')
            estatus_raw = g('ESTATUS MEL')

            sar_raw = [g('SAR - ALTURA GEOGRAFICA'), g('SAR - CONTRATOS/ANEXOS'),
                       g('SAR - CERO DANO') or g('SAR - CERO DAÑO'), g('SAR - 3D CEIM')]

            detail = []; done = 0; total = 0
            for col_key, _ in PROG_MAP:
                v = g(col_key)
                if not v: detail.append('nodata'); continue
                try:
                    n = float(v); total += 1
                    if n == 1: done += 1; detail.append('ok')
                    elif n == 0: detail.append('pending')
                    else: detail.append('missing')
                except: detail.append('nodata')

            pct = round(done / total * 100) if total > 0 else None
            rows.append({
                'rut': g('RUT'),
                'nombre': f"{nombres} {ap} {am}".strip(),
                'apellidoPaterno': ap, 'cargo': cargo, 'estatus': estatus_raw,
                'sar': sar_raw,
                'progress': {'pct': pct, 'done': done, 'total': total, 'detail': detail}
            })

        now = datetime.datetime.utcnow().isoformat()
        payload = json.dumps({'persons': rows, 'filename': file.filename, 'uploaded_at': now})
        conn = get_db()
        conn.execute("""
            INSERT INTO current_data (id, filename, uploaded_at, payload) VALUES (1,?,?,?)
            ON CONFLICT(id) DO UPDATE SET filename=excluded.filename,
                uploaded_at=excluded.uploaded_at, payload=excluded.payload
        """, [file.filename, now, payload])
        conn.commit(); conn.close()
        return {"ok": True, "count": len(rows), "filename": file.filename}
    except HTTPException: raise
    except Exception as e:
        raise HTTPException(500, f"Error procesando Excel: {str(e)}")

@app.get("/api/current-data")
def get_current_data():
    conn = get_db()
    row = conn.execute("SELECT payload FROM current_data WHERE id=1").fetchone()
    conn.close()
    if not row: return {"persons": [], "filename": None, "uploaded_at": None}
    return json.loads(row["payload"])

@app.get("/api/snapshots")
def list_snapshots():
    conn = get_db()
    rows = conn.execute("SELECT payload FROM snapshots ORDER BY date ASC").fetchall()
    conn.close()
    return [json.loads(r["payload"]) for r in rows]

@app.post("/api/snapshots", status_code=201)
def create_snapshot(snap: dict = Body(...)):
    if not snap.get("id") or not snap.get("label"):
        raise HTTPException(400, "Snapshot inválido")
    conn = get_db()
    try:
        conn.execute("INSERT INTO snapshots (id,label,date,faena,payload) VALUES (?,?,?,?,?)",
            [snap["id"], snap["label"], snap["date"], snap.get("faena","MEL"), json.dumps(snap)])
        conn.commit()
    except sqlite3.IntegrityError:
        raise HTTPException(409, "Ya existe un snapshot con ese id")
    finally: conn.close()
    return {"ok": True}

@app.delete("/api/snapshots/{snap_id}")
def delete_snapshot(snap_id: str):
    conn = get_db()
    cur = conn.execute("DELETE FROM snapshots WHERE id=?", [snap_id])
    conn.commit(); conn.close()
    if cur.rowcount == 0: raise HTTPException(404, "No encontrado")
    return {"ok": True}

@app.get("/")
def root():
    return FileResponse("dashboard.html")
