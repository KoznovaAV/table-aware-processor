from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import JSONResponse
from app.parser import TableParser
from app.chunker import TableChunker
from app.profiler import TableProfiler
import tempfile
import os
import pandas as pd

app = FastAPI(title="Table-Aware API", version="1.0")
parser = TableParser()
profiler = TableProfiler()

def _tmp(file: UploadFile) -> str:
    ext = os.path.splitext(file.filename)[1]
    t = tempfile.NamedTemporaryFile(delete=False, suffix=ext)
    t.write(file.file.read())
    t.close()
    return t.name

@app.post("/parse")
async def parse(file: UploadFile = File(...)):
    p = _tmp(file)
    try:
        return JSONResponse(parser.parse_file(p))
    finally:
        os.remove(p)

@app.post("/chunk")
async def chunk(file: UploadFile = File(...), max_rows: int = Query(200, ge=10), max_cells: int = Query(5000, ge=100)):
    p = _tmp(file)
    try:
        parsed = parser.parse_file(p)
        chunker = TableChunker(max_rows_per_chunk=max_rows, max_cells_per_chunk=max_cells)
        chunks = chunker.chunk_file(parsed, p)
        return JSONResponse({"chunks_count": len(chunks), "chunks": chunks})
    finally:
        os.remove(p)

@app.post("/profile")
async def profile(file: UploadFile = File(...)):
    p = _tmp(file)
    try:
        if file.filename.endswith(".xlsx"):
            df = pd.read_excel(p)
        else:
            df = pd.read_csv(p)
        return JSONResponse(profiler.profile(df))
    finally:
        os.remove(p)

@app.post("/process-all")
async def process_all(file: UploadFile = File(...), max_rows: int = Query(200, ge=10), max_cells: int = Query(5000, ge=100)):
    p = _tmp(file)
    try:
        parsed = parser.parse_file(p)
        chunker = TableChunker(max_rows_per_chunk=max_rows, max_cells_per_chunk=max_cells)
        ch = chunker.chunk_file(parsed, p)
        if file.filename.endswith(".xlsx"):
            df = pd.read_excel(p)
        else:
            df = pd.read_csv(p)
        return JSONResponse({
            "metadata": parsed,
            "chunks": ch,
            "profile": profiler.profile(df),
            "summary": {
                "total_chunks": len(ch),
                "file_type": parsed["file_type"]
            }
        })
    finally:
        os.remove(p)