from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import JSONResponse
import json
import tempfile
import os

app = FastAPI(title="Table-Aware API")


@app.post("/process")
async def process(
    file: UploadFile = File(...),
    max_rows: int = Query(200),
    max_cells: int = Query(5000)
):

    suffix = os.path.splitext(file.filename)[1] if file.filename else ".tmp"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    try:
        from app.parser import TableParser
        from app.chunker import TableChunker

        parser = TableParser()
        meta = parser.parse_file(tmp_path)

        chunker = TableChunker(
            max_chunk_bytes=max_rows * 100,
            max_cells_per_chunk=max_cells
        )
        chunks = chunker.chunk_file(meta, tmp_path)

        result = {
            "filename": file.filename,
            "metadata": meta,
            "chunks_count": len(chunks),
            "chunks": chunks[:30] 
        }

        json_str = json.dumps(result, default=str, ensure_ascii=False)

        return JSONResponse(content=json.loads(json_str))

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "message": "Processing failed"}
        )
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


@app.get("/")
async def root():
    return {
        "service": "Table-Aware API",
        "endpoints": {
            "POST /process": "Upload file -> get metadata + chunks"
        },
        "docs": "http://localhost:8000/docs"
    }