from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import JSONResponse
import tempfile
import os

app = FastAPI(title="Table-Aware API")


@app.post("/process")
async def process(
    file: UploadFile = File(...),
    max_bytes: int = Query(50000, description="Max chunk size in bytes"),
    max_cells: int = Query(5000, description="Max cells per chunk")
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
            max_chunk_bytes=max_bytes,
            max_cells_per_chunk=max_cells
        )
        chunks = chunker.chunk_file(meta, tmp_path)

        result = {
            "filename": file.filename,
            "metadata": meta, 
            "chunks_count": len(chunks),
            "chunks": chunks[:30] 
        }

        for sheet in result["metadata"]["sheets"].values():
            sheet.pop("df", None)

        return JSONResponse(content=result)

    except Exception as e:
        import traceback
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "traceback": traceback.format_exc()}
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