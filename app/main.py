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
        # Возвращаем метаданные и структуру, но без самих данных (DF слишком тяжелый для JSON)
        # Для этого модифицируем parser, чтобы он не включал DF в ответ, если это не нужно
        # Но пока оставим как есть, просто учти, что DF в JSON не сериализуется напрямую.
        # В текущем parser.py мы возвращаем dict с ключом 'sheets_data', где лежит DF.
        # FastAPI не сможет сериализовать DataFrame в JSON автоматически.
        # Поэтому для эндпоинта /parse лучше вернуть только метаданные.
        
        parsed = parser.parse_file(p)
        # Удаляем DataFrame из ответа, так как он не JSON-сериализуем
        for sheet in parsed.get("sheets_data", {}).values():
            sheet.pop("df", None)
            
        return JSONResponse(parsed)
    finally:
        os.remove(p)

@app.post("/chunk")
async def chunk(file: UploadFile = File(...), max_rows: int = Query(200, ge=10), max_cells: int = Query(5000, ge=100)):
    p = _tmp(file)
    try:
        parsed = parser.parse_file(p)
        chunker = TableChunker(max_rows_per_chunk=max_rows, max_cells_per_chunk=max_cells)
        
        # Chunker теперь умеет работать с результатом parser'а
        chunks = chunker.chunk_file(parsed, p)
        
        return JSONResponse({
            "chunks_count": len(chunks),
            "chunks": chunks
        })
    finally:
        os.remove(p)

@app.post("/profile")
async def profile(file: UploadFile = File(...)):
    p = _tmp(file)
    try:
        # Для профилирования нам нужен DF. Читаем его через парсер, чтобы не дублировать логику CSV/Excel detection
        parsed = parser.parse_file(p)
        
        # Берем первый лист для общего профиля (или можно агрегировать)
        first_sheet_df = None
        for sheet_info in parsed.get("sheets_data", {}).values():
            first_sheet_df = sheet_info.get("df")
            break
            
        if first_sheet_df is None:
            return JSONResponse({"error": "No data found"})
            
        prof = profiler.profile(first_sheet_df)
        return JSONResponse(prof)
    finally:
        os.remove(p)

@app.post("/process-all")
async def process_all(file: UploadFile = File(...), max_rows: int = Query(200, ge=10), max_cells: int = Query(5000, ge=100)):
    p = _tmp(file)
    try:
        parsed = parser.parse_file(p)
        
        # 1. Чанкинг
        chunker = TableChunker(max_rows_per_chunk=max_rows, max_cells_per_chunk=max_cells)
        chunks = chunker.chunk_file(parsed, p)
        
        # 2. Профилирование (берем первый лист как пример)
        first_sheet_df = None
        for sheet_info in parsed.get("sheets_data", {}).values():
            first_sheet_df = sheet_info.get("df")
            break
        
        profile_data = {}
        if first_sheet_df is not None:
            profile_data = profiler.profile(first_sheet_df)
            
        # 3. Метаданные (без DF)
        metadata = parsed.copy()
        for sheet in metadata.get("sheets_data", {}).values():
            sheet.pop("df", None)

        return JSONResponse({
            "metadata": metadata,
            "chunks": chunks,
            "profile": profile_data,
            "summary": {
                "total_chunks": len(chunks),
                "file_type": parsed["file_type"]
            }
        })
    finally:
        os.remove(p)