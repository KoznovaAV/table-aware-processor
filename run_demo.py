import os
import sys
import json
from app.parser import TableParser
from app.chunker import TableChunker
from app.profiler import TableProfiler

def run_demo():
    print("Запуск демо-обработки...")
    
    # Проверяем наличие примеров
    example_file = "examples/primer_1.xlsx"
    if not os.path.exists(example_file):
        print(f"Файл {example_file} не найден!")
        return

    # 1. Парсинг
    parser = TableParser()
    parsed_data = parser.parse_file(example_file)
    print(f"Файл прочитан. Листов: {len(parsed_data['sheets_data'])}")

    # 2. Чанкинг
    chunker = TableChunker(max_rows_per_chunk=50, max_cells_per_chunk=1000)
    chunks = chunker.chunk_file(parsed_data, example_file)
    print(f"Создано чанков: {len(chunks)}")

    # 3. Профилирование (первый лист)
    first_sheet_df = list(parsed_data["sheets_data"].values())[0]["df"]
    profiler = TableProfiler()
    profile = profiler.profile(first_sheet_df)
    print(f"Профиль создан. Предупреждений: {len(profile['warnings'])}")

    # 4. Сохранение
    os.makedirs("output", exist_ok=True)
    result = {
        "filename": parsed_data["filename"],
        "chunks_count": len(chunks),
        "profile_warnings": profile["warnings"],
        "sample_chunk": chunks[0] if chunks else None
    }
    
    with open("output/demo_result.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
        
    print("Результат сохранен в output/demo_result.json")

if __name__ == "__main__":
    run_demo()