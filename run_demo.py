#!/usr/bin/env python3
import os
import json
import time
import glob
import datetime
import warnings
import pandas as pd
from app.parser import TableParser
from app.chunker import TableChunker
from app.profiler import TableProfiler

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

class CustomEncoder(json.JSONEncoder):
    """Кастомный сериализатор для обработки дат и NaN"""
    def default(self, obj):
        if isinstance(obj, (pd.Timestamp, datetime.datetime, datetime.date)):
            return obj.isoformat() 
        if pd.isna(obj):
            return None
        return super().default(obj)

def process_single_file(file_path: str, parser, chunker, profiler, output_dir: str) -> bool:
    basename = os.path.basename(file_path)
    print(f"\n{'='*50}")
    print(f"📂 Обработка: {basename}")
    start_time = time.time()

    try:
        print("  ⏳ Шаг 1/3: Чтение структуры...")
        parsed_data = parser.parse_file(file_path)
        print(f"  ✅ Листов: {len(parsed_data['sheets'])}")

        print("  ⏳ Шаг 2/3: Разбиение на чанки...")
        chunks = chunker.chunk_file(parsed_data, file_path)
        print(f"  ✅ Создано чанков: {len(chunks)}")

        print("  ⏳ Шаг 3/3: Профилирование...")
        profile = {}
        try:
            first_sheet = list(parsed_data["sheets"].keys())[0]
            ext = os.path.splitext(file_path)[1].lower()
            
            if ext == ".xlsx":
                df_sample = pd.read_excel(file_path, sheet_name=first_sheet, nrows=1000)
            else:
                df_sample = pd.read_csv(file_path, nrows=1000, on_bad_lines="skip")
            
            profile = profiler.profile(df_sample)
            print(f"  ✅ Предупреждений: {len(profile['warnings'])}")
        except Exception as e:
            print(f"  ⚠️ Профилирование пропущено: {e}")

        out_name = os.path.splitext(basename)[0] + "_processed.json"
        out_path = os.path.join(output_dir, out_name)
        print(f"  💾 Сохранение в {out_name}...")

        meta = {
            "filename": basename,
            "chunks_count": len(chunks),
            "profile_warnings": profile.get("warnings", []),
            "sample_chunk": chunks[0] if chunks else None
        }

        with open(out_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(meta, ensure_ascii=False, indent=2, cls=CustomEncoder))
            f.write(',\n"chunks": [\n')
            
            for i, chunk in enumerate(chunks):
                f.write(json.dumps(chunk, ensure_ascii=False, cls=CustomEncoder))
                if i < len(chunks) - 1:
                    f.write(",\n")
                else:
                    f.write("\n")
            f.write(']\n}')

        elapsed = time.time() - start_time
        file_size_mb = os.path.getsize(out_path) / (1024 * 1024)
        print(f"  🏁 Готово! Время: {elapsed:.2f}с | Размер: {file_size_mb:.2f} МБ")
        return True

    except Exception as e:
        print(f"  ❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_demo():
    print("🚀 Запуск пакетной обработки демо-файлов...\n")
    
    parser = TableParser()
    chunker = TableChunker(max_chunk_bytes=100000, max_cells_per_chunk=10000)
    profiler = TableProfiler()

    examples_dir = "examples"
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    patterns = [os.path.join(examples_dir, "*.xlsx"), os.path.join(examples_dir, "*.csv")]
    files = []
    for p in patterns:
        files.extend(glob.glob(p))
    
    if not files:
        print(f"⚠️ В папке '{examples_dir}' не найдено файлов .xlsx или .csv.")
        return

    print(f"📦 Найдено файлов: {len(files)}")
    success_count = 0
    total_start = time.time()

    for file_path in sorted(files):
        if process_single_file(file_path, parser, chunker, profiler, output_dir):
            success_count += 1

    total_elapsed = time.time() - total_start
    print(f"\n{'='*50}")
    print("🎉 Обработка завершена!")
    print(f"✅ Успешно: {success_count}/{len(files)}")
    print(f"⏱️ Общее время: {total_elapsed:.2f} сек.")
    print(f"📁 Результаты сохранены в: {output_dir}/")

if __name__ == "__main__":
    run_demo()