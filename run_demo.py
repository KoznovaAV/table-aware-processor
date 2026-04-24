#!/usr/bin/env python3
import os
import json
import time
import pandas as pd
from app.parser import TableParser
from app.chunker import TableChunker
from app.profiler import TableProfiler

def run_demo():
    print("Table-Aware Processing Demo\n")
    parser = TableParser()
    chunker = TableChunker(max_rows_per_chunk=200)
    profiler = TableProfiler()

    examples_dir = "examples"
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    files = [f for f in os.listdir(examples_dir) if f.endswith((".xlsx", ".csv"))]
    if not files:
        print("No files in examples/. Place primer_*.xlsx or other tables there.")
        return

    for fname in files:
        fpath = os.path.join(examples_dir, fname)
        print(f"\n{fname}")
        start = time.time()
        try:
            parsed = parser.parse_file(fpath)
            chunks = chunker.chunk_file(parsed, fpath)
            df = pd.read_excel(fpath) if fname.endswith(".xlsx") else pd.read_csv(fpath)
            prof = profiler.profile(df)

            result = {
                "metadata": parsed,
                "chunks": chunks,
                "profile": prof,
                "summary": {
                    "total_chunks": len(chunks),
                    "file_type": parsed["file_type"]
                }
            }
            out_path = os.path.join(output_dir, f"{os.path.splitext(fname)[0]}_processed.json")
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)

            print(f"Saved to {out_path} | Chunks: {len(chunks)} | Time: {time.time() - start:.2f}s")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    run_demo()