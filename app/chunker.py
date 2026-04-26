import pandas as pd
import numpy as np

class TableChunker:
    def __init__(self, max_chunk_bytes: int = 50000, max_cells_per_chunk: int = 5000):
        self.max_bytes = max_chunk_bytes
        self.max_cells = max_cells_per_chunk

    def chunk_file(self, parsed_data: dict, file_path: str) -> list:

        chunks = []
        filename = parsed_data.get("filename", "unknown")
        
        for sheet_name, info in parsed_data["sheets"].items():
            df = info.get("df")
            
            if df is None or df.empty:
                continue

            try:
                chunks.extend(self._make_chunks(df, sheet_name, info, filename))
            except Exception as e:
                print(f"Error processing sheet {sheet_name}: {e}")
                continue
        
        return chunks

    def _make_chunks(self, df: pd.DataFrame, sheet: str, info: dict, fname: str) -> list:
        out = []
        headers = info["columns"]
        total = len(df)

        if total == 0:
            return out

        df_clean = df.replace([np.nan, np.inf, -np.inf], "")
        records = df_clean.to_dict("records")

        start = 0
        header_rows = info.get("header_rows", 1)

        while start < total:
            end = start
            current_bytes = 0
            chunk_data = []

            while end < total:
                row = records[end]
                row_content = ",".join(f"{h['name']}:{str(row.get(h['name'], ''))}" for h in headers)
                meta_content = f"Sheet:{sheet}," + ",".join(h["name"] for h in headers)
                
                row_bytes = len((meta_content + row_content).encode("utf-8"))

                if current_bytes + row_bytes > self.max_bytes and end > start:
                    break
                if (end - start + 1) * len(df.columns) > self.max_cells:
                    break

                current_bytes += row_bytes
                chunk_data.append(row)
                end += 1

            if end == start:
                end = start + 1
                chunk_data = [records[start]]
                current_bytes = len((f"Sheet:{sheet}," + ",".join(h["name"] for h in headers) + 
                                     ",".join(f"{h['name']}:{str(chunk_data[0].get(h['name'], ''))}" for h in headers)).encode("utf-8"))

            if chunk_data:
                col_letter = self._num2col(len(df.columns))
                
                abs_row_start = start + 1 
                abs_row_end = end

                out.append({
                    "chunk_id": f"{fname}_{sheet}_{start}_{end}",
                    "chunk_size_bytes": current_bytes,
                    "source_ref": {
                        "sheet": sheet,
                        "range": f"A{abs_row_start}:{col_letter}{abs_row_end}",
                        "row_start": abs_row_start,
                        "row_end": abs_row_end
                    },
                    "context": {
                        "headers": [h["name"] for h in headers],
                        "sheet_name": sheet,
                        "header_rows": header_rows
                    },
                    "data": chunk_data,
                    "text_projection": self._build_projection(pd.DataFrame(chunk_data), sheet, headers, abs_row_start, abs_row_end)
                })
            start = end

        return out

    def _build_projection(self, df: pd.DataFrame, sheet: str, headers: list, r_start: int, r_end: int) -> str:
        cols = ", ".join(h["name"] for h in headers)
        df_display = df.fillna('')
        return f"Sheet: {sheet}\nColumns: {cols}\nRows {r_start}-{r_end}:\n{df_display.to_string(index=False)}"

    @staticmethod
    def _num2col(n: int) -> str:
        res = ""
        while n > 0:
            n -= 1
            res = chr(65 + n % 26) + res
            n //= 26
        return res if res else "A"