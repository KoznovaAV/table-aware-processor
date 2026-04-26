import pandas as pd
import numpy as np

class TableChunker:
    def __init__(self, max_chunk_bytes: int = 50000, max_cells_per_chunk: int = 5000):
        self.max_bytes = max_chunk_bytes
        self.max_cells = max_cells_per_chunk

    def chunk_file(self, parsed_data: dict, file_path: str) -> list:  # <-- ИСПРАВЛЕНО: было "parsed_ dict"
        chunks = []
        for sheet_name, info in parsed_data["sheets"].items():
            try:
                if parsed_data["file_type"] == "xlsx":
                    df = pd.read_excel(file_path, sheet_name=sheet_name)
                else:
                    df = self._read_csv(file_path)

                df = df.replace([np.nan, np.inf, -np.inf], "")
                chunks.extend(self._make_chunks(df, sheet_name, info, parsed_data["filename"]))
            except Exception as e:
                print(f"Error processing sheet {sheet_name}: {e}")
                continue
        return chunks

    def _read_csv(self, path: str) -> pd.DataFrame:
        for enc in ["utf-8", "windows-1251", "cp1252"]:
            for sep in [",", ";", "\t"]:
                try:
                    df = pd.read_csv(path, encoding=enc, sep=sep, on_bad_lines="skip")
                    if not df.empty:
                        return df
                except Exception:
                    continue
        raise ValueError(f"Cannot read CSV: {path}")

    def _make_chunks(self, df: pd.DataFrame, sheet: str, info: dict, fname: str) -> list:
        out = []
        headers = info["columns"]
        total = len(df)

        if total == 0:
            return out

        records = []
        for _, row in df.iterrows():
            row_dict = {}
            for col in df.columns:
                val = row[col]
                if pd.isna(val):
                    row_dict[col] = ""
                elif isinstance(val, (np.integer, np.int64, np.int32)):
                    row_dict[col] = int(val)
                elif isinstance(val, (np.floating, np.float64, np.float32)):
                    row_dict[col] = float(val) if not np.isinf(val) else ""
                else:
                    row_dict[col] = str(val)
            records.append(row_dict)

        start = 0
        header_rows = info.get("header_rows", 1)

        while start < total:
            end = start
            current_bytes = 0
            chunk_data = []

            while end < total:
                row = records[end]
                row_str = ",".join(f"{h['name']}:{str(row.get(h['name'], ''))}" for h in headers)
                row_bytes = len((f"Sheet:{sheet}," + ",".join(h["name"] for h in headers) + row_str).encode("utf-8"))

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

            if chunk_data:
                chunk_df = pd.DataFrame(chunk_data)
                col_letter = self._num2col(len(df.columns))

                out.append({
                    "chunk_id": f"{fname}_{sheet}_{start}_{end}",
                    "chunk_size_bytes": current_bytes,
                    "source_ref": {
                        "sheet": sheet,
                        "range": f"A{start + header_rows + 1}:{col_letter}{end + header_rows}",
                        "row_start": start + 1,
                        "row_end": end
                    },
                    "context": {
                        "headers": [h["name"] for h in headers],
                        "sheet_name": sheet,
                        "header_rows": header_rows
                    },
                    "data": chunk_data,
                    "text_projection": self._build_projection(chunk_df, sheet, headers, start + 1, end)
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