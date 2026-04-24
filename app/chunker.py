import pandas as pd

class TableChunker:

    def __init__(self, max_rows_per_chunk: int = 200, max_cells_per_chunk: int = 5000):
        self.max_rows = max_rows_per_chunk
        self.max_cells = max_cells_per_chunk

    def _convert_to_serializable(self, obj):
        if isinstance(obj, dict):
            return {k: self._convert_to_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_to_serializable(item) for item in obj]
        elif isinstance(obj, pd.Timestamp):
            return str(obj)
        elif pd.isna(obj):
            return None
        elif isinstance(obj, (int, float, str, bool)) or obj is None:
            return obj
        else:
            return str(obj)

    def chunk_file(self, parsed_data: dict, file_path: str) -> list:
        chunks = []
        for sheet_name, info in parsed_data["sheets"].items():
            if parsed_data["file_type"] == "xlsx":
                df = pd.read_excel(file_path, sheet_name=sheet_name)
            else:
                encodings = ["utf-8", "windows-1251", "cp1252", "latin1"]
                separators = [",", ";", "\t", "|"]
                df = None
                for enc in encodings:
                    for sep in separators:
                        try:
                            df = pd.read_csv(file_path, encoding=enc, sep=sep, on_bad_lines="skip")
                            if not df.empty:
                                break
                        except Exception:
                            continue
                    if df is not None and not df.empty:
                        break
                if df is None or df.empty:
                    raise ValueError("Cannot read CSV file")
            chunks.extend(self._make_chunks(df, sheet_name, info, parsed_data["filename"]))
        return self._convert_to_serializable(chunks)

    def _make_chunks(self, df: pd.DataFrame, sheet: str, info: dict, fname: str) -> list:
        out = []
        headers = info["columns"]
        total = len(df)
        start = 0
        while start < total:
            end = min(start + self.max_rows, total)
            cells_count = (end - start) * len(df.columns)
            if cells_count > self.max_cells and len(df.columns) > 0:
                end = start + max(1, self.max_cells // len(df.columns))
            piece = df.iloc[start:end]
            col_letter = self._num2col(len(df.columns))
            out.append({
                "chunk_id": f"{fname}_{sheet}_{start}_{end}",
                "source_ref": {
                    "sheet": sheet,
                    "range": f"A{start + 2}:{col_letter}{end + 1}",
                    "row_start": start + 1,
                    "row_end": end
                },
                "context": {
                    "headers": headers,
                    "sheet_name": sheet,
                    "header_rows": info.get("header_rows", 1)
                },
                "data": piece.to_dict("records"),
                "text_projection": self._build_projection(piece, sheet, headers, start + 1, end)
            })
            start = end
        return out

    def _build_projection(self, df: pd.DataFrame, sheet: str, headers: list, r_start: int, r_end: int) -> str:
        cols = ", ".join(h["name"] for h in headers)
        df_clean = df.fillna("")
        return f"Sheet: {sheet}\nColumns: {cols}\nRows {r_start}-{r_end}:\n{df_clean.to_string(index=False)}"

    def _num2col(self, n: int) -> str:
        res = ""
        while n > 0:
            n, rem = divmod(n - 1, 26)
            res = chr(65 + rem) + res
        return res