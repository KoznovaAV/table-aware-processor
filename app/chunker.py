import pandas as pd

class TableChunker:
    def __init__(self, max_chunk_bytes: int = 50000, max_cells_per_chunk: int = 5000):
        self.max_chunk_bytes = max_chunk_bytes
        self.max_cells_per_chunk = max_cells_per_chunk

    def _estimate_row_bytes(self, row_dict: dict, headers: list, sheet_name: str) -> int:
        meta = f"Sheet:{sheet_name}," + ",".join(h["name"] for h in headers)
        row_str = ",".join(f"{h['name']}:{str(row_dict.get(h['name'], ''))}" for h in headers)
        return len((meta + row_str).encode("utf-8"))

    def chunk_file(self, parsed_data: dict, file_path: str) -> list:
        chunks = []
        for sheet_name, info in parsed_data["sheets"].items():
            if parsed_data["file_type"] == "xlsx":
                df = pd.read_excel(file_path, sheet_name=sheet_name)
            else:
                df = self._read_csv_smart(file_path)
            chunks.extend(self._make_chunks(df, sheet_name, info, parsed_data["filename"]))
        return chunks

    def _read_csv_smart(self, path: str) -> pd.DataFrame:
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

        records = df.to_dict('records')
        start = 0
        header_rows = info.get("header_rows", 1)

        while start < total:
            end = start
            current_bytes = 0
            chunk_data = []

            while end < total:
                row = records[end]
                row_bytes = self._estimate_row_bytes(row, headers, sheet)

                if current_bytes + row_bytes > self.max_chunk_bytes and end > start:
                    break
                if (end - start + 1) * len(df.columns) > self.max_cells_per_chunk:
                    break

                current_bytes += row_bytes
                chunk_data.append(row)
                end += 1

            if end == start:
                end = start + 1
                chunk_data = [records[start]]

            col_letter = self._num2col(len(df.columns))
            chunk_df = pd.DataFrame(chunk_data)

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
        return f"Sheet: {sheet}\nColumns: {cols}\nRows {r_start}-{r_end}:\n{df.fillna('').to_string(index=False)}"

    def _num2col(self, n: int) -> str:
        res = ""
        while n > 0:
            n, rem = divmod(n - 1, 26)
            res = chr(65 + rem) + res
        return res