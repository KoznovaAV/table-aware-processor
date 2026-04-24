import pandas as pd

class TableChunker:

    def __init__(self, max_chunk_bytes: int = 10000, max_cells_per_chunk: int = 5000):
        self.max_chunk_bytes = max_chunk_bytes
        self.max_cells_per_chunk = max_cells_per_chunk

    def _estimate_row_size(self, row, headers, sheet_name: str) -> int:
        """Оценивает размер строки в байтах вместе с метаинформацией"""
        meta = f"Sheet: {sheet_name}\n"
        for h in headers:
            meta += f"{h['name']} ({h['type']}): "
        meta_size = len(meta.encode('utf-8'))
        
        row_text = ""
        for h in headers:
            val = row.get(h['name'], '')
            row_text += f"{h['name']}: {val}\n"
        
        return meta_size + len(row_text.encode('utf-8'))

    def _estimate_chunk_size(self, chunk_df: pd.DataFrame, sheet_name: str, headers: list) -> int:
        """Оценивает полный размер чанка в байтах"""
        total = 0
        for _, row in chunk_df.iterrows():
            total += self._estimate_row_size(row.to_dict(), headers, sheet_name)
        return total

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
                    raise ValueError(f"Cannot read CSV file: {file_path}")
            
            chunks.extend(self._make_chunks(df, sheet_name, info, parsed_data["filename"]))
        return chunks

    def _make_chunks(self, df: pd.DataFrame, sheet: str, info: dict, fname: str) -> list:
        out = []
        headers = info["columns"]
        total_rows = len(df)
        start = 0
        header_rows = info.get("header_rows", 1)
        
        while start < total_rows:
            current_chunk_rows = []
            current_size = 0
            end = start
            
            while end < total_rows:
                row = df.iloc[end].to_dict()
                row_size = self._estimate_row_size(row, headers, sheet)
                
                if current_size + row_size > self.max_chunk_bytes and len(current_chunk_rows) > 0:
                    break
                
                if (end - start + 1) * len(df.columns) > self.max_cells_per_chunk:
                    break
                
                current_chunk_rows.append(end)
                current_size += row_size
                end += 1
            
            if not current_chunk_rows:
                current_chunk_rows = [start]
                end = start + 1
            
            chunk_df = df.iloc[current_chunk_rows]
            
            col_letter = self._num2col(len(df.columns))
            out.append({
                "chunk_id": f"{fname}_{sheet}_{start}_{end}",
                "chunk_size_bytes": current_size,
                "source_ref": {
                    "sheet": sheet,
                    "range": f"A{start + header_rows + 1}:{col_letter}{end + header_rows}",
                    "row_start": start + 1,
                    "row_end": end
                },
                "context": {
                    "headers": headers,
                    "sheet_name": sheet,
                    "header_rows": header_rows
                },
                "data": chunk_df.to_dict("records"),
                "text_projection": self._build_projection(chunk_df, sheet, headers, start + 1, end)
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