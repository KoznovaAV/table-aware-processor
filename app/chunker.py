import pandas as pd
import numpy as np
from typing import Optional, Dict, List, Any

class TableChunker:

    def __init__(self, max_rows_per_chunk: int = 200, max_cells_per_chunk: int = 5000):
        self.max_rows = max_rows_per_chunk
        self.max_cells = max_cells_per_chunk

    def _infer_schema(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Определяет заголовки и типы данных."""
        if df.empty:
            return {"df": df, "headers": [], "header_rows": 0}

        # Эвристика шапки
        header_row_idx = 0
        for i in range(len(df)):
            if not df.iloc[i].isna().all():
                header_row_idx = i
                break
        
        headers_raw = df.iloc[header_row_idx].astype(str).tolist()
        seen = {}
        unique_headers = []
        for h in headers_raw:
            if h in seen:
                seen[h] += 1
                unique_headers.append(f"{h}_{seen[h]}")
            else:
                seen[h] = 0
                unique_headers.append(h)
        
        df_temp = df.copy()
        df_temp.columns = unique_headers
        df_data = df_temp.iloc[header_row_idx + 1:].reset_index(drop=True)
        
        columns_info = []
        for col in df_data.columns:
            series = df_data[col]
            non_null = series.dropna()
            
            col_info = {
                "name": str(col),
                "type": "empty",
                "null_count": int(series.isna().sum()),
                "null_pct": round(float(series.isna().mean()), 4) if len(series) > 0 else 0.0
            }
            
            if not non_null.empty:
                if pd.api.types.is_numeric_dtype(non_null):
                    col_info["type"] = "number"
                    col_info["min"] = float(non_null.min())
                    col_info["max"] = float(non_null.max())
                elif pd.api.types.is_datetime64_any_dtype(non_null):
                    col_info["type"] = "datetime"
                else:
                    try:
                        num_series = pd.to_numeric(non_null)
                        col_info["type"] = "number"
                        col_info["min"] = float(num_series.min())
                        col_info["max"] = float(num_series.max())
                    except:
                        col_info["type"] = "string"
                        top_vals = non_null.value_counts().head(3).index.tolist()
                        col_info["top_values"] = [str(v) for v in top_vals]
            
            columns_info.append(col_info)
            
        return {
            "df": df_data,
            "headers": columns_info,
            "header_rows": header_row_idx + 1
        }

    def chunk_file(self, parsed_data: dict, file_path: str) -> list:
        """
        Принимает результат parser.parse_file() и путь к файлу (для имени).
        Извлекает DataFrames из parsed_data и чанкает их.
        """
        chunks = []
        filename = parsed_data.get("filename", "unknown")
        sheets_data = parsed_data.get("sheets_data", {})
        
        for sheet_name, sheet_info in sheets_data.items():
            df_raw = sheet_info.get("df")
            if df_raw is None or df_raw.empty:
                continue
            
            # Применяем ffill для обработки объединенных ячеек Excel
            df_clean = df_raw.ffill()
            
            # Инференс схемы (заголовки, типы)
            schema = self._infer_schema(df_clean)
            df_final = schema["df"]
            headers = schema["headers"]
            header_rows_count = schema["header_rows"]
            
            # Генерация чанков
            chunks.extend(self._make_chunks(df_final, sheet_name, headers, header_rows_count, filename))
            
        return chunks

    def _make_chunks(self, df: pd.DataFrame, sheet: str, headers: list, 
                     header_rows_count: int, fname: str) -> list:
        out = []
        total = len(df)
        if total == 0:
            return out
            
        start = 0
        while start < total:
            end = min(start + self.max_rows, total)
            
            current_rows = end - start
            cells_count = current_rows * len(df.columns)
            
            if cells_count > self.max_cells and len(df.columns) > 0:
                allowed_rows = max(1, self.max_cells // len(df.columns))
                end = min(start + allowed_rows, total)
            
            piece = df.iloc[start:end]
            
            excel_row_start = start + header_rows_count + 1 
            excel_row_end = end + header_rows_count
            col_letter = self._num2col(len(df.columns))
            
            chunk_id = f"{fname}_{sheet}_{start}_{end}"
            
            out.append({
                "chunk_id": chunk_id,
                "source_ref": {
                    "sheet": sheet,
                    "range": f"A{excel_row_start}:{col_letter}{excel_row_end}",
                    "row_start": int(excel_row_start),
                    "row_end": int(excel_row_end),
                    "local_row_start": int(start),
                    "local_row_end": int(end)
                },
                "context": {
                    "headers": [h["name"] for h in headers],
                    "header_details": headers,
                    "sheet_name": sheet,
                    "header_rows": header_rows_count
                },
                "data": piece.to_dict("records"),
                "text_projection": self._build_projection(piece, sheet, headers, excel_row_start, excel_row_end)
            })
            start = end
        return out

    def _build_projection(self, df: pd.DataFrame, sheet: str, headers: list, r_start: int, r_end: int) -> str:
        cols = ", ".join(h["name"] for h in headers)
        df_clean = df.fillna("")
        text_table = df_clean.to_string(index=False, max_colwidth=50)
        return f"Sheet: {sheet}\nColumns: {cols}\nRows {r_start}-{r_end}:\n{text_table}"

    def _num2col(self, n: int) -> str:
        res = ""
        while n > 0:
            n, rem = divmod(n - 1, 26)
            res = chr(65 + rem) + res
        return res