import pandas as pd
import os
from typing import Dict, Any, Optional

class TableParser:

    def parse_file(self, file_path: str) -> dict:
        """
        Парсит файл и возвращает словарь с метаданными и готовыми DataFrame для каждого листа.
        """
        _, ext = os.path.splitext(file_path)
        ext = ext.lower()
        
        if ext == ".xlsx":
            return self._parse_excel(file_path)
        elif ext == ".csv":
            return self._parse_csv(file_path)
        
        raise ValueError(f"Unsupported format: {ext}")

    def _detect_type(self, series: pd.Series) -> str:
        s = series.dropna()
        if s.empty:
            return "empty"
        
        # Проверка на булевы значения
        bool_values = {"true", "false", "1", "0", "yes", "no", "да", "нет"}
        if s.astype(str).str.lower().isin(bool_values).all():
            return "bool"
        
        # Проверка на числа
        try:
            pd.to_numeric(s)
            return "number"
        except (ValueError, TypeError):
            pass
        
        # Проверка на даты
        try:
            # errors='coerce' превратит неподходящие строки в NaT
            dt_series = pd.to_datetime(s, errors='coerce')
            # Если большинство значений успешно распарсились как даты
            if dt_series.notna().sum() > len(s) / 2:
                if s.astype(str).str.contains(":").any():
                    return "datetime"
                return "date"
        except Exception:
            pass
            
        return "string"

    def _parse_excel(self, path: str) -> dict:
        import openpyxl
        # Читаем книгу для получения имен листов
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        
        result = {
            "file_type": "xlsx",
            "filename": os.path.basename(path),
            "sheets_data": {} # Здесь будем хранить DF и метаданные
        }
        
        try:
            for name in wb.sheetnames:
                # Читаем лист БЕЗ заголовка, чтобы сохранить сырую структуру
                # header=None важно для обработки сложных шапок в chunker
                df_raw = pd.read_excel(path, sheet_name=name, header=None)
                
                if df_raw.empty:
                    continue
                
                # Предварительная очистка от полностью пустых строк/колонок по краям (опционально)
                # df_raw = df_raw.dropna(how='all').dropna(axis=1, how='all')
                
                # Определяем типы на основе сырых данных (примерно)
                # Примечание: точные типы лучше определять в chunker после ffill, 
                # но здесь мы дадим базовую оценку по первой непустой строке данных
                cols_meta = []
                for i, col_series in df_raw.items():
                    col_name = f"Col_{i}" # Временное имя, chunker переименует
                    cols_meta.append({
                        "original_index": i,
                        "type_hint": self._detect_type(col_series)
                    })

                r, c = df_raw.shape
                col_letter = self._num2col(c)
                
                result["sheets_data"][name] = {
                    "sheet_name": name,
                    "df": df_raw, # Передаем сам DataFrame!
                    "row_count": r,
                    "column_count": c,
                    "columns_meta": cols_meta,
                    "source_ref": {
                        "sheet": name,
                        "range": f"A1:{col_letter}{r}",
                        "row_start": 1,
                        "row_end": r
                    }
                }
        finally:
            wb.close()
            
        return result

    def _parse_csv(self, path: str) -> dict:
        df = None
        detected_enc = "utf-8"
        detected_sep = ","
        
        for enc in ["utf-8", "windows-1251", "cp1252", "latin1"]:
            for sep in [",", ";", "\t", "|"]:
                try:
                    # Читаем без заголовка для единообразия с Excel
                    df_temp = pd.read_csv(path, encoding=enc, sep=sep, on_bad_lines="skip", header=None)
                    if not df_temp.empty and len(df_temp.columns) > 1:
                        df = df_temp
                        detected_enc = enc
                        detected_sep = sep
                        break
                except Exception:
                    continue
            if df is not None:
                break
                
        if df is None or df.empty:
            # Фоллбэк: пробуем с header=0 если совсем плохо
            try:
                 df = pd.read_csv(path, encoding="utf-8", sep=",")
                 detected_enc = "utf-8"
                 detected_sep = ","
            except:
                raise ValueError("Cannot read CSV file")

        cols_meta = []
        for i, col_series in df.items():
            cols_meta.append({
                "original_index": i,
                "type_hint": self._detect_type(col_series)
            })

        r, c = df.shape
        col_letter = self._num2col(c)
        
        result = {
            "file_type": "csv",
            "filename": os.path.basename(path),
            "encoding": detected_enc,
            "separator": detected_sep,
            "sheets_data": {
                "default_sheet": {
                    "sheet_name": "default_sheet",
                    "df": df,
                    "row_count": r,
                    "column_count": c,
                    "columns_meta": cols_meta,
                    "source_ref": {
                        "sheet": "default_sheet",
                        "range": f"A1:{col_letter}{r}",
                        "row_start": 1,
                        "row_end": r
                    }
                }
            }
        }
        return result

    def _num2col(self, n: int) -> str:
        res = ""
        while n > 0:
            n, rem = divmod(n - 1, 26)
            res = chr(65 + rem) + res
        return res