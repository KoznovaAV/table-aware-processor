import pandas as pd
import os

class TableParser:

    def parse_file(self, file_path: str) -> dict:
        _, ext = os.path.splitext(file_path)
        ext = ext.lower()
        if ext == ".xlsx":
            return self._parse_excel(file_path)
        elif ext == ".csv":
            return self._parse_csv(file_path)
        raise ValueError(f"Unsupported format: {ext}")

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

    def _detect_type(self, series: pd.Series) -> str:
        s = series.dropna()
        if s.empty:
            return "empty"
        bool_values = ["true", "false", "1", "0", "yes", "no"]
        if s.astype(str).str.lower().isin(bool_values).all():
            return "bool"
        try:
            pd.to_numeric(s)
            return "number"
        except (ValueError, TypeError):
            pass
        try:
            pd.to_datetime(s, errors='coerce')
            if not s.astype(str).str.contains(":").all():
                return "date"
            return "datetime"
        except (ValueError, TypeError):
            return "string"

    def _parse_excel(self, path: str) -> dict:
        import openpyxl
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        result = {
            "file_type": "xlsx",
            "filename": os.path.basename(path),
            "sheets": {},
            "total_sheets": len(wb.sheetnames)
        }
        try:
            for name in wb.sheetnames:
                df = pd.read_excel(path, sheet_name=name)
                if df.empty:
                    continue
                cols = []
                for i, c in enumerate(df.columns):
                    col_name = str(c) if not pd.isna(c) else f"Column_{i}"
                    cols.append({
                        "name": col_name,
                        "index": i,
                        "type": self._detect_type(df.iloc[:, i])
                    })
                r, c = df.shape
                sample = df.head(5).to_dict("records")
                sample_clean = self._convert_to_serializable(sample)
                col_letter = self._num2col(c)
                result["sheets"][name] = {
                    "sheet_name": name,
                    "columns": cols,
                    "row_count": r,
                    "column_count": c,
                    "header_rows": 1,
                    "source_ref": {
                        "sheet": name,
                        "range": f"A1:{col_letter}{r}",
                        "row_start": 1,
                        "row_end": r
                    },
                    "sample_data": sample_clean
                }
        finally:
            wb.close()
        return self._convert_to_serializable(result)

    def _parse_csv(self, path: str) -> dict:
        df = None
        for enc in ["utf-8", "windows-1251", "cp1252", "latin1"]:
            for sep in [",", ";", "\t", "|"]:
                try:
                    df = pd.read_csv(path, encoding=enc, sep=sep, on_bad_lines="skip")
                    if not df.empty:
                        break
                except Exception:
                    continue
            if df is not None and not df.empty:
                break
        if df is None or df.empty:
            raise ValueError("Cannot read CSV file")
        cols = []
        for i, c in enumerate(df.columns):
            col_name = str(c) if not pd.isna(c) else f"Column_{i}"
            cols.append({
                "name": col_name,
                "index": i,
                "type": self._detect_type(df.iloc[:, i])
            })
        r, c = df.shape
        sample = df.head(5).to_dict("records")
        sample_clean = self._convert_to_serializable(sample)
        col_letter = self._num2col(c)
        result = {
            "file_type": "csv",
            "filename": os.path.basename(path),
            "sheets": {
                "default_sheet": {
                    "sheet_name": "default_sheet",
                    "columns": cols,
                    "row_count": r,
                    "column_count": c,
                    "header_rows": 1,
                    "source_ref": {
                        "sheet": "default_sheet",
                        "range": f"A1:{col_letter}{r}",
                        "row_start": 1,
                        "row_end": r
                    },
                    "sample_data": sample_clean
                }
            },
            "total_sheets": 1
        }
        return self._convert_to_serializable(result)

    def _num2col(self, n: int) -> str:
        res = ""
        while n > 0:
            n, rem = divmod(n - 1, 26)
            res = chr(65 + rem) + res
        return res