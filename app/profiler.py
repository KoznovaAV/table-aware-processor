import pandas as pd
import numpy as np
from typing import Dict, Any

class TableProfiler:

    def profile(self, df: pd.DataFrame) -> dict:
        """
        Создает общий профиль таблицы: статистика по строкам/колонкам,
        типы данных, пропуски и предупреждения о качестве данных.
        """
        if df.empty:
            return {
                "total_rows": 0,
                "total_columns": 0,
                "columns": {},
                "warnings": ["Empty dataframe"]
            }

        prof = {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "columns": {},
            "warnings": []
        }

        for c in df.columns:
            s = df[c]
            non_null = s.dropna()
            null_count = s.isna().sum()
            null_pct = round((null_count / len(s)) * 100, 2) if len(s) > 0 else 0.0
            
            # Определение типа (согласовано с логикой chunker)
            col_type = self._infer_type(s)
            
            col_p = {
                "name": str(c),
                "type": col_type,
                "null_percentage": null_pct,
                "unique_count": int(s.nunique()),
                "dtype_pandas": str(s.dtype) # Сохраняем оригинальный dtype pandas
            }

            # Статистика в зависимости от типа
            if col_type == "number" and not non_null.empty:
                ns = pd.to_numeric(non_null, errors='coerce')
                col_p["stats"] = {
                    "min": float(ns.min()) if not ns.empty else None,
                    "max": float(ns.max()) if not ns.empty else None,
                    "avg": round(float(ns.mean()), 2) if not ns.empty else None
                }
            elif col_type in ["string", "mixed"]:
                # Топ значений для строк
                top = non_null.astype(str).value_counts().head(5)
                col_p["top_values"] = [
                    {"value": str(k), "count": int(v)} for k, v in top.items()
                ]
            elif col_type == "date":
                # Можно добавить min/max дату, если нужно
                pass

            prof["columns"][str(c)] = col_p

        # Генерация предупреждений
        self._add_warnings(prof)

        return prof

    def _infer_type(self, series: pd.Series) -> str:
        """Упрощенная логика определения типа для профайлера"""
        s = series.dropna()
        if s.empty:
            return "empty"
        
        if pd.api.types.is_numeric_dtype(s):
            return "number"
        if pd.api.types.is_datetime64_any_dtype(s):
            return "date"
        
        # Проверка на строковые даты
        try:
            pd.to_datetime(s.head(10), errors='raise')
            return "date"
        except:
            pass
            
        return "string"

    def _add_warnings(self, prof: dict):
        """Добавляет предупреждения на основе статистики"""
        warnings = []
        
        # 1. Много пропусков
        high_null_cols = [
            name for name, data in prof["columns"].items() 
            if data["null_percentage"] > 90
        ]
        if high_null_cols:
            warnings.append(f"Columns with >90% nulls: {', '.join(high_null_cols[:3])}")

        # 2. Высокая кардинальность строк
        high_card_cols = [
            name for name, data in prof["columns"].items() 
            if data.get("type") == "string" and data.get("unique_count", 0) > 100
        ]
        if high_card_cols:
            warnings.append(f"High cardinality string columns: {', '.join(high_card_cols[:3])}")

        # 3. Смешанные типы (эвристика: если unique_count близок к row_count, но тип object)
        # Это может означать, что в колонке лежат разные сущности
        
        prof["warnings"] = warnings