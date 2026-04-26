import pandas as pd


class TableProfiler:

    def profile(self, df: pd.DataFrame) -> dict:
        prof = {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "columns": {},
            "warnings": []
        }

        for c in df.columns:
            s = df[c]
            null_pct = round(s.isna().mean() * 100, 2)
            col_p = {
                "null_percentage": null_pct,
                "unique_count": int(s.nunique()),
                "dtype": str(s.dtype)
            }

            if pd.api.types.is_numeric_dtype(s):
                ns = s.dropna()
                if len(ns) > 0:
                    col_p["stats"] = {
                        "min": float(ns.min()),
                        "max": float(ns.max()),
                        "avg": round(float(ns.mean()), 2)
                    }
            elif s.dtype == "object":
                top = s.dropna().astype(str).value_counts().head(5)
                col_p["top_values"] = [
                    {"value": str(k), "count": int(v)} for k, v in top.items()
                ]

            prof["columns"][str(c)] = col_p

        if any(v["null_percentage"] > 90 for v in prof["columns"].values()):
            prof["warnings"].append("High null percentage (>90%) in some columns")

        return prof