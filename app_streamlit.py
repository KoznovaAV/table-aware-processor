import streamlit as st
import pandas as pd
import tempfile
import os
import json
from app.parser import TableParser
from app.chunker import TableChunker
from app.profiler import TableProfiler

st.set_page_config(page_title="Table-Aware Processor", layout="wide")
st.title("Table-Aware Processing")
st.markdown("Загрузите XLSX или CSV файл для анализа")

max_chunk_bytes = st.sidebar.slider("Максимальный размер чанка (байт)", 1000, 50000, 10000, step=1000)
max_cells = st.sidebar.slider("Ячеек в чанке", 1000, 10000, 5000)
include_profile = st.sidebar.checkbox("Профилирование", value=True)

uploaded_file = st.file_uploader("Выберите файл", type=["xlsx", "csv"])

if uploaded_file:
    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(uploaded_file.name)[1]) as tmp:
        tmp.write(uploaded_file.getvalue())
        tmp_path = tmp.name
    
    try:
        parser = TableParser()
        parsed = parser.parse_file(tmp_path)
        
        st.subheader("Структура таблицы")
        col1, col2, col3 = st.columns(3)
        col1.metric("Тип файла", parsed["file_type"].upper())
        col2.metric("Листов", parsed["total_sheets"])
        
        total_rows = sum(s["row_count"] for s in parsed["sheets"].values())
        col3.metric("Всего строк", total_rows)
        
        for sheet_name, sheet_data in parsed["sheets"].items():
            with st.expander(f"Лист: {sheet_name}"):
                st.write(f"Строк: {sheet_data['row_count']}, Колонок: {sheet_data['column_count']}")
                st.write(f"Диапазон: {sheet_data['source_ref']['range']}")
                
                cols_df = pd.DataFrame(sheet_data["columns"])
                st.dataframe(cols_df)
                
                st.write("Пример данных:")
                st.json(sheet_data["sample_data"])
        
        chunker = TableChunker(max_chunk_bytes=max_chunk_bytes, max_cells_per_chunk=max_cells)
        chunks = chunker.chunk_file(parsed, tmp_path)
        
        st.subheader(f"Чанки (всего: {len(chunks)})")
        for i, chunk in enumerate(chunks[:5]):
            with st.expander(f"Чанк {i+1}: строки {chunk['source_ref']['row_start']}-{chunk['source_ref']['row_end']}"):
                st.write(f"Диапазон: {chunk['source_ref']['range']}")
                st.write("Заголовки:")
                for h in chunk["context"]["headers"]:
                    st.write(f"  - {h['name']} ({h['type']})")
                st.write("Данные (первые 3 строки):")
                st.json(chunk["data"][:3])
                st.text_area("Текстовая проекция", chunk["text_projection"], height=200)
        
        if include_profile:
            if uploaded_file.name.endswith(".xlsx"):
                df = pd.read_excel(tmp_path)
            else:
                df = pd.read_csv(tmp_path, encoding="utf-8", on_bad_lines="skip")
            
            profiler = TableProfiler()
            profile = profiler.profile(df)
            
            st.subheader("Профиль данных")
            st.write(f"Всего строк: {profile['total_rows']}, Колонок: {profile['total_columns']}")
            
            if profile["warnings"]:
                st.warning("Предупреждения:")
                for w in profile["warnings"]:
                    st.write(f"- {w}")
            
            for col_name, col_profile in profile["columns"].items():
                with st.expander(f"Колонка: {col_name}"):
                    st.write(f"Тип: {col_profile['dtype']}")
                    st.write(f"Пропуски: {col_profile['null_percentage']}%")
                    st.write(f"Уникальных: {col_profile['unique_count']}")
                    if "stats" in col_profile:
                        st.write("Статистики:", col_profile["stats"])
                    if "top_values" in col_profile:
                        st.write("Топ значения:", col_profile["top_values"])
        
        result = {"metadata": parsed, "chunks": chunks}
        if include_profile:
            result["profile"] = profile
        
        st.download_button("Скачать JSON", json.dumps(result, ensure_ascii=False, indent=2), "result.json", "application/json")
    
    finally:
        os.remove(tmp_path)