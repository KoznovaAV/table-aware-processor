import streamlit as st
import pandas as pd
import tempfile
import os
import json
from app.parser import TableParser
from app.chunker import TableChunker

st.set_page_config(page_title="Table-Aware Processor", layout="wide")
st.title("📊 Table-Aware Processing")
st.markdown("Загрузите XLSX или CSV файл для анализа структуры и умного чанкинга под RAG/LLM.")

max_chunk_bytes = st.sidebar.slider("Максимальный размер чанка (байт)", 1000, 50000, 10000, step=1000)
max_cells = st.sidebar.slider("Ячеек в чанке", 1000, 10000, 5000)
uploaded_file = st.file_uploader("Выберите файл", type=["xlsx", "csv"])

if uploaded_file:
    suffix = os.path.splitext(uploaded_file.name)[1]
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(uploaded_file.getvalue())
        tmp_path = tmp.name

    try:
        with st.spinner("Чтение и анализ структуры..."):
            parser = TableParser()
            parsed = parser.parse_file(tmp_path)

        st.subheader("📋 Структура таблицы")
        col1, col2, col3 = st.columns(3)
        col1.metric("Тип файла", parsed["file_type"].upper())
        col2.metric("Листов", parsed["total_sheets"])
        total_rows = sum(s["row_count"] for s in parsed["sheets"].values())
        col3.metric("Всего строк", total_rows)

        for sheet_name, sheet_data in parsed["sheets"].items():
            with st.expander(f"Лист: {sheet_name}", expanded=False):
                c1, c2 = st.columns(2)
                c1.write(f"**Строк:** {sheet_data['row_count']} | **Колонок:** {sheet_data['column_count']}")
                c2.write(f"**Диапазон:** {sheet_data['source_ref']['range']}")

                if sheet_data["columns"]:
                    cols_df = pd.DataFrame(sheet_data["columns"])
                    st.dataframe(cols_df, use_container_width=True)

                if sheet_data.get("sample_data"):
                    st.write("**Пример данных (первые 5 строк):**")
                    st.json(sheet_data["sample_data"])

        with st.spinner("Генерация чанков..."):
            chunker = TableChunker(max_chunk_bytes=max_chunk_bytes, max_cells_per_chunk=max_cells)
            chunks = chunker.chunk_file(parsed, tmp_path)

        st.subheader(f"🧩 Чанки (всего: {len(chunks)})")
        for i, chunk in enumerate(chunks[:5]):
            with st.expander(f"Чанк {i+1}: строки {chunk['source_ref']['row_start']}-{chunk['source_ref']['row_end']}"):
                st.write(f"**Диапазон:** {chunk['source_ref']['range']} | **Размер:** {chunk.get('chunk_size_bytes', 0)} байт")
                st.write("**🏷️ Заголовки:**")
                for h in chunk["context"]["headers"]:
                    st.write(f"  - {h}")
                st.write("**📦 Данные (первые 3 строки):**")
                st.json(chunk["data"][:3])
                st.text_area("📝 Текстовая проекция (для LLM/RAG)", chunk["text_projection"], height=150)

        # ВАЖНО: Удаляем DF из метаданных перед сохранением в JSON
        for sheet in parsed["sheets"].values():
            sheet.pop("df", None)

        result = {"metadata": parsed, "chunks": chunks}
        st.download_button(
            label="💾 Скачать результат (JSON)",
            data=json.dumps(result, ensure_ascii=False, indent=2, default=str),
            file_name=f"{os.path.splitext(uploaded_file.name)[0]}_processed.json",
            mime="application/json"
        )

    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
else:
    st.info("👆 Загрузите XLSX или CSV файл для начала работы")