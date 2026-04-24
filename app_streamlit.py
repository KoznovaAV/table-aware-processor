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
st.markdown("Загрузите XLSX или CSV файл для умного чанкинга и анализа.")

# --- Sidebar Settings ---
st.sidebar.header("Настройки чанкинга")
max_rows = st.sidebar.slider("Макс. строк в чанке", 50, 1000, 200, step=50)
max_cells = st.sidebar.slider("Макс. ячеек в чанке", 500, 10000, 5000, step=500)
include_profile = st.sidebar.checkbox("Включить профилирование", value=True)

# --- File Upload ---
uploaded_file = st.file_uploader("Выберите файл (.xlsx, .csv)", type=["xlsx", "csv"])

if uploaded_file:
    # Сохраняем во временный файл для обработки библиотеками (pandas/openpyxl требуют путь)
    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(uploaded_file.name)[1]) as tmp:
        tmp.write(uploaded_file.getvalue())
        tmp_path = tmp.name
    
    try:
        # 1. Парсинг (Чтение файла ОДИН раз)
        with st.spinner("Чтение и анализ структуры..."):
            parser = TableParser()
            parsed_data = parser.parse_file(tmp_path)
        
        # --- Отображение Метаданных ---
        st.subheader("Структура файла")
        col1, col2, col3 = st.columns(3)
        col1.metric("Тип файла", parsed_data["file_type"].upper())
        col2.metric("Листов/Таблиц", len(parsed_data["sheets_data"]))
        
        # Считаем общее количество строк во всех листах
        total_rows = sum(info["row_count"] for info in parsed_data["sheets_data"].values())
        col3.metric("Всего строк данных", total_rows)

        # Детали по листам
        for sheet_name, sheet_info in parsed_data["sheets_data"].items():
            with st.expander(f"Лист: {sheet_name} ({sheet_info['row_count']} строк)"):
                c1, c2 = st.columns([1, 2])
                with c1:
                    st.write("**Колонки:**")
                    # Формируем красивую таблицу колонок из метаданных
                    cols_list = []
                    for col_meta in sheet_info.get("columns_meta", []):
                        cols_list.append({
                            "Index": col_meta["original_index"],
                            "Type Hint": col_meta["type_hint"]
                        })
                    if cols_list:
                        st.dataframe(pd.DataFrame(cols_list), hide_index=True)
                
                with c2:
                    st.write("**Пример данных (сырой вид до чанкинга):**")
                    # Берем DataFrame из памяти (не читаем диск!)
                    df_preview = sheet_info["df"].head(5)
                    st.dataframe(df_preview)

        # 2. Чанкинг
        with st.spinner("Генерация чанков..."):
            chunker = TableChunker(max_rows_per_chunk=max_rows, max_cells_per_chunk=max_cells)
            # Передаем parsed_data целиком, чтобы chunker взял оттуда DF
            chunks = chunker.chunk_file(parsed_data, tmp_path)
        
        st.success(f"Создано чанков: {len(chunks)}")

        # --- Отображение Чанков ---
        st.subheader("Примеры чанков (первые 5)")
        for i, chunk in enumerate(chunks[:5]):
            with st.expander(f"Chunk #{i+1}: Rows {chunk['source_ref']['row_start']}-{chunk['source_ref']['row_end']}"):
                col_a, col_b = st.columns([1, 2])
                with col_a:
                    st.json(chunk["source_ref"])
                    st.write("**Headers:**")
                    st.write([h["name"] for h in chunk["context"]["header_details"]])
                
                with col_b:
                    st.write("**Data (Records):**")
                    st.json(chunk["data"][:3]) # Показываем только первые 3 строки данных
                    
                    st.write("**Text Projection (для LLM):**")
                    st.code(chunk["text_projection"], language="plaintext")

        # 3. Профилирование (Используем данные из памяти, если возможно)
        if include_profile:
            st.subheader("Профиль данных (Первый лист)")
            
            # Берем первый лист для демонстрации профиля
            first_sheet_name = list(parsed_data["sheets_data"].keys())[0]
            df_for_profile = parsed_data["sheets_data"][first_sheet_name]["df"]
            
            profiler = TableProfiler()
            profile = profiler.profile(df_for_profile)
            
            p_col1, p_col2 = st.columns(2)
            p_col1.metric("Строк в профиле", profile["total_rows"])
            p_col2.metric("Колонок", profile["total_columns"])
            
            if profile["warnings"]:
                st.warning("Предупреждения качества данных:")
                for w in profile["warnings"]:
                    st.write(f"- {w}")
            
            # Краткая сводка по колонкам
            st.write("**Статистика по колонкам:**")
            profile_df_data = []
            for col_name, col_stats in profile["columns"].items():
                row = {
                    "Column": col_name,
                    "Type": col_stats["type"],
                    "Nulls %": f"{col_stats['null_percentage']}%",
                    "Unique": col_stats["unique_count"]
                }
                if "stats" in col_stats:
                    row["Min"] = round(col_stats["stats"]["min"], 2)
                    row["Max"] = round(col_stats["stats"]["max"], 2)
                profile_df_data.append(row)
            
            st.dataframe(pd.DataFrame(profile_df_data), hide_index=True)

        # 4. Скачивание результата
        # Подготовка JSON для скачивания (убираем большие объекты DF, если они случайно попали)
        # В нашем chunker'е data - это dict records, это ок.
        result_json = {
            "metadata": {
                "filename": parsed_data["filename"],
                "file_type": parsed_data["file_type"],
                "sheets_count": len(parsed_data["sheets_data"])
            },
            "chunks": chunks,
            "profile_sample": profile if include_profile else None
        }
        
        st.download_button(
            label="Скачать результат (JSON)",
            data=json.dumps(result_json, ensure_ascii=False, indent=2),
            file_name=f"{uploaded_file.name}_processed.json",
            mime="application/json"
        )

    except Exception as e:
        st.error(f"ошибка обработки: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
    
    finally:
        # Очистка временного файла
        if os.path.exists(tmp_path):
            os.remove(tmp_path)