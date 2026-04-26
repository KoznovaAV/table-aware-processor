# Используем легкий образ Python
FROM python:3.10-slim

# Рабочая директория внутри контейнера
WORKDIR /app

# Копируем файл зависимостей и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь остальной код
COPY . .

# Открываем порты для API (8000) и Streamlit (8501)
EXPOSE 8000 8501

# Команда запуска (по умолчанию запускаем API)
# Если нужно запустить Streamlit, меняем CMD на: streamlit run app_streamlit.py --server.port=8501 --server.address=0.0.0.0
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]