FROM python:3.10-slim

WORKDIR /app

# Системные зависимости для pandas/openpyxl
RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000 8501

# Запуск по умолчанию: API
# Для Streamlit: docker run -p 8501:8501 table-processor streamlit run app_streamlit.py --server.port=8501 --server.address=0.0.0.0
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]