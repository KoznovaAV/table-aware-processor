# 📊 Table-Aware Processor

Модуль обработки табличных файлов (`.xlsx`, `.csv`) с сохранением структуры и умным чанкингом для RAG/LLM-систем.

## 🚀 Live Demo

Проект развернут в облаке и доступен онлайн:

*   **Web Interface:** [https://table-aware-procapp-kozinaki.streamlit.app](https://table-aware-procapp-kozinaki.streamlit.app)
*   **REST API Docs:** [https://table-aware-processor.onrender.com/docs](https://table-aware-processor.onrender.com/docs)

> ⚠️ **Важно:** Бесплатные инстансы "засыпают" после периода неактивности. Первый запрос может занять 30–60 секунд для "пробуждения" сервера.

## ✨ Возможности

*   ✅ **Парсинг:** Чтение `.xlsx` и `.csv` с автоопределением кодировки и разделителей.
*   ✅ **Структура:** Извлечение листов, колонок, типов данных (`string/number/date/bool`), заголовков и ссылок на источник.
*   ✅ **Умный чанкинг:** Разбиение таблиц по строкам/ячейкам с наследованием заголовков и контекста.
*   ✅ **Текстовая проекция:** Генерация LLM-готового текстового представления каждого чанка.
*   ✅ **Профилирование:** Анализ пропусков, уникальных значений и статистики данных.
*   ✅ **FastAPI:** REST API с настраиваемыми параметрами чанкинга (`max_rows`, `max_cells`).
*   ✅ **Чистый код:** Соответствие PEP8 (проверено ruff), полная типизация.

## 🛠️ Инструкция по запуску

### 1. Локальный запуск (Python)

```bash
# Клонирование репозитория
git clone https://github.com/KoznovaAV/table-aware-processor.git
cd table-aware-processor

# Создание виртуального окружения
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# или
.\.venv\Scripts\activate   # Windows

# Установка зависимостей
pip install -r requirements.txt
```
#### ▶️ Запуск API сервера

Прямой запуск Python-модуля:
```bash
python app/main.py
```
Или через uvicorn:
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```
👉 Откройте http://localhost:8000/docs для тестирования через Swagger UI.
#### ▶️ Запуск Web-интерфейса (Streamlit)
```bash
streamlit run app_streamlit.py
```
👉 Откройте http://localhost:8501 в браузере.
### 2. Запуск через Docker
```bash
docker build -t table-processor .
```
#### ▶️ Запуск API
```bash
docker run -p 8000:8000 table-processor
```
API доступно по адресу: http://localhost:8000/docs

#### ▶️ Запуск Streamlit UI
```bash
docker run -p 8501:8501 table-processor streamlit run app_streamlit.py --server.port=8501 --server.address=0.0.0.0
```
Интерфейс доступен по адресу: http://localhost:8501
## 📡 Использование API
### Основной эндпоинт для обработки файлов: POST /process
Параметры:
```bash
`file` — файл `.xlsx` или `.csv` (multipart/form-data)
`max_bytes` — максимальный размер чанка в байтах (по умолчанию `50000`)
`max_cells` — максимальное количество ячеек в чанке (по умолчанию `5000`)
```
Пример запроса (curl):
```bash
curl -X POST "http://localhost:8000/process?max_bytes=50000&max_cells=5000" \
  -F "file=@primer_1.xlsx"
```
Ответ: JSON объект, содержащий метаданные файла и массив чанков с текстовыми проекциями.
