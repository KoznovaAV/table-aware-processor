# Table-Aware Processing

Модуль обработки табличных файлов (`.xlsx`, `.csv`) с сохранением структуры и умным чанкингом для RAG/LLM-систем.

## 📋 Возможности
✅ Чтение `.xlsx` и `.csv` с автоопределением кодировки и разделителей  
✅ Извлечение структуры: листы, колонки, типы (`string/number/date/bool/mixed/empty`), шапка, `source_ref`  
✅ Умный чанкинг по строкам/ячейкам с наследованием заголовков и точной текстовой проекцией  
✅ Профилирование: пропуски, статистики, top-N, предупреждения  
✅ FastAPI с настраиваемыми параметрами чанкинга (`max_rows`, `max_cells`)  
✅ Соответствие PEP8 (ruff), готовый `run_demo.py` и примеры вывода

## 🛠️ Запуск

```bash
# 1. Создание окружения
py -m venv .venv
.\.venv\Scripts\activate  # Windows
source .venv/bin/activate # macOS/Linux

# 2. Установка зависимостей
pip install -r requirements.txt

# 3. Запуск демо (обработает файлы из examples/ и сохранит JSON в output/)
py run_demo.py

# 4. Запуск API
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000