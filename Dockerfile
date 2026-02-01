FROM python:3.11-slim

WORKDIR /app

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Сначала устанавливаем numpy отдельно
COPY requirements.txt .
RUN pip install --no-cache-dir "numpy==1.24.3"
RUN pip install --no-cache-dir -r requirements.txt

# Копируем остальной код
COPY . .

# Создаем директорию для временных файлов
RUN mkdir -p /tmp/reports

# Запускаем бота
CMD ["python", "bot.py"]
