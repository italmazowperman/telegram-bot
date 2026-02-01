FROM python:3.11-slim

WORKDIR /app

# Устанавливаем системные зависимости для reportlab и numpy
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Копируем и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код
COPY . .

# Создаем директорию для временных файлов
RUN mkdir -p /tmp/reports

# Запускаем бота
CMD ["python", "bot.py"]
