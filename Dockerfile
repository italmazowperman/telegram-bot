FROM python:3.11-slim

WORKDIR /app

# Устанавливаем зависимости для reportlab
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Копируем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код
COPY . .

# Создаем директорию для временных файлов
RUN mkdir -p /tmp/reports

# Запускаем бота
CMD ["python", "bot.py"]