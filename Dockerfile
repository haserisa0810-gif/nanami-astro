FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

WORKDIR /app

# WeasyPrint PDF生成に必要なライブラリと日本語フォント
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        fontconfig \
        fonts-noto-cjk \
        libpango-1.0-0 \
        libpangoft2-1.0-0 \
        libcairo2 \
        libgdk-pixbuf-2.0-0 \
        libffi8 \
        shared-mime-info \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["sh", "-c", "python -m uvicorn routes:app --host 0.0.0.0 --port ${PORT:-8080}"]
