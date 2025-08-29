# ===== Stage 1: Build =====
FROM python:3.12-slim AS builder

# Evita buffers de saída
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copia apenas requirements para aproveitar cache
COPY src/process_status/requirements.txt .

# Instala dependências em um diretório isolado
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# Copia o restante do código
COPY src/process_status /app/

COPY src/utils /app/src/utils
COPY src/kafka_service /app/src/kafka_service

# ===== Stage 2: Runtime =====
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Copia a virtualenv do stage anterior
COPY --from=builder /opt/venv /opt/venv

# Copia o código da aplicação
COPY --from=builder /app /app

# Ajusta PATH para usar a virtualenv
ENV PATH="/opt/venv/bin:$PATH"

# Comando padrão
CMD ["python", "main.py"]
