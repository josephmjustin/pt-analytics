# Stage 1: Build
FROM python:3.10-slim AS builder
WORKDIR /pt-analytics
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: Runtime
FROM python:3.10-slim
WORKDIR /pt-analytics
COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY src ./src/
EXPOSE 8080
RUN useradd app
USER app
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8080"]