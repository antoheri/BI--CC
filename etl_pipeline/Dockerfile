FROM alpine:latest

RUN apk add --update --no-cache python3 py3-pip libtool krb5-libs krb5-dev unixodbc unixodbc-dev

WORKDIR /etl_pipeline

COPY requirements.txt .

RUN python3 -m venv .venv

RUN .venv/bin/pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["/etl_pipeline/.venv/bin/python", "etl_pipeline.py"]
