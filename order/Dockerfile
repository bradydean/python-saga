FROM registry.access.redhat.com/ubi8/python-39:latest

USER root

RUN useradd app && mkdir /app && chown -R app:app /app

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt && rm /app/requirements.txt

USER app

COPY order.py /app/order.py
