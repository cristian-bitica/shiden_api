FROM mcr.microsoft.com/devcontainers/python:3.13

# Java — required by PySpark
RUN apt-get update && apt-get install -y \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# uv — fast Python package manager
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Keeps Python from buffering stdout/stderr
ENV PYTHONUNBUFFERED=1
