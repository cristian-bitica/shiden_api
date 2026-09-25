FROM mcr.microsoft.com/devcontainers/python:3.13

# Java — required by PySpark
RUN apt-get update && apt-get install -y \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Babashka — Clojure scripting tool installed via Homebrew for Agents Swarm
# Homebrew refuses to run as root, so install it as the non-root vscode user.
USER vscode
RUN /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)" \
    && eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)" \
    && brew install borkdude/brew/babashka tmux
USER root

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PATH="/home/linuxbrew/.linuxbrew/bin:${PATH}"

# uv — fast Python package manager
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Keeps Python from buffering stdout/stderr
ENV PYTHONUNBUFFERED=1
