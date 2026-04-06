FROM mcr.microsoft.com/devcontainers/base:ubuntu-22.04

USER vscode
RUN curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update

ENV PATH="/home/vscode/.local/bin:${PATH}"
