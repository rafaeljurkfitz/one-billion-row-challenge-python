# Base: Python 3.12.1 Slim
FROM python:3.12.1-slim

# Configuração do ambiente
ENV PYTHONFAULTHANDLER=1 \
    PYTHONHASHSEED=random \
    PYTHONUNBUFFERED=1

# Instalar dependências do sistema para Polars e UV
# RUN apt-get update && apt-get install -y --no-install-recommends cargo rustc
    
# Copiar o UV binário da imagem oficial
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Criar diretório de trabalho
WORKDIR /app

# Copiar o arquivo de dependências e o script Python
COPY pyproject.toml uv.lock /app/
COPY ./src/ /app/

# Instalar as dependências usando UV
RUN uv sync --frozen

# Comando para rodar o script
CMD ["uv", "run", "./using_polars.py"]
