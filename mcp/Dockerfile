FROM python:3.12-slim-bookworm
WORKDIR /srv
COPY pyproject.toml ./
COPY whg_mcp ./whg_mcp
RUN pip install --no-cache-dir . && useradd -r -u 1000 mcp
USER mcp
ENV WHG_MCP_TRANSPORT=streamable-http WHG_MCP_HOST=0.0.0.0 WHG_MCP_PORT=8000
EXPOSE 8000
CMD ["python", "-m", "whg_mcp.server"]
