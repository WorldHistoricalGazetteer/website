# WHG Workbench real-time collaboration server (Hocuspocus). Built on the host via the compose
# `build:` directive (same pattern as the commented `embeddings` service) — no registry push needed.
FROM node:18-alpine

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund

COPY server.js ./

ENV NODE_ENV=production
# Container listens on HOCUSPOCUS_PORT (from .env); nginx proxies wss://<host>/collab/ to it.
CMD ["node", "server.js"]
