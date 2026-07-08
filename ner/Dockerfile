# WHG place-name extraction service (Map-your-Data NER). Built on the host via the compose `build:`
# directive (same pattern as ../hocuspocus) so spaCy + the model never touch the pulled web image.
# Django (workbench.ner_extract) reaches it over the compose network as http://ner:${NER_PORT}.
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
 && python -m spacy download en_core_web_sm

COPY app.py ./

ENV NER_PORT=8020
# Listens on all interfaces INSIDE the container; compose does not publish a host port, so it is only
# reachable from other services on the same docker network (i.e. Django).
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${NER_PORT:-8020}"]
