# WHG local LLM host (Ollama) — Map-your-Data place-name extraction, place#211.
#
# Thin wrapper over the official image whose only job is to make the container SELF-SEEDING: on first
# start it pulls the model named in OLLAMA_PRELOAD into the mounted model volume, then hands over to
# `ollama serve`. Without this, a fresh volume would serve 404s until someone remembered to exec a
# pull by hand.
#
# ONE instance per host, defined in the PRODUCTION compose stack only and shared with the dev stack
# over the external `whg-llm` bridge network (see compose/docker-compose-template.j2). The model is
# CPU-bound and the droplet has no GPU, so a second instance would simply contend for the same cores.
FROM ollama/ollama:0.33.1

# The model to have resident before the service reports healthy. Comma-separate for more than one.
# qwen3:0.6b (522 MB, Apache-2.0) was chosen empirically, not by size: on the place#211 benchmark it
# ties qwen2.5:0.5b on TNA REQ 2 (27/33) but is the smallest model that can carry a prompt naming
# rivers, mountains and seas without collapsing into a repetition loop — which lifts a travel
# narrative from 6/9 to 8/9. qwen2.5:1.5b is 2.5x the cost and worse at both.
ENV OLLAMA_PRELOAD=qwen3:0.6b

# chmod here rather than relying on the mode git recorded — a checkout that loses the exec
# bit would otherwise fail the container at runtime, not at build.
COPY entrypoint.sh /usr/local/bin/whg-ollama-entrypoint.sh
RUN chmod +x /usr/local/bin/whg-ollama-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/whg-ollama-entrypoint.sh"]
