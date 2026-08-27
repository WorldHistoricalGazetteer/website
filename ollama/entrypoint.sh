#!/bin/sh
# Start `ollama serve`, wait for its API, pull any not-yet-present preload models, then stay in the
# foreground as the serve process. Pull failures are logged and tolerated: a reachable server with a
# missing model degrades to a 404 on that model, which the ner service reports as "llm unavailable",
# whereas exiting here would put the container into a restart loop.
set -e

/bin/ollama serve &
SERVE_PID=$!

# The API is up as soon as `ollama list` succeeds; ~1s cold, but allow for a slow host.
i=0
while [ "$i" -lt 60 ]; do
    if /bin/ollama list >/dev/null 2>&1; then
        break
    fi
    i=$((i + 1))
    sleep 1
done

for model in $(echo "${OLLAMA_PRELOAD:-}" | tr ',' ' '); do
    [ -n "$model" ] || continue
    if /bin/ollama show "$model" >/dev/null 2>&1; then
        echo "whg-ollama: $model already present"
    else
        echo "whg-ollama: pulling $model ..."
        /bin/ollama pull "$model" || echo "whg-ollama: WARNING could not pull $model"
    fi
done

echo "whg-ollama: ready"
wait "$SERVE_PID"
