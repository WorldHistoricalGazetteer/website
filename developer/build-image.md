## Store Versioned Images in Docker Hub

The web/celery services run the prebuilt image `worldhistoricalgazetteer/web:<x.x.x>`
from Docker Hub. The repo is bind-mounted into the container at `/app`, so **source**
changes (Python, templates, committed webpack bundles) go live on a plain deploy
`restart`. **`requirements.txt` / pip-dependency changes are baked into the image at
build time**, so they need a new image built + pushed, and the compose `image:` tag
pointed at it (the version to install per environment is recorded in the DO config).

### Build + push (auto-increments the Docker Hub tag)
Helper calculates the next version number and pushes the built image to Docker Hub:
```bash
# Usage: build_docker.py [major|minor|patch] [push] [--no-cache]
python3 ./server-admin/build_docker.py patch push
```

Builds are **cached by default**. The `Dockerfile` is layered so that:
- system packages (apt) and the virtualenv are their own cached layers, and
- the Python-deps layer's cache key is the **content of `requirements.txt`**.

So a dependency bump rebuilds **only the pip layer** (≈1–2 min) and reuses the slow
`apt-get install` layer. A `.dockerignore` keeps the build context tiny (the Dockerfile
only needs `requirements.txt`; everything else is bind-mounted at runtime).

Pass `--no-cache` to force a full rebuild — e.g. to refresh the base image or apt
packages, not just Python deps:
```bash
python3 ./server-admin/build_docker.py patch push --no-cache
```

> Requires BuildKit (Docker ≥ 23 enables it by default) for the `# syntax` directive
> and the pip cache mount in the `Dockerfile`.

### Manual build / push
```bash
docker build -t worldhistoricalgazetteer/web:<x.x.x> --build-arg USER_NAME=whgadmin .
docker push worldhistoricalgazetteer/web:<x.x.x>
```
