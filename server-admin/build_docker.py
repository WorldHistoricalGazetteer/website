import os
import sys
import subprocess
import requests

def get_latest_version(dockerhub_api):
    try:
        response = requests.get(f"{dockerhub_api}?page_size=1")
        response.raise_for_status()
        results = response.json().get('results', [])
        if results:
            latest_version = results[0]['name']
        else:
            latest_version = "0.0.0"
    except requests.RequestException as e:
        print(f"Failed to fetch tags from Docker Hub: {e}")
        sys.exit(1)
    return latest_version

def increment_version(version, version_type):
    major, minor, patch = map(int, version.split('.'))

    if version_type == 'major':
        major += 1
        minor = 0
        patch = 0
    elif version_type == 'minor':
        minor += 1
        patch = 0
    elif version_type == 'patch':
        patch += 1
    else:
        print(f"Unknown version type: {version_type}. Use 'major', 'minor', or 'patch'.")
        sys.exit(1)

    return f"{major}.{minor}.{patch}"

def build_and_tag_image(docker_image, new_version, no_cache=False):
    cmd = ["docker", "build"]
    if no_cache:
        # Full rebuild (ignores the layer cache) — use when you want to refresh
        # the base image / apt packages, not just Python deps.
        cmd.append("--no-cache")
    cmd += [
        "-t",
        f"{docker_image}:{new_version}",
        "--build-arg",
        "USER_NAME=whgadmin",
        ".",
    ]
    # The layered Dockerfile uses BuildKit-only features (`# syntax=` directive +
    # `RUN --mount=type=cache` pip cache). Enable BuildKit explicitly so the build
    # works regardless of the host's daemon default and without the buildx plugin
    # (Docker 18.09+ ships the BuildKit builder; DOCKER_BUILDKIT=1 activates it).
    env = {**os.environ, "DOCKER_BUILDKIT": "1"}
    try:
        subprocess.run(cmd, check=True, env=env)
    except subprocess.CalledProcessError:
        print("Failed to build the Docker image.")
        sys.exit(1)

def push_image(docker_image, new_version):
    try:
        subprocess.run(["docker", "push", f"{docker_image}:{new_version}"], check=True)
        print(f"Docker image pushed to Docker Hub with tag {docker_image}:{new_version}")
    except subprocess.CalledProcessError:
        print("Failed to push the Docker image.")
        sys.exit(1)

def main():
    # Ensure required environment variables are set
    docker_image = os.getenv("DOCKER_IMAGE", "worldhistoricalgazetteer/web")
    
    # Configuration
    dockerhub_api = f"https://hub.docker.com/v2/repositories/{docker_image}/tags/"
    
    # Parse args: version type + optional `push`, with an optional `--no-cache`
    # flag anywhere. Builds are CACHED by default now (the Dockerfile is layered
    # so a dependency bump rebuilds only the pip layer); pass --no-cache to force
    # a full rebuild (e.g. to refresh the base image / apt packages).
    args = sys.argv[1:]
    no_cache = "--no-cache" in args
    args = [a for a in args if a != "--no-cache"]

    if len(args) < 1 or len(args) > 2:
        print("Usage: build_docker.py [major|minor|patch] [push] [--no-cache]")
        sys.exit(1)

    version_type = args[0]
    push = args[1] if len(args) == 2 else None

    # Get the current version from Docker Hub
    current_version = get_latest_version(dockerhub_api)
    print(f"Current version: {current_version}")

    # Increment the version
    new_version = increment_version(current_version, version_type)
    print(f"New version: {new_version}")

    # Build and tag the Docker image
    build_and_tag_image(docker_image, new_version, no_cache=no_cache)

    # Push the new version to Docker Hub if the push parameter is passed
    if push == "push":
        push_image(docker_image, new_version)
    else:
        print(f"Docker image built and tagged as {docker_image}:{new_version}, but not pushed to Docker Hub.")

if __name__ == "__main__":
    main()
