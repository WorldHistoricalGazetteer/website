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

def build_and_tag_image(docker_image, new_version):
    try:
        subprocess.run([
            "docker", 
            "build",
            "--no-cache",
            "-t", 
            f"{docker_image}:{new_version}", 
            "--build-arg", 
            "USER_NAME=whgadmin", 
            "."
        ], check=True)
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
    
    # Handle version type and push parameter
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: build_docker.py [major|minor|patch] [push]")
        sys.exit(1)

    version_type = sys.argv[1]
    push = sys.argv[2] if len(sys.argv) == 3 else None

    # Get the current version from Docker Hub
    current_version = get_latest_version(dockerhub_api)
    print(f"Current version: {current_version}")

    # Increment the version
    new_version = increment_version(current_version, version_type)
    print(f"New version: {new_version}")

    # Build and tag the Docker image
    build_and_tag_image(docker_image, new_version)

    # Push the new version to Docker Hub if the push parameter is passed
    if push == "push":
        push_image(docker_image, new_version)
    else:
        print(f"Docker image built and tagged as {docker_image}:{new_version}, but not pushed to Docker Hub.")

if __name__ == "__main__":
    main()
