## Preliminary Check of Environment Variables

- Ensure that `~/sites/env_template.py` is up-to-date, including the `DOCKER_IMAGE_TAG`:
```bash
cat ~/sites/env_template.py
```

## Deploy directly from `main`

### Update

This is sufficient for changes to templates and/or static files (including Webpack bundles):

```bash
cd ~/sites/whgazetteer-org && \
git pull origin main --rebase
```

### Restart

#### If you have changed Django code and not Celery:

```bash
cd ~/sites/whgazetteer-org && \
git pull origin main --rebase && \
sudo python3 ./server-admin/load_env.py && \
docker-compose -f docker-compose-autocontext.yml --env-file ./.env/.env restart web
```

#### If you have changed Django code AND Celery:

```bash
cd ~/sites/whgazetteer-org && \
git pull origin main --rebase && \
sudo python3 ./server-admin/load_env.py && \
docker-compose -f docker-compose-autocontext.yml --env-file ./.env/.env restart web celery_worker celery_beat
```

#### Full Restart:

```bash
cd ~/sites/whgazetteer-org && \
git pull origin main --rebase && \
sudo python3 ./server-admin/load_env.py && \
docker-compose -f docker-compose-autocontext.yml --env-file ./.env/.env restart
```

#### Full Recreation:

```bash
cd ~/sites/whgazetteer-org && \
git pull origin main --rebase && \
sudo python3 ./server-admin/load_env.py && \
docker-compose -f docker-compose-autocontext.yml --env-file ./.env/.env up -d --force-recreate && \
docker ps
```

### If ABSOLUTELY necessary, include WebPack Build (for Javascript & CSS bundling)

> NOTE: Server resources are so tightly constrained that running WebPack Build may result in significant outage and may crash the Elasticsearch service. Bundles should be
> built locally (usually in a development Docker network) and deployed through GitHub.

Server resources are too limited for rebuilding while the main site is running:

```bash
# 1. Update from repository and rebuild docker-compose file
cd ~/sites/whgazetteer-org && \
git pull origin main --rebase && \
sudo python3 ./server-admin/load_env.py && \

# 2. Stop the main site to free up ALL resources
docker-compose -f docker-compose-autocontext.yml --env-file ./.env/.env down --remove-orphans && \

# 3. Reset the Network
# Try to remove it first. '|| true' ensures the script continues even if the network is already gone.
(docker network rm whgazetteer-org_whgazetteer-org 2>/dev/null || true) && \
docker network create -d bridge whgazetteer-org_whgazetteer-org && \

# 4. Run the webpack build (using the free resources)
docker-compose -f webpack.build.yml -p whgazetteer-org-build run --rm webpack-builder && \

# 5. Restart the main site
docker-compose -f docker-compose-autocontext.yml --env-file ./.env/.env up -d --force-recreate && \
docker ps
```

## Deploy Staging

- Switch to the `dev-whgazetteer-org` site, pull updates, and update environment:
```bash
cd ~/sites/dev-whgazetteer-org
git pull origin staging && sudo python3 ./server-admin/load_env.py
```
- _Or, to switch to a different branch_
```bash
git fetch origin
git checkout staging  # Replace "staging" with the desired branch name
```
- If all is OK, restart network:
```bash
docker-compose -f docker-compose-autocontext.yml --env-file ./.env/.env down && \
docker-compose -f docker-compose-autocontext.yml --env-file ./.env/.env up -d && \
docker ps
```

#### If necessary, apply Django migrations
```bash
docker exec -it web_dev-whgazetteer-org_staging bash -c "./manage.py makemigrations"
```
```bash
docker exec -it web_dev-whgazetteer-org_staging bash -c "./manage.py migrate"
```

#### Check Logs
```bash
docker logs -f postgres_dev-whgazetteer-org_staging
```
```bash
docker logs -f web_dev-whgazetteer-org_staging
```
```bash
docker logs -f celery-worker_dev-whgazetteer-org_staging
```

#### Monitor Django Logs
```
# For example, `validation` log on `staging` branch:
docker exec -it web_dev-whgazetteer-org_staging bash -c "tail -f ./whg/logs/validation.log"

```

```
# For example, `authentication` log on `orcid-integration` branch:
docker exec -it web_dev-whgazetteer-org_orcid-integration bash -c "tail -f ./whg/logs/authentication.log"

```

## Deploy to Main from Staging

Firstly, merge `staging` into `main`:
```bash
cd ~/sites/whgazetteer-org
git fetch origin
git checkout main
git pull origin main
git merge origin/staging -m "Merging staging into main"
# At this point, Git will attempt to merge the staging branch into the main branch. If there are merge conflicts,
# Git will notify you, and you will need to manually resolve these conflicts.
# After resolving conflicts, use `git add <resolved-files>` to stage the resolved files,
# and `git commit` to complete the merge.
git push origin main
```

- Then ensure that `whgazetteer-org/server-admin/env_template.py` is up-to-date, including the `DOCKER_IMAGE_TAG`:
```bash
cat ~/sites/env_template.py
```

- Then update the root static folder, which may include webpack updates which would not otherwise be modified in the absence of a webpack service in this docker network:
```bash
# Synchronise from dev-whgazetteer-org/static/ to whgazetteer-org/static/, overwriting older files but deleting none
rsync -a ~/sites/dev-whgazetteer-org/static/ ~/sites/whgazetteer-org/static/
# Ensure correct ownerships
sudo chown -R whgadmin:whgadmin ~/sites/whgazetteer-org/static/
```

- Then switch to the `whgazetteer-org` site, pull updates, update environment, and restart network:
```bash
cd ~/sites/whgazetteer-org
git pull origin main && sudo python3 ./server-admin/load_env.py
docker-compose -f docker-compose-autocontext.yml --env-file ./.env/.env down && \
docker-compose -f docker-compose-autocontext.yml --env-file ./.env/.env up -d && \
docker ps
# For safety's sake, switch back to staging site
cd ~/sites/dev-whgazetteer-org
```

#### If necessary, apply Django migrations
```bash
docker exec -it web_whgazetteer-org_main bash -c "./manage.py makemigrations"
```
```bash
docker exec -it web_whgazetteer-org_main bash -c "./manage.py migrate"
```

#### Check Logs
```bash
docker logs -f postgres_whgazetteer-org_main
```
```bash
docker logs -f web_whgazetteer-org_main
```
```bash
docker logs -f celery-worker_whgazetteer-org_main
```

### Regenerate Map Data
Occasionally, it may be necessary to force a refresh of mapdata. You can refresh the data for a single dataset, a collection, all datasets, or all collections.

#### Refresh a Single Dataset by its ID:
```bash
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata --datasets --id <id>
```
#### Refresh a Single Collection by its ID:
```bash
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata --collections --id <id>
```
#### Refresh All Datasets
```bash
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata --datasets
```
#### Refresh All Collections
```bash
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata --collections
```
#### Refresh All Datasets and Collections
```bash
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata
```
#### Clear Cache Only (Will regenerate on first access)
```bash
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata --clear-only
```
