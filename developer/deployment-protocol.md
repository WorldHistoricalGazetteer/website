## Preliminary Check of Environment Variables

- Ensure that `~/sites/env_template.py` is up-to-date, including the `DOCKER_IMAGE_TAG`:
```bash
cat ~/sites/env_template.py
```

## Deploy directly from `main`

### Without WebPack Build

```bash
cd ~/sites/whgazetteer-org
git pull origin main && sudo python3 ./server-admin/load_env.py && \
docker-compose -f docker-compose-autocontext.yml --env-file ./.env/.env up -d --force-recreate && \
docker ps
```

### With WebPack Build

```bash
cd ~/sites/whgazetteer-org
git pull origin main && sudo python3 ./server-admin/load_env.py && \
ENV_CONTEXT=whgazetteer-org npm run build:prod && \
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
docker exec -it web_whgazetteer-org_main bash -c "./manage.py refresh_mapdata_cache dataset <id>"
```
#### Refresh a Single Collection by its ID:
```bash
docker exec -it web_whgazetteer-org_main bash -c "./manage.py refresh_mapdata_cache collection <id>"
```
#### Refresh All Datasets
```bash
docker exec -it web_whgazetteer-org_main bash -c "./manage.py refresh_mapdata_cache dataset"
```
#### Refresh All Collections
```bash
docker exec -it web_whgazetteer-org_main bash -c "./manage.py refresh_mapdata_cache collection"
```
