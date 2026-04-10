# Deployment Protocol

## Overview

| Environment | Server path | Branch | URL |
|---|---|---|---|
| **Production** | `~/sites/whgazetteer-org` | `main` | `https://whgazetteer.org` |
| **Development** | `~/sites/dev-whgazetteer-org` | `staging` | `https://dev.whgazetteer.org` |

Both environments use auto-generated Docker Compose files, regenerated from
templates by `load_env.py` whenever settings change.

The development stack runs with reduced resources (2 Gunicorn workers, memory
limits on all containers, no Flower or Webpack) to protect the production site.

---

## Local Workflow

All code changes, branch merges, and webpack builds happen locally. The servers
only ever pull and restart.

### Webpack Build

Webpack bundles are committed to the repository and **must only be built
locally**. The webpack container is only included in the `local` Docker Compose
configuration.

```bash
npm run build
git add static/webpack/
git commit -m "Rebuild webpack bundles"
git push origin main
```

### Branch Merges

Merge feature branches into `main` locally:

```bash
git checkout main
git pull origin main
git merge feature-branch
git push origin main
```

To propagate to the development site, fast-forward `staging`:

```bash
git checkout staging
git merge --ff-only main
git push origin staging
```

### Feature Branch Testing

To test a feature branch on `dev.whgazetteer.org` without merging it into
`staging`, use the `--branch` option of the deploy script:

```bash
# On the DO server:
deploy --branch=api/crc-gateway          # deploy feature branch to dev
deploy --branch=api/crc-gateway --logs   # deploy and tail logs
```

When finished testing, return dev to the `staging` branch:

```bash
deploy                                   # reverts dev to staging (default)
```

**Note:** `--branch` is only allowed for the `dev` environment. Production
always deploys `main`.

---

## Deploy Script

A single script on the DO server handles all deployment operations:

```
~/sites/whgazetteer-org/server-admin/deploy.sh
```

### Quick Reference

```bash
deploy                           # dev, restart web (staging branch)
deploy prod                      # prod, restart web
deploy pull                      # dev, pull only
deploy prod full                 # prod, restart all containers
deploy restart --celery          # dev, restart web + celery
deploy prod recreate --migrate   # prod, full recreation + migrations
deploy status                    # dev, show containers
deploy prod status               # prod, show containers
deploy --branch=api/crc-gateway  # dev, deploy a feature branch
deploy pull --branch=api/crc-gateway  # dev, pull a feature branch only
```

### Arguments

| Argument | Description |
|---|---|
| `dev` | Target development environment (default) |
| `prod` | Target production environment |
| `pull` | Pull code only, no restart |
| `restart` | Pull + regenerate config + restart web (default) |
| `full` | Pull + regenerate config + restart all containers |
| `recreate` | Pull + regenerate config + tear down + recreate all |
| `status` | Show running containers |
| `--branch=<name>` | Override the dev branch (default: `staging`; not allowed for prod) |
| `--celery` | Also restart celery worker and beat (with `restart`) |
| `--migrate` | Run Django migrations after deploy |
| `--logs` | Tail web container logs after deploy |

### Setup

Add an alias on the DO server (in `~/.bashrc`):

```bash
alias deploy='~/sites/whgazetteer-org/server-admin/deploy.sh'
```

---

## Manual Operations

### Apply Django Migrations

```bash
# Production
docker exec -it web_whgazetteer-org_main bash -c "./manage.py migrate"

# Development
docker exec -it web_dev-whgazetteer-org_staging bash -c "./manage.py migrate"
```

### Check Logs

```bash
# Production
docker logs -f web_whgazetteer-org_main

# Development
docker logs -f web_dev-whgazetteer-org_staging
```

### Regenerate Map Data

Substitute the appropriate container name for the environment.

```bash
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata --datasets --id <id>
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata --collections --id <id>
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata --datasets
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata --collections
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata --clear-only
```

---

## Notes

- **Webpack** runs only in local development. Bundles are built locally,
  committed to git, and deployed via `git pull`.

- **Database backups** run nightly at 01:00 UTC via `pg_basebackup`, retained
  for 2 days at `~/backup/whgazetteer-org/`, and pulled to Pitt CRC NFS
  (`/ix1/ishi/backups/`) at 03:00 UTC.

- **Elasticsearch** is hosted on the Pitt CRC. The local ES and Kibana services
  have been decommissioned.

- **Development resource limits** are in `env_template.py` under
  `dev-whgazetteer-org`: 2 Gunicorn workers, memory caps on all containers,
  Flower disabled.
