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

All code changes, promotions, and webpack builds happen locally. The servers
only ever pull and restart.

### Webpack Build

Webpack bundles are committed to the repository and **must only be built
locally**. The webpack container is only included in the `local` Docker Compose
configuration.

⚠️ Use **`npm run build:prod`**. Bare `npm run build` starts webpack in **watch
mode** and never exits.

```bash
npm run build:prod
git add static/webpack/
git commit -m "Rebuild webpack bundles"
git push origin staging     # day-to-day work happens on staging
```

Because the bundles are committed and `STATIC_ROOT` is `<BASE_DIR>/static`, a
bundle change needs **no `--collectstatic`** on deploy.

### Promoting staging to production

`staging` is the development branch and `main` is production. Work lands on
`staging` first, is verified on `dev.whgazetteer.org`, and is then promoted to
`main` **one commit at a time**.

⚠️ **NEVER merge `staging` into `main`.** `staging` carries dev-only apps (GRACE)
that must not reach production, and the merge would also collide with the ~139
generated bundle files under `static/webpack/`. The branches have diverged, so
`git merge --ff-only` between them fails outright in either direction.

⚠️ **Promote by EXPLICIT SHA, never by looping a commit range.** Iterating
`git log origin/main..origin/staging` to promote "the new ones" is how three GRACE
commits once reached `main`: most of the range fails noisily, but some hunks apply,
and the failures scroll past looking like success.

A plain `git cherry-pick` conflicts on the committed bundles, so apply the
**source-only** diff and rebuild once on `main`:

```bash
git checkout main
git pull --ff-only origin main

# 1. Source only — exclude the generated bundles.
git diff <sha>^ <sha> -- . ':(exclude)static/webpack' | git apply --index
git commit -C <sha>          # reuse the original commit message

# 2. One rebuild on main, as its own commit.
npm run build:prod
git add -A static/webpack
git commit -m "build(webpack): rebuild bundles on main"

git push origin main
```

Before pushing, confirm the promoted sources match what was verified on dev —
this should print nothing:

```bash
git diff origin/staging main -- <the files the commit touched>
```

Then deploy (see **Deploy Script** below). For a change that is only Python,
templates, JS or bundles, `deploy prod restart` is sufficient — **no `--migrate`,
no `--celery`, no `--collectstatic`**. Add `--migrate` only for a schema change and
`--celery` only when Celery *task* code changed, since a plain restart leaves the
worker on stale modules.

`main` is a protected branch: it cannot be force-pushed without temporarily
editing the branch-protection rule.

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
