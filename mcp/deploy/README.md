# Deploying whg-mcp (production droplet)

Public URL: `https://mcp.whgazetteer.org/` (MCP streamable HTTP, stateless JSON, at the root path).
Anonymous and read-only. It calls the public WHG API and never sends a token.

```
client --TLS--> host nginx (sites-available/mcp.whgazetteer.org: per-IP limits, access log)
       --> 127.0.0.1:8020 --> container whg-mcp (compose project ~/stacks/mcp, network `mcp`)
       --> https://whgazetteer.org via extra_hosts host-gateway --> host nginx main vhost --> Django
```

The upstream calls appear in `/var/log/nginx/access.log` with the container's address (172.23.0.x) and
`User-Agent: WHG-MCP/<version>`. They are subject to the main vhost's `app_rl` limit (10 r/s, burst 100,
for the container as one client) and to the API's own rules.

## Files

| Repo (this directory) | Droplet |
|---|---|
| `docker-compose.yml` | `/home/whgadmin/stacks/mcp/docker-compose.yml` |
| `nginx/mcp.whgazetteer.org` | `/etc/nginx/sites-available/mcp.whgazetteer.org`, symlinked from `sites-enabled/` |
| (none) | `/etc/nginx/ssl/mcp.whgazetteer.org-selfsigned.{crt,key}`: a TEMPORARY certificate until Let's Encrypt can issue |

`server-admin/deploy.sh` writes only `~/nginx-sites/<site>`, and the deployment repo's `make nginx-sync`
copies its own list without deleting anything. Neither touches this vhost. Its `limit_req_zone`,
`limit_conn_zone` and `log_format` are defined in this file rather than in `conf.d/00-whg-edge.conf`,
because `nginx-sync` overwrites that file. The vhost depends on `$whg_rl_key` from `00-whg-edge.conf`.

## Limits and budget

- nginx, per client IP (IPv6 per /64): `limit_req` 30 r/min with burst 20 (nodelay), `limit_conn` 10, and
  429 when exceeded. Request body is capped at 64 KB.
- The server's in-process daily budget is `WHG_MCP_DAILY_BUDGET=2000` upstream calls per UTC day, shared by
  all clients. When it is spent, tools return an error until midnight UTC. The counter resets whenever the
  container restarts.
- In the server code: bbox at most 10 x 10 degrees, radius at most 100 km, at most 50 results, responses
  capped at 1.5 MB, GET only, allow-listed paths.
- Container: uid 1000 (non-root), `read_only`, `cap_drop: ALL`, `no-new-privileges`, 256 MB memory,
  64 pids, 1 CPU, published only on 127.0.0.1:8020.

## Deploy or update

From a laptop holding the droplet key (`K="-o IdentitiesOnly=yes -i ~/.ssh/whg_nyc1_ed25519"`). The image
does not go to Docker Hub.

```bash
cd mcp && docker build -t worldhistoricalgazetteer/whg-mcp:<ver> .
docker save worldhistoricalgazetteer/whg-mcp:<ver> | gzip | ssh $K whgadmin@157.230.49.27 'gunzip | docker load'
# bump `image:` in deploy/docker-compose.yml, commit, then copy it to the droplet:
scp $K deploy/docker-compose.yml whgadmin@157.230.49.27:stacks/mcp/docker-compose.yml
ssh $K whgadmin@157.230.49.27 'cd ~/stacks/mcp && docker compose up -d && docker ps --filter name=whg-mcp'
```

To change the nginx block, keep a backup and reload only after a passing test:

```bash
ssh $K root@157.230.49.27 'cp -p /etc/nginx/sites-available/mcp.whgazetteer.org /root/mcp.vhost.bak-$(date +%F) && \
  install -m644 /root/mcp.whgazetteer.org.new /etc/nginx/sites-available/mcp.whgazetteer.org && nginx -t && systemctl reload nginx'
```

After the reload, check `journalctl -u nginx`. If a `limit_req_zone` key changes, the reload is silently
refused, so rename the zone when its key changes.

## Rollback

- Previous image: set the old tag in `~/stacks/mcp/docker-compose.yml`, then `docker compose up -d`.
- Remove the service entirely:
  ```bash
  ssh $K root@157.230.49.27 'rm /etc/nginx/sites-enabled/mcp.whgazetteer.org && nginx -t && systemctl reload nginx'
  ssh $K whgadmin@157.230.49.27 'cd ~/stacks/mcp && docker compose down'
  ```
- Full nginx restore (pre-MCP state, taken 2026-11-17):
  `tar -xzf /root/nginx.bak-mcp-20261117.tgz -C /etc && nginx -t && systemctl reload nginx`.
- DNS: delete the `mcp` A record.

## DNS and certificate (one-off, needs DO DNS access)

1. DigitalOcean DNS, zone `whgazetteer.org`: add `A mcp -> 168.144.13.214` (the reserved IP, the same
   target as every other name) with TTL 3600. Add no AAAA, because the droplet has no public IPv6.
2. When `dig +short mcp.whgazetteer.org @ns1.digitalocean.com` returns that address, run as root on the
   droplet:
   ```bash
   certbot certonly --nginx -d mcp.whgazetteer.org --account bc752e357f22755a54d4a2291bb3bde9 --non-interactive --agree-tos
   sed -i -e 's#^\(\s*\)ssl_certificate     /etc/nginx/ssl/mcp.whgazetteer.org-selfsigned.crt;#\1ssl_certificate     /etc/letsencrypt/live/mcp.whgazetteer.org/fullchain.pem;#' \
          -e 's#^\(\s*\)ssl_certificate_key /etc/nginx/ssl/mcp.whgazetteer.org-selfsigned.key;#\1ssl_certificate_key /etc/letsencrypt/live/mcp.whgazetteer.org/privkey.pem;#' \
          /etc/nginx/sites-available/mcp.whgazetteer.org
   nginx -t && systemctl reload nginx && rm -f /etc/nginx/ssl/mcp.whgazetteer.org-selfsigned.*
   ```
   Then make the same edit in `nginx/mcp.whgazetteer.org` here. `certbot.timer` is already active and
   renews the new certificate with the others.

## Logs

- Client requests: `/var/log/nginx/mcp.access.log`. The `lr=` field records the limit outcome (`REJECTED`
  means a 429). Errors and limit events go to `/var/log/nginx/mcp.error.log`. Both are rotated by the
  standard nginx logrotate.
- Upstream calls: `grep 'WHG-MCP/' /var/log/nginx/access.log`.
- Server: `docker logs whg-mcp` (json-file, 3 x 10 MB).

## Smoke test

Before DNS exists, use `--resolve mcp.whgazetteer.org:443:168.144.13.214` and `-k`:

```bash
curl -s https://mcp.whgazetteer.org/ -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

A foreign `Host` gets 421 from the container. A foreign `Origin` gets 403.
