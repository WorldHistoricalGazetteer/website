## Update mapdata cache

### Basic Usage
- For a single collection or dataset:
```sh
# EXAMPLE - Collection ID 134
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata --collections --id 134

# EXAMPLE - Dataset ID 1592
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata --datasets --id 1592
```
- For all collections or datasets:
```sh
# All collections
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata --collections

# All datasets (continues even if individual datasets fail)
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata --datasets
```
- For all collections and datasets:
```sh
# By default, continues on error - skips problematic datasets like Getty TGN
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata
```
- To only clear cache without regenerating (will regenerate on first access):
```sh
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata --clear-only
```

### Advanced Options
- Stop processing if an error occurs (default is to continue):
```sh
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata --stop-on-error
```
- Resume from a specific ID (useful after interruption):
```sh
# Skip datasets/collections with ID < 100
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata --datasets --skip-to 100
```

### Handling Large Datasets
Very large datasets (>1M places, e.g., Getty TGN) may cause out-of-memory errors during regeneration.
The command will automatically skip these and continue with other datasets.
For these datasets, you can also use `--clear-only` and let the cache regenerate on first access:
```sh
# Clear cache for specific large dataset
docker exec -it web_whgazetteer-org_main python manage.py regenerate_mapdata --datasets --id 16 --clear-only
```

## Update `toponyms` (table of unique place names)
- This database table is used to create sitemap.xml and the schema.org metadata in related search landing pages (e.g. https://whgazetteer.org/search/San%20Jos%C3%A9)
- A search landing page is created for every place name which has more than one attestation AND at least one country code AND at least one timespan.
- It uses the database as its source, because `timespans` have not been rigorously aggregated into the Elasticsearch index.
- It takes a couple of hours to complete so should not be triggered unless an update is considered essential.
```sh
docker exec -it web_whgazetteer-org_main bash -c "./manage.py populate_toponyms"
```

## Find all files containing given text

### Case-sensitive
```bash
find ./ -type f \( -name "*.html" -o -name "*.py" -o -name "*.js" \) ! -path "./whg/static/*" ! -path "./static/*" -exec grep -lz -P "Grossner" {} + | xargs -0 -I {} echo {} | sort -u | sed "s|^./||"
```

### Non-case-sensitive
```bash
find ./ -type f \( -name "*.html" -o -name "*.py" -o -name "*.js" \) ! -path "./whg/static/*" ! -path "./static/*" -exec grep -lzi -P "grossner" {} + | xargs -0 -I {} echo {} | sort -u | sed "s|^./||"
```

## Running Docker in Windows

### Open WSL in Console
- Run PowerShell as Administrator (right-click)
- Switch to WSL as root user
```sh
wsl -u root
```
- Switch to local WHG directory
```sh
cd /mnt/i/workspace/whg3
```

## SSL Certificates
- Certification is automated using the `python3-certbot-dns-digitalocean` plugin, which allows Certbot to automatically manage DNS-01 challenges by updating DNS records on DigitalOcean.
- The domain must first be set up on DigitalOcean.
- To generate a new certificate:
```sh
sudo certbot certonly --dns-digitalocean --dns-digitalocean-credentials /etc/letsencrypt/digitalocean.ini -d <domain.com>
```
- The certificate and key will be stored locally and updated automatically when necessary.
- If it becomes necessary to delete the certificate, the following will ensure that the certificate and its renewal are removed and cancelled:
```sh
sudo certbot delete --cert-name <domain.com>
```

## Blog
- The WHG Blog runs in a separate Docker network
- The filesystem is at `~/sites/blog-whgazetteer-org`
- Credentials are stored in the hidden `.env` file
- Nginx requires a particularly quirky configuration:
```
# Redirect HTTP to HTTPS
server {
    listen 80;
    listen [::]:80;

    server_name blog.whgazetteer.org;

    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl;
    server_name blog.whgazetteer.org;

    index index.php index.html index.htm;

    root /home/whgadmin/sites/blog-whgazetteer-org/wordpress;

    # SSL configuration
    ssl_certificate /etc/letsencrypt/live/blog.whgazetteer.org/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/blog.whgazetteer.org/privkey.pem;
    include /etc/letsencrypt/options-ssl-nginx.conf;
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem;

    location / {
        # try_files $uri $uri/ /index.php$is_args$args;
        try_files $uri $uri/ /index.php?$args;
    }

    location ~ \.php$ {
        include fastcgi_params;
        fastcgi_split_path_info ^(.+\.php)(/.+)$;
        fastcgi_pass 127.0.0.1:9000;
        fastcgi_index index.php;
        fastcgi_param SCRIPT_FILENAME /var/www/html/$fastcgi_script_name; # <<< Note that the container root has to be specified here
        fastcgi_param PATH_INFO $fastcgi_path_info;
        try_files $uri =404;
    }

    location ~ /\.ht {
        deny all;
    }

    location = /favicon.ico {
        log_not_found off; access_log off;
    }

    location = /robots.txt {
        log_not_found off; access_log off; allow all;
    }

    location ~* \.(css|gif|ico|jpeg|jpg|js|png)$ {
        expires max;
        log_not_found off;
    }
}
```
