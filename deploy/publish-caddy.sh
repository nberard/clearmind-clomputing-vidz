#!/usr/bin/env bash
# Publish the grid on https://SUBDOMAIN.DOMAIN through the clear-soken Caddy
# (same approach as transmission-stack/deploy-portal.sh): writes a site file in
# clear-soken/proxy/sites and reloads Caddy. The wildcard DNS *.DOMAIN covers
# the subdomain and Caddy gets the certificate on its own.
#
#   ./deploy/publish-caddy.sh -H touplitoui.me            # https://grid.touplitoui.me
#   ./deploy/publish-caddy.sh -H touplitoui.me -s vidz    # https://vidz.touplitoui.me
#   ./deploy/publish-caddy.sh -H touplitoui.me -r         # remove the site
set -euo pipefail

DOMAIN="" SUBDOMAIN="grid" REMOVE=0
CLEARSOKEN_DIR="${HOME}/docker/clear-soken"

usage() {
    echo "Usage: $0 -H DOMAIN [-s SUBDOMAIN] [-K CLEARSOKEN_DIR] [-r]"
    echo "  -H DOMAIN          publish on https://SUBDOMAIN.DOMAIN (required)"
    echo "  -s SUBDOMAIN       default: grid"
    echo "  -K CLEARSOKEN_DIR  clone of clear-soken (default: ~/docker/clear-soken)"
    echo "  -r                 remove the Caddy site"
    exit 1
}
while getopts "H:s:K:rh" o; do
    case $o in
        H) DOMAIN=$OPTARG ;; s) SUBDOMAIN=$OPTARG ;; K) CLEARSOKEN_DIR=$OPTARG ;; r) REMOVE=1 ;; *) usage ;;
    esac
done
[ -n "$DOMAIN" ] || usage

SITES_DIR="$CLEARSOKEN_DIR/proxy/sites"
SITE_FILE="$SITES_DIR/vibegen-grid.caddy"
HOST="$SUBDOMAIN.$DOMAIN"

reload_caddy() {
    docker exec caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile > /dev/null 2>&1
}

[ -d "$SITES_DIR" ] || { echo "Caddy sites folder not found: $SITES_DIR (use -K)"; exit 2; }

if [ "$REMOVE" = 1 ]; then
    rm -f "$SITE_FILE" && reload_caddy && echo " -> Caddy site removed"
    exit 0
fi

docker network inspect proxy > /dev/null 2>&1 || { echo "Docker network 'proxy' not found (created by clear-soken)"; exit 3; }
if ! docker network inspect proxy --format '{{range .Containers}}{{.Name}} {{end}}' | grep -q grid; then
    echo "The grid is not on the 'proxy' network yet. In .env set:"
    echo "  COMPOSE_FILE=docker-compose.yml:docker-compose.proxy.yml"
    echo "then: docker compose up -d"
    exit 4
fi

cat > "$SITE_FILE" << CADDY
# vibegen grid (created by clearmind-clomputing-vidz/deploy/publish-caddy.sh)
# Dashboard is read-only without the token; every write needs the grid token.
$HOST {
	encode zstd gzip
	header {
		Strict-Transport-Security "max-age=31536000"
		X-Content-Type-Options nosniff
		X-Frame-Options DENY
		X-Robots-Tag "noindex, nofollow"
		Referrer-Policy no-referrer
		-Server
	}
	request_body {
		# collector uploads batches of thumbnails
		max_size 20MB
	}
	reverse_proxy vibegen-grid:8000
}
CADDY

if reload_caddy; then
    echo " --> https://$HOST published through Caddy"
else
    rm -f "$SITE_FILE"
    echo "Caddy reload failed, site removed. Check: docker logs caddy --tail 20"
    exit 5
fi
