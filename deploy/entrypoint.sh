#!/bin/sh
# entrypoint.sh — runs DB migrations then hands off to supervisord
set -e

echo "[entrypoint] Running database migrations..."
for f in /app/migrations/*.sql; do
    echo "[entrypoint] Applying $f ..."
    psql "$DATABASE_URL" -f "$f" || true   # 'IF NOT EXISTS' guards make idempotent
done
echo "[entrypoint] Migrations done."

echo "[entrypoint] Starting services via supervisord..."
exec /usr/bin/supervisord -c /etc/supervisor/supervisord.conf
