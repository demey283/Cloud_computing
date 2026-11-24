#!/bin/bash
# Installeert of werkt het dashboard "Weerstation" bij vanuit de template.
# - Automatisch: eenmalig na de eerste setup van InfluxDB (lege volume).
# - Manueel bijwerken na een wijziging aan de template:
#     docker exec influxdb bash /docker-entrypoint-initdb.d/01-dashboard.sh
# Het dashboard hoort bij de stack "weerstation", zodat opnieuw toepassen het bestaande
# dashboard bijwerkt in plaats van een kopie te maken.
set -e
ORG="${DOCKER_INFLUXDB_INIT_ORG}"
TEMPLATE=/templates/weerstation-dashboard.json

stack_id() {
  grep -o '"id": "[0-9a-f]\{16\}"' | head -1 | grep -o '[0-9a-f]\{16\}'
}

STACK_ID=$(influx stacks --org "$ORG" --stack-name weerstation --json | stack_id || true)
if [ -z "$STACK_ID" ]; then
  STACK_ID=$(influx stacks init --org "$ORG" --stack-name weerstation --json | stack_id)
fi

influx apply --org "$ORG" --stack-id "$STACK_ID" --file "$TEMPLATE" --force yes
echo "Dashboard Weerstation geïnstalleerd/bijgewerkt (stack $STACK_ID)."
