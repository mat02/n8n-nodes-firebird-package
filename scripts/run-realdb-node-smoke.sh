#!/usr/bin/env sh

set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

if [ ! -f "$repository/realdb.env" ]; then
	printf '%s\n' 'Missing realdb.env.' >&2
	exit 1
fi

exec docker run --rm \
	-v "$repository:/workspace:ro" \
	-w /tmp \
	-e FIREBIRD_SMOKE_DIAGNOSTICS="${FIREBIRD_SMOKE_DIAGNOSTICS:-}" \
	-e FIREBIRD_DEBUG="${FIREBIRD_SMOKE_DIAGNOSTICS:+1}" \
	-e FIREBIRD_WIRE_CRYPT="${FIREBIRD_WIRE_CRYPT:-}" \
	-e FIREBIRD_PLUGIN_NAME="${FIREBIRD_PLUGIN_NAME:-}" \
	-e FIREBIRD_SMOKE_TIMEOUT="${FIREBIRD_SMOKE_TIMEOUT:-}" \
	-e FIREBIRD_SMOKE_ITERATIONS="${FIREBIRD_SMOKE_ITERATIONS:-}" \
	-e FIREBIRD_SMOKE_NODE_FIREBIRD_VERSION="${FIREBIRD_SMOKE_NODE_FIREBIRD_VERSION:-}" \
	"${FIREBIRD_SMOKE_NODE_IMAGE:-node:14-bullseye}" \
	sh -c 'cp -a /workspace /tmp/firebird-smoke && cd /tmp/firebird-smoke && npm install --no-save --no-package-lock && if [ ! -f dist/nodes/FirebirdNode/Firebird.node.js ]; then npm run build; fi && if [ -n "${FIREBIRD_SMOKE_NODE_FIREBIRD_VERSION:-}" ]; then npm install --production --no-save --no-package-lock "node-firebird@$FIREBIRD_SMOKE_NODE_FIREBIRD_VERSION"; fi && node scripts/realdb-node-smoke.js'
