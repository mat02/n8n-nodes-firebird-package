#!/usr/bin/env sh

set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

if [ ! -f "$repository/realdb.env" ]; then
	printf '%s\n' 'Missing realdb.env.' >&2
	exit 1
fi

if [ ! -f "$repository/dist/nodes/FirebirdNode/Firebird.node.js" ]; then
	printf '%s\n' 'Compiled node not found. Run this from the dist branch.' >&2
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
	node:14-bullseye \
	sh -c 'cp -a /workspace /tmp/firebird-smoke && cd /tmp/firebird-smoke && npm install --production --no-save --no-package-lock && node scripts/realdb-node-smoke.js'
