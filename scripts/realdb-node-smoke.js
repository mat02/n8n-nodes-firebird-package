'use strict';

const fs = require('fs');
const path = require('path');

const envPath = path.resolve(__dirname, '..', 'realdb.env');

function loadEnvironment(filePath) {
	if (!fs.existsSync(filePath)) {
		throw new Error(`Missing ${path.basename(filePath)}.`);
	}

	const environment = {};
	for (const [index, line] of fs.readFileSync(filePath, 'utf8').split(/\r?\n/).entries()) {
		const trimmed = line.trim();
		if (!trimmed || trimmed.startsWith('#')) {
			continue;
		}

		const separator = trimmed.indexOf('=');
		if (separator === -1) {
			throw new Error(`Invalid ${path.basename(filePath)} entry on line ${index + 1}.`);
		}

		const key = trimmed.slice(0, separator).trim();
		let value = trimmed.slice(separator + 1).trim();
		if (!key) {
			throw new Error(`Missing key in ${path.basename(filePath)} on line ${index + 1}.`);
		}
		if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
			value = value.slice(1, -1);
		}
		environment[key] = value;
	}

	return environment;
}

function required(environment, name) {
	if (!environment[name]) {
		throw new Error(`Missing ${name} in ${path.basename(envPath)}.`);
	}
	return environment[name];
}

function numberValue(environment, name, defaultValue) {
	const value = environment[name] === undefined ? defaultValue : Number(environment[name]);
	if (!Number.isFinite(value)) {
		throw new Error(`${name} must be a number in ${path.basename(envPath)}.`);
	}
	return value;
}

async function main() {
	const environment = loadEnvironment(envPath);
	const nodePath = path.resolve(__dirname, '..', 'dist', 'nodes', 'FirebirdNode', 'Firebird.node.js');
	if (!fs.existsSync(nodePath)) {
		throw new Error('Compiled node not found. Run the smoke test through the ephemeral build container.');
	}
	const credentials = {
		host: required(environment, 'FIREBIRD_HOST'),
		port: numberValue(environment, 'FIREBIRD_PORT', 3050),
		database: required(environment, 'FIREBIRD_DATABASE'),
		user: required(environment, 'FIREBIRD_USER'),
		password: required(environment, 'FIREBIRD_PASSWORD'),
		role: environment.FIREBIRD_ROLE || null,
		retryConnectionInterval: 1000,
		pageSize: 4096,
		lowercase_keys: false,
		wireCrypt: numberValue(environment, 'FIREBIRD_WIRE_CRYPT', 1),
	};
	const timeout = numberValue(environment, 'FIREBIRD_TIMEOUT', 10);
	const { Firebird } = require(nodePath);
	const node = new Firebird();
	const parameters = {
		operation: 'executeQuery',
		query: 'SELECT 1 AS SMOKE_RESULT FROM RDB$DATABASE',
		params: '',
		timeout,
	};

	const context = {
		getCredentials: async () => credentials,
		getNode: () => ({ name: 'Firebird smoke test' }),
		getNodeParameter: (name) => parameters[name],
		getInputData: () => [{ json: {} }],
		helpers: {
			returnJsonArray: (data) => (Array.isArray(data) ? data : [data]).map((json) => ({ json })),
		},
		continueOnFail: () => false,
		prepareOutputData: (items) => [items],
	};

	const result = await node.execute.call(context);
	const row = result[0] && result[0][0] && result[0][0].json;
	if (!row || row.SMOKE_RESULT !== 1) {
		throw new Error('The node did not return SMOKE_RESULT = 1.');
	}

	console.log(`Smoke query succeeded with WireCrypt=${credentials.wireCrypt} against ${credentials.host}:${credentials.port}.`);
}

main().catch((error) => {
	console.error(`Smoke query failed: ${error.message}`);
	process.exitCode = 1;
});
