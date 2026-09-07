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

function setting(environment, name, defaultValue) {
	return process.env[name] ? process.env[name] : (environment[name] === undefined ? defaultValue : environment[name]);
}

function diagnostic(message) {
	if (process.env.FIREBIRD_SMOKE_DIAGNOSTICS === '1') {
		console.log(`[smoke-debug] +${Date.now() - global.smokeStartedAt}ms ${message}`);
	}
}

function errorDetails(error) {
	const gdscode = error && error.gdscode;
	return gdscode === undefined ? error.message : `${error.message} (gdscode=${gdscode})`;
}

async function main() {
	global.smokeStartedAt = Date.now();
	diagnostic('loading local configuration');
	const environment = loadEnvironment(envPath);
	const smokeTimeout = numberValue({ FIREBIRD_SMOKE_TIMEOUT: setting(environment, 'FIREBIRD_SMOKE_TIMEOUT', 30) }, 'FIREBIRD_SMOKE_TIMEOUT', 30);
	const iterations = numberValue({ FIREBIRD_SMOKE_ITERATIONS: setting(environment, 'FIREBIRD_SMOKE_ITERATIONS', 1) }, 'FIREBIRD_SMOKE_ITERATIONS', 1);
	if (!Number.isInteger(iterations) || iterations < 1) {
		throw new Error('FIREBIRD_SMOKE_ITERATIONS must be a positive integer.');
	}
	const timeoutId = setTimeout(() => {
		console.error(`Smoke query timed out after ${smokeTimeout} seconds.`);
		process.exit(1);
	}, smokeTimeout * 1000);
	const nodePath = path.resolve(__dirname, '..', 'dist', 'nodes', 'FirebirdNode', 'Firebird.node.js');
	if (!fs.existsSync(nodePath)) {
		throw new Error('Compiled node not found. Run the smoke test through the ephemeral build container.');
	}
	const wireCrypt = numberValue({ FIREBIRD_WIRE_CRYPT: setting(environment, 'FIREBIRD_WIRE_CRYPT', 1) }, 'FIREBIRD_WIRE_CRYPT', 1);
	const pluginName = setting(environment, 'FIREBIRD_PLUGIN_NAME', '');
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
		wireCrypt,
	};
	if (pluginName) {
		credentials.pluginName = pluginName;
	}
	const timeout = numberValue(environment, 'FIREBIRD_TIMEOUT', 10);
	const driverVersion = require('node-firebird/package.json').version;
	diagnostic(`loaded node-firebird=${driverVersion}, wireCrypt=${wireCrypt}, plugin=${pluginName || 'automatic'}`);
	const { Firebird } = require(nodePath);
	const node = new Firebird();
	const parameters = {
		operation: 'executeQuery',
		query: "SELECT 1 AS SMOKE_RESULT, RDB$GET_CONTEXT('SYSTEM', 'WIRE_ENCRYPTED') AS WIRE_ENCRYPTED FROM RDB$DATABASE",
		params: '',
		timeout,
	};

	const context = {
		getCredentials: async () => {
			diagnostic('n8n node requested credentials');
			return credentials;
		},
		getNode: () => ({ name: 'Firebird smoke test' }),
		getNodeParameter: (name) => parameters[name],
		getInputData: () => [{ json: {} }],
		helpers: {
			returnJsonArray: (data) => (Array.isArray(data) ? data : [data]).map((json) => ({ json })),
		},
		continueOnFail: () => false,
		prepareOutputData: (items) => [items],
	};

	for (let iteration = 1; iteration <= iterations; iteration++) {
		try {
			diagnostic(`invoking Firebird.execute iteration=${iteration}/${iterations}`);
			const result = await node.execute.call(context);
			const row = result[0] && result[0][0] && result[0][0].json;
			if (!row || row.SMOKE_RESULT !== 1) {
				throw new Error('The node did not return SMOKE_RESULT = 1.');
			}
			if (credentials.wireCrypt === 1 && row.WIRE_ENCRYPTED !== 'TRUE') {
				throw new Error(`WireCrypt was enabled, but the server reported WIRE_ENCRYPTED = ${row.WIRE_ENCRYPTED || 'NULL'}.`);
			}
		} catch (error) {
			throw new Error(`Smoke iteration ${iteration}/${iterations} failed: ${errorDetails(error)}`);
		}

		if (iterations > 1 && (iteration % 100 === 0 || iteration === iterations)) {
			console.log(`Smoke progress: ${iteration}/${iterations} successful attachments.`);
		}
	}

	clearTimeout(timeoutId);
	console.log(`Smoke query succeeded with node-firebird=${driverVersion}, iterations=${iterations}, WireCrypt=${credentials.wireCrypt} against ${credentials.host}:${credentials.port}.`);
}

main().catch((error) => {
	console.error(`Smoke query failed: ${error.message}`);
	process.exit(1);
});
