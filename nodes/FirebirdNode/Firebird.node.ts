import { IExecuteFunctions } from 'n8n-core';
import {
	IDataObject,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
} from 'n8n-workflow';
// @ts-ignore
import { promisifyAll } from 'bluebird';
import * as fbd from 'node-firebird';

const fbdAsync: any = promisifyAll(fbd);

import { copyInputItems } from './GenericFunctions';

export class Firebird implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Firebird',
		name: 'firebird',
		icon: 'file:Firebird.svg',
		group: ['input'],
		version: 1,
		description: 'Get, add and update data in Firebird database',
		defaults: {
			name: 'Firebird',
			color: '#006d8c',
		},
		inputs: ['main'],
		outputs: ['main'],
		credentials: [
			{
				name: 'firebird',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				options: [
					{
						name: 'Execute Query',
						value: 'executeQuery',
						description: 'Execute an SQL query.',
					},
					{
						name: 'Insert',
						value: 'insert',
						description: 'Insert rows in database.',
					},
					{
						name: 'Update',
						value: 'update',
						description: 'Update rows in database.',
					},
				],
				default: 'insert',
				description: 'The operation to perform.',
			},

			// ----------------------------------
			//         executeQuery
			// ----------------------------------
			{
				displayName: 'Query',
				name: 'query',
				type: 'string',
				typeOptions: {
					alwaysOpenEditWindow: true,
				},
				displayOptions: {
					show: {
						operation: [
							'executeQuery',
						],
					},
				},
				default: '',
				placeholder: 'SELECT id, name FROM product WHERE id == :param1 and value > :param2',
				required: true,
				description: 'The SQL query to execute.',
			},
			{
				displayName: 'Parameters',
				name: 'params',
				type: 'string',
				displayOptions: {
					show: {
						operation: [
							'executeQuery',
						],
					},
				},
				default: '',
				placeholder: 'param1, param2',
				description: 'Comma separated list of named parameters that are used in the query and should be provided by the previous node output. Allowed characters in parameter name: _a-zA-Z0-9.',
			},


			// ----------------------------------
			//         insert
			// ----------------------------------
			{
				displayName: 'Table',
				name: 'table',
				type: 'string',
				displayOptions: {
					show: {
						operation: [
							'insert',
						],
					},
				},
				default: '',
				required: true,
				description: 'Name of the table in which to insert data to.',
			},
			{
				displayName: 'Columns',
				name: 'columns',
				type: 'string',
				displayOptions: {
					show: {
						operation: [
							'insert',
						],
					},
				},
				default: '',
				placeholder: 'id,name,description',
				description: 'Comma separated list of the properties which should used as columns for the new rows.',
			},


			// ----------------------------------
			//         update
			// ----------------------------------
			{
				displayName: 'Table',
				name: 'table',
				type: 'string',
				displayOptions: {
					show: {
						operation: [
							'update',
						],
					},
				},
				default: '',
				required: true,
				description: 'Name of the table in which to update data in',
			},
			{
				displayName: 'Update Key',
				name: 'updateKey',
				type: 'string',
				displayOptions: {
					show: {
						operation: [
							'update',
						],
					},
				},
				default: 'id',
				required: true,
				description: 'Name of the property which decides which rows in the database should be updated. Normally that would be "id".',
			},
			{
				displayName: 'Columns',
				name: 'columns',
				type: 'string',
				displayOptions: {
					show: {
						operation: [
							'update',
						],
					},
				},
				default: '',
				placeholder: 'name,description',
				description: 'Comma separated list of the properties which should used as columns for rows to update.',
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const credentials = await this.getCredentials('firebird');

		if (credentials === undefined) {
			throw new NodeOperationError(this.getNode(), 'No credentials got returned!');
		}

		const items = this.getInputData();
		const operation = this.getNodeParameter('operation', 0) as string;
		let returnItems: any[] = [];
	
		if (operation === 'executeQuery') {
			// ----------------------------------
			//         executeQuery
			// ----------------------------------
			try {
				const queryResult = (await Promise.all(items.map(async (item, index) => {
					let db = await fbdAsync.attachAsync(credentials);
					promisifyAll(db);
					const rawQuery = this.getNodeParameter('query', index) as string;
					const paramsString = this.getNodeParameter('params', 0) as string;
					const params = paramsString.split(',').map(param => param.trim());

					const insertItems = copyInputItems([items[index]], params)[0];
					
					let parametrizedQuery = rawQuery;
					let queryItems: any[] = [];
					let match;

					const re = /'[^']+'|(:)([_a-zA-Z0-9]+)/gm;

					while ((match = re.exec(parametrizedQuery)) !== null)
					{
						if (match[2] === undefined) {
							continue;
						}
						const paramName = match[2];
						if (!params.includes(paramName)) {
							throw new NodeOperationError(this.getNode(), `The parameter "${paramName}" is unknown!`);
						}
						queryItems.push(insertItems[paramName]);
						parametrizedQuery = parametrizedQuery.substring(0, match.index) + '?' + parametrizedQuery.substring((match.index ?? 0) + 1 + match[2].length);
					}

					let result;
					if (queryItems.length > 0) {
						result = await db.queryAsync(parametrizedQuery, queryItems);
					} else {
						result = await db.queryAsync(rawQuery);
					}
					db.detachAsync();
					return result;
				})) as any[]).reduce((collection, result) => {
					if (Array.isArray(result)) {
						return collection.concat(result);
					}
					collection.push(result);
					return collection;
				}, []);

				returnItems = this.helpers.returnJsonArray(queryResult);
			} catch (error) {
				if (this.continueOnFail()) {
					returnItems = this.helpers.returnJsonArray({ error: error.message });
				} else {
					throw error;
				}
			}
		} else if (operation === 'insert') {
			// ----------------------------------
			//         insert
			// ----------------------------------

			try {
				const table = this.getNodeParameter('table', 0) as string;
				const columnString = this.getNodeParameter('columns', 0) as string;
				const columns = columnString.split(',').map(column => column.trim());
				const insertItems = copyInputItems(items, columns);
				const insertPlaceholder = `(${columns.map(column => '?').join(',')})`;

				const insertSQL = `INSERT INTO ${table}(${columnString}) VALUES ${items.map(item => insertPlaceholder).join(',')};`;
				const queryItems = insertItems.reduce((collection, item) => collection.concat(Object.values(item as any)), []); // tslint:disable-line:no-any
				
				let db = await fbdAsync.attachAsync(credentials);
				promisifyAll(db);
				returnItems = await db.queryAsync(insertSQL, queryItems);
				db.detachAsync();
				returnItems = this.helpers.returnJsonArray(returnItems[0] as unknown as IDataObject);
			} catch (error) {
				if (this.continueOnFail()) {
					returnItems = this.helpers.returnJsonArray({ error: error.message });
				} else {
					throw error;
				}
			}

		} else if (operation === 'update') {
			// ----------------------------------
			//         update
			// ----------------------------------

			try {
				const table = this.getNodeParameter('table', 0) as string;
				const updateKey = this.getNodeParameter('updateKey', 0) as string;
				const columnString = this.getNodeParameter('columns', 0) as string;
				const columns = columnString.split(',').map(column => column.trim());

				if (!columns.includes(updateKey)) {
					columns.unshift(updateKey);
				}

				const updateItems = copyInputItems(items, columns);
				const updateSQL = `UPDATE ${table} SET ${columns.map(column => `${column} = ?`).join(',')} WHERE ${updateKey} = ?;`;

				const queryResult = await Promise.all(updateItems.map(async (item) => {
					let db = await fbdAsync.attachAsync(credentials);
					promisifyAll(db);
					let result = await db.queryAsync(updateSQL, Object.values(item).concat(item[updateKey]));
					db.detachAsync();
					return result;
				}));
				returnItems = this.helpers.returnJsonArray(returnItems[0] as unknown as IDataObject);
			} catch (error) {
				if (this.continueOnFail()) {
					returnItems = this.helpers.returnJsonArray({ error: error.message });
				} else {
					throw error;
				}
			}
		} else {
			if (this.continueOnFail()) {
				returnItems = this.helpers.returnJsonArray({ error: `The operation "${operation}" is not supported!` });
			} else {
				throw new NodeOperationError(this.getNode(), `The operation "${operation}" is not supported!`);
			}
		}
		return this.prepareOutputData(returnItems);
	}
}
