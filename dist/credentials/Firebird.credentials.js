"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Firebird = void 0;
class Firebird {
    constructor() {
        this.name = 'firebird';
        this.displayName = 'Firebird';
        this.documentationUrl = '';
        this.properties = [
            {
                displayName: 'Host',
                name: 'host',
                type: 'string',
                default: '127.0.0.1',
            },
            {
                displayName: 'Database',
                name: 'database',
                type: 'string',
                default: 'database.fdb',
            },
            {
                displayName: 'User',
                name: 'user',
                type: 'string',
                default: 'SYSDBA',
            },
            {
                displayName: 'Password',
                name: 'password',
                type: 'string',
                typeOptions: {
                    password: true,
                },
                default: 'masterkey',
            },
            {
                displayName: 'Port',
                name: 'port',
                type: 'number',
                default: 3050,
            },
            {
                displayName: 'Reconnect Interval',
                name: 'retryConnectionInterval',
                type: 'number',
                default: 1000,
                description: 'Reconnect interval in case of connection drop.',
            },
            {
                displayName: 'Role',
                name: 'role',
                type: 'string',
                default: null,
                description: 'Connection role.',
            },
            {
                displayName: 'Page Size',
                name: 'pageSize',
                type: 'number',
                default: 4096,
                description: 'Page size when creating new database.',
            },
            {
                displayName: 'Lowercase Keys',
                name: 'lowercase_keys',
                type: 'boolean',
                default: false,
                description: 'Set to true to lowercase keys.',
            },
            {
                displayName: 'WireCrypt',
                name: 'wireCrypt',
                type: 'options',
                options: [
                    {
                        name: 'Disabled',
                        value: 0,
                    },
                    {
                        name: 'Enabled',
                        value: 1,
                    },
                ],
                default: 0,
                description: 'Enable encrypted network transport when supported by the Firebird server.',
            },
            {
                displayName: 'Authentication Plugin',
                name: 'pluginName',
                type: 'options',
                options: [
                    {
                        name: 'Automatic',
                        value: '',
                    },
                    {
                        name: 'Srp',
                        value: 'Srp',
                    },
                    {
                        name: 'Srp256',
                        value: 'Srp256',
                    },
                    {
                        name: 'Legacy Auth',
                        value: 'Legacy_Auth',
                    },
                ],
                default: 'Srp',
                description: 'Select the Firebird authentication plugin. Srp is the recommended default. Use Legacy Auth only when required by the server.',
            },
        ];
    }
}
exports.Firebird = Firebird;
//# sourceMappingURL=Firebird.credentials.js.map