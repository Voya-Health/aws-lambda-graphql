"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DynamoDBConnectionManager = void 0;
const assert_1 = __importDefault(require("assert"));
const client_dynamodb_1 = require("@aws-sdk/client-dynamodb");
const client_apigatewaymanagementapi_1 = require("@aws-sdk/client-apigatewaymanagementapi");
const util_dynamodb_1 = require("@aws-sdk/util-dynamodb");
const errors_1 = require("./errors");
const helpers_1 = require("./helpers");
const isTTLExpired_1 = require("./helpers/isTTLExpired");
const DEFAULT_TTL = 7200;
/**
 * DynamoDBConnectionManager
 *
 * Stores connections in DynamoDB table (default table name is Connections, you can override that)
 */
class DynamoDBConnectionManager {
    constructor({ apiGatewayManager, region, connectionsTable = 'Connections', dynamoDbClient, subscriptions, ttl = DEFAULT_TTL, debug = false, }) {
        this.hydrateConnection = async (connectionId, options) => {
            const { retryCount = 0, timeout = 50 } = options || {};
            // if connection is not found, throw so we can terminate connection
            let connection;
            for (let i = 0; i <= retryCount; i++) {
                const result = await this.db.send(new client_dynamodb_1.GetItemCommand({
                    TableName: this.connectionsTable,
                    Key: util_dynamodb_1.marshall({
                        id: connectionId,
                    }),
                }));
                if (result.Item) {
                    // Jump out of loop
                    connection = util_dynamodb_1.unmarshall(result.Item);
                    break;
                }
                // wait for another round
                await new Promise((r) => setTimeout(r, timeout));
            }
            if (!connection || isTTLExpired_1.isTTLExpired(connection.ttl)) {
                throw new errors_1.ConnectionNotFoundError(`Connection ${connectionId} not found`);
            }
            return connection;
        };
        this.setConnectionData = async (data, { id }) => {
            await this.db.send(new client_dynamodb_1.UpdateItemCommand({
                TableName: this.connectionsTable,
                Key: util_dynamodb_1.marshall({
                    id,
                }),
                UpdateExpression: 'set #data = :data',
                ExpressionAttributeValues: util_dynamodb_1.marshall({
                    ':data': data,
                }),
                ExpressionAttributeNames: {
                    '#data': 'data',
                },
            }));
        };
        this.registerConnection = async ({ connectionId, endpoint, }) => {
            const connection = {
                id: connectionId,
                data: { endpoint, context: {}, isInitialized: false },
            };
            if (this.debug)
                console.log(`Connected ${connection.id}`, connection.data);
            await this.db.send(new client_dynamodb_1.PutItemCommand({
                TableName: this.connectionsTable,
                Item: util_dynamodb_1.marshall(Object.assign({ createdAt: new Date().toString(), id: connection.id, data: connection.data }, (this.ttl === false || this.ttl == null
                    ? {}
                    : {
                        ttl: helpers_1.computeTTL(this.ttl),
                    }))),
            }));
            return connection;
        };
        this.sendToConnection = async (connection, payload) => {
            try {
                // TODO filter on region of connection (dont' try to send to wrong region in the 1st place)
                console.debug('=== sendToConnection', connection.data.endpoint);
                await this.createApiGatewayManager(connection.data.endpoint).send(new client_apigatewaymanagementapi_1.PostToConnectionCommand({
                    ConnectionId: connection.id,
                    Data: payload,
                }));
            }
            catch (e) {
                // this is stale connection
                // remove it from DB
                if (e && e.statusCode === 410) {
                    await this.unregisterConnection(connection);
                }
                else {
                    console.warn('=== error in sendToConnection', e);
                }
            }
        };
        this.unregisterConnection = async ({ id }) => {
            await Promise.all([
                this.db.send(new client_dynamodb_1.DeleteItemCommand({
                    Key: util_dynamodb_1.marshall({
                        id,
                    }),
                    TableName: this.connectionsTable,
                })),
                this.subscriptions.unsubscribeAllByConnectionId(id),
            ]);
        };
        this.closeConnection = async ({ id, data }) => {
            if (this.debug)
                console.log('Disconnected ', id);
            await this.createApiGatewayManager(data.endpoint).send(new client_apigatewaymanagementapi_1.DeleteConnectionCommand({
                ConnectionId: id,
            }));
        };
        assert_1.default.ok(typeof region === 'string', 'Please provide region as a string');
        assert_1.default.ok(typeof connectionsTable === 'string', 'Please provide connectionsTable as a string');
        assert_1.default.ok(typeof subscriptions === 'object', 'Please provide subscriptions to manage subscriptions.');
        assert_1.default.ok(ttl === false || (typeof ttl === 'number' && ttl > 0), 'Please provide ttl as a number greater than 0 or false to turn it off');
        assert_1.default.ok(dynamoDbClient == null || typeof dynamoDbClient === 'object', 'Please provide dynamoDbClient as an instance of DynamoDB.DocumentClient');
        assert_1.default.ok(apiGatewayManager == null || typeof apiGatewayManager === 'object', 'Please provide apiGatewayManager as an instance of ApiGatewayManagementApi');
        assert_1.default.ok(typeof debug === 'boolean', 'Please provide debug as a boolean');
        this.apiGatewayManager = apiGatewayManager;
        this.region = region;
        this.connectionsTable = connectionsTable;
        this.db = dynamoDbClient || new client_dynamodb_1.DynamoDBClient({});
        this.subscriptions = subscriptions;
        this.ttl = ttl;
        this.debug = debug;
    }
    /**
     * Creates api gateway manager
     *
     * If custom api gateway manager is provided, uses it instead
     */
    createApiGatewayManager(endpoint) {
        if (this.apiGatewayManager) {
            return this.apiGatewayManager;
        }
        this.apiGatewayManager = new client_apigatewaymanagementapi_1.ApiGatewayManagementApiClient({ endpoint });
        return this.apiGatewayManager;
    }
}
exports.DynamoDBConnectionManager = DynamoDBConnectionManager;
//# sourceMappingURL=DynamoDBConnectionManager.js.map