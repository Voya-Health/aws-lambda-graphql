"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DynamoDBSubscriptionManager = void 0;
const assert_1 = __importDefault(require("assert"));
const client_dynamodb_1 = require("@aws-sdk/client-dynamodb");
const util_dynamodb_1 = require("@aws-sdk/util-dynamodb");
const helpers_1 = require("./helpers");
const DEFAULT_TTL = 7200;
// polyfill Symbol.asyncIterator
if (Symbol.asyncIterator === undefined) {
    Symbol.asyncIterator = Symbol.for('asyncIterator');
}
/**
 * DynamoDBSubscriptionManager
 *
 * Stores all subsrciptions in Subscriptions and SubscriptionOperations tables (both can be overridden)
 *
 * DynamoDB table structures
 *
 * Subscriptions:
 *  event: primary key (HASH)
 *  subscriptionId: range key (RANGE) - connectionId:operationId (this is always unique per client)
 *
 * SubscriptionOperations:
 *  subscriptionId: primary key (HASH) - connectionId:operationId (this is always unique per client)
 */
class DynamoDBSubscriptionManager {
    constructor({ dynamoDbClient, subscriptionsTableName = 'Subscriptions', subscriptionOperationsTableName = 'SubscriptionOperations', ttl = DEFAULT_TTL, getSubscriptionNameFromEvent = (event) => event.event, getSubscriptionNameFromConnection = (name) => name, } = {}) {
        this.subscribersByEvent = (event) => {
            let ExclusiveStartKey;
            let done = false;
            const name = this.getSubscriptionNameFromEvent(event);
            return {
                next: async () => {
                    var _a, _b;
                    if (done) {
                        return { value: [], done: true };
                    }
                    const time = Math.round(Date.now() / 1000);
                    const result = await this.db.send(new client_dynamodb_1.QueryCommand({
                        ExclusiveStartKey,
                        TableName: this.subscriptionsTableName,
                        Limit: 50,
                        KeyConditionExpression: 'event = :event',
                        FilterExpression: '#ttl > :time OR attribute_not_exists(#ttl)',
                        ExpressionAttributeValues: util_dynamodb_1.marshall({
                            ':event': name,
                            ':time': time,
                        }),
                        ExpressionAttributeNames: {
                            '#ttl': 'ttl',
                        },
                    }));
                    ExclusiveStartKey = result.LastEvaluatedKey;
                    if (ExclusiveStartKey == null) {
                        done = true;
                    }
                    // we store connectionData on subscription too so we don't
                    // need to load data from connections table
                    const value = (_b = (_a = result.Items) === null || _a === void 0 ? void 0 : _a.map((item) => util_dynamodb_1.unmarshall(item))) !== null && _b !== void 0 ? _b : [];
                    return { value, done: done && value.length === 0 };
                },
                [Symbol.asyncIterator]() {
                    return this;
                },
            };
        };
        this.subscribe = async (names, connection, operation) => {
            const subscriptionId = this.generateSubscriptionId(connection.id, operation.operationId);
            // we can only subscribe to one subscription in GQL document
            if (names.length !== 1) {
                throw new Error('Only one active operation per event name is allowed');
            }
            let [name] = names;
            name = this.getSubscriptionNameFromConnection(name, connection);
            const ttlField = this.ttl === false || this.ttl == null
                ? {}
                : { ttl: helpers_1.computeTTL(this.ttl) };
            await this.db.send(new client_dynamodb_1.BatchWriteItemCommand({
                RequestItems: {
                    [this.subscriptionsTableName]: [
                        {
                            PutRequest: {
                                Item: util_dynamodb_1.marshall(Object.assign({ connection,
                                    operation, event: name, subscriptionId, operationId: operation.operationId }, ttlField)),
                            },
                        },
                    ],
                    [this.subscriptionOperationsTableName]: [
                        {
                            PutRequest: {
                                Item: util_dynamodb_1.marshall(Object.assign({ subscriptionId, event: name }, ttlField)),
                            },
                        },
                    ],
                },
            }));
        };
        this.unsubscribe = async (subscriber) => {
            const subscriptionId = this.generateSubscriptionId(subscriber.connection.id, subscriber.operationId);
            await this.db.send(new client_dynamodb_1.TransactWriteItemsCommand({
                TransactItems: [
                    {
                        Delete: {
                            TableName: this.subscriptionsTableName,
                            Key: util_dynamodb_1.marshall({
                                event: subscriber.event,
                                subscriptionId,
                            }),
                        },
                    },
                    {
                        Delete: {
                            TableName: this.subscriptionOperationsTableName,
                            Key: util_dynamodb_1.marshall({
                                subscriptionId,
                            }),
                        },
                    },
                ],
            }));
        };
        this.unsubscribeOperation = async (connectionId, operationId) => {
            const operation = await this.db.send(new client_dynamodb_1.GetItemCommand({
                TableName: this.subscriptionOperationsTableName,
                Key: util_dynamodb_1.marshall({
                    subscriptionId: this.generateSubscriptionId(connectionId, operationId),
                }),
            }));
            if (operation.Item) {
                await this.db.send(new client_dynamodb_1.TransactWriteItemsCommand({
                    TransactItems: [
                        {
                            Delete: {
                                TableName: this.subscriptionsTableName,
                                Key: util_dynamodb_1.marshall({
                                    event: operation.Item.event,
                                    subscriptionId: operation.Item.subscriptionId,
                                }),
                            },
                        },
                        {
                            Delete: {
                                TableName: this.subscriptionOperationsTableName,
                                Key: util_dynamodb_1.marshall({
                                    subscriptionId: operation.Item.subscriptionId,
                                }),
                            },
                        },
                    ],
                }));
            }
        };
        this.unsubscribeAllByConnectionId = async (connectionId) => {
            let cursor;
            do {
                const { Items, LastEvaluatedKey } = await this.db.send(new client_dynamodb_1.ScanCommand({
                    TableName: this.subscriptionsTableName,
                    ExclusiveStartKey: cursor,
                    FilterExpression: 'begins_with(subscriptionId, :connection_id)',
                    ExpressionAttributeValues: util_dynamodb_1.marshall({
                        ':connection_id': connectionId,
                    }),
                    Limit: 12,
                }));
                if (Items == null || (LastEvaluatedKey == null && Items.length === 0)) {
                    return;
                }
                if (Items.length > 0) {
                    await this.db.send(new client_dynamodb_1.BatchWriteItemCommand({
                        RequestItems: {
                            [this.subscriptionsTableName]: Items.map((item) => ({
                                DeleteRequest: {
                                    Key: util_dynamodb_1.marshall({
                                        event: item.event,
                                        subscriptionId: item.subscriptionId,
                                    }),
                                },
                            })),
                            [this.subscriptionOperationsTableName]: Items.map((item) => ({
                                DeleteRequest: {
                                    Key: util_dynamodb_1.marshall({ subscriptionId: item.subscriptionId }),
                                },
                            })),
                        },
                    }));
                }
                cursor = LastEvaluatedKey;
            } while (cursor);
        };
        this.generateSubscriptionId = (connectionId, operationId) => {
            return `${connectionId}:${operationId}`;
        };
        assert_1.default.ok(typeof subscriptionOperationsTableName === 'string', 'Please provide subscriptionOperationsTableName as a string');
        assert_1.default.ok(typeof subscriptionsTableName === 'string', 'Please provide subscriptionsTableName as a string');
        assert_1.default.ok(ttl === false || (typeof ttl === 'number' && ttl > 0), 'Please provide ttl as a number greater than 0 or false to turn it off');
        assert_1.default.ok(dynamoDbClient == null || typeof dynamoDbClient === 'object', 'Please provide dynamoDbClient as an instance of DynamoDBClient');
        this.subscriptionsTableName = subscriptionsTableName;
        this.subscriptionOperationsTableName = subscriptionOperationsTableName;
        this.db = dynamoDbClient || new client_dynamodb_1.DynamoDBClient({});
        this.ttl = ttl;
        this.getSubscriptionNameFromEvent = getSubscriptionNameFromEvent;
        this.getSubscriptionNameFromConnection = getSubscriptionNameFromConnection;
    }
}
exports.DynamoDBSubscriptionManager = DynamoDBSubscriptionManager;
//# sourceMappingURL=DynamoDBSubscriptionManager.js.map