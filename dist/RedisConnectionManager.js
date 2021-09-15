"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.RedisConnectionManager = void 0;
const assert_1 = __importDefault(require("assert"));
const aws_sdk_1 = require("aws-sdk");
const errors_1 = require("./errors");
const helpers_1 = require("./helpers");
/**
 * RedisConnectionManager
 *
 * Stores connections in Redis store
 */
class RedisConnectionManager {
    constructor({ apiGatewayManager, redisClient, subscriptions, }) {
        this.hydrateConnection = async (connectionId, options) => {
            const { retryCount = 0, timeout = 50 } = options || {};
            // if connection is not found, throw so we can terminate connection
            let connection;
            for (let i = 0; i <= retryCount; i++) {
                const key = helpers_1.prefixRedisKey(`connection:${connectionId}`);
                const result = await this.redisClient.get(key);
                if (result) {
                    // Jump out of loop
                    connection = JSON.parse(result);
                    break;
                }
                // wait for another round
                await new Promise((r) => setTimeout(r, timeout));
            }
            if (!connection) {
                throw new errors_1.ConnectionNotFoundError(`Connection ${connectionId} not found`);
            }
            return connection;
        };
        this.setConnectionData = async (data, connection) => {
            await this.redisClient.set(helpers_1.prefixRedisKey(`connection:${connection.id}`), JSON.stringify(Object.assign(Object.assign({}, connection), { data })), 'EX', 7200);
        };
        this.registerConnection = async ({ connectionId, endpoint, }) => {
            const connection = {
                id: connectionId,
                data: { endpoint, context: {}, isInitialized: false },
            };
            await this.redisClient.set(helpers_1.prefixRedisKey(`connection:${connectionId}`), JSON.stringify({
                createdAt: new Date().toString(),
                id: connection.id,
                data: connection.data,
            }), 'EX', 7200);
            return connection;
        };
        this.sendToConnection = async (connection, payload) => {
            try {
                await this.createApiGatewayManager(connection.data.endpoint)
                    .postToConnection({ ConnectionId: connection.id, Data: payload })
                    .promise();
            }
            catch (e) {
                // this is stale connection
                // remove it from store
                if (e && e.statusCode === 410) {
                    await this.unregisterConnection(connection);
                }
                else {
                    throw e;
                }
            }
        };
        this.unregisterConnection = async ({ id }) => {
            const key = helpers_1.prefixRedisKey(`connection:${id}`);
            await Promise.all([
                this.redisClient.del(key),
                this.subscriptions.unsubscribeAllByConnectionId(id),
            ]);
        };
        this.closeConnection = async ({ id, data }) => {
            await this.createApiGatewayManager(data.endpoint)
                .deleteConnection({ ConnectionId: id })
                .promise();
        };
        assert_1.default.ok(typeof subscriptions === 'object', 'Please provide subscriptions to manage subscriptions.');
        assert_1.default.ok(redisClient == null || typeof redisClient === 'object', 'Please provide redisClient as an instance of ioredis.Redis');
        assert_1.default.ok(apiGatewayManager == null || typeof apiGatewayManager === 'object', 'Please provide apiGatewayManager as an instance of ApiGatewayManagementApi');
        this.apiGatewayManager = apiGatewayManager;
        this.redisClient = redisClient;
        this.subscriptions = subscriptions;
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
        this.apiGatewayManager = new aws_sdk_1.ApiGatewayManagementApi({ endpoint });
        return this.apiGatewayManager;
    }
}
exports.RedisConnectionManager = RedisConnectionManager;
//# sourceMappingURL=RedisConnectionManager.js.map