"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.RedisSubscriptionManager = void 0;
const assert_1 = __importDefault(require("assert"));
const helpers_1 = require("./helpers");
// polyfill Symbol.asyncIterator
if (Symbol.asyncIterator === undefined) {
    Symbol.asyncIterator = Symbol.for('asyncIterator');
}
/**
 * RedisSubscriptionManager
 *
 * Stores all subsrciption information in redis store
 *
 * Record types:
 *
 * subscription:
 *  key: `[app prefix]:subscription:[connectionId]:[operationId]:{[eventName]}` (where eventName is a keyslot)
 *  value: RedisSubscriber (this is always unique per client)
 *
 * subscriptionOperation:
 *  key: `[app prefix]:subscriptionOperation:[connectionId]:[operationId]`
 *  value: eventName
 *
 * connectionSubscriptionsList:
 *  key: `[app prefix]:connectionSubscriptionsList:[connectionId]`
 *  value: redis list of subscription keys corresponding to connectionId
 *
 * eventSubscriptionsList:
 *  key: `[app prefix]:eventSubscriptionsList:${eventName}`
 *  value: redis list of subscription keys corresponding to eventName
 */
class RedisSubscriptionManager {
    constructor({ redisClient, getSubscriptionNameFromEvent = (event) => event.event, getSubscriptionNameFromConnection = (name) => name, }) {
        this.subscribersByEvent = (event) => {
            let offset = 0;
            const name = this.getSubscriptionNameFromEvent(event);
            return {
                next: async () => {
                    const keys = await this.redisClient.lrange(helpers_1.prefixRedisKey(`eventSubscriptionsList:${name}`), offset, offset + 50);
                    offset += 50;
                    if (keys.length === 0) {
                        return { value: [], done: true };
                    }
                    const subscribers = (await this.redisClient.mget(...keys)).map((sub) => (sub ? JSON.parse(sub) : null));
                    return { value: subscribers, done: false };
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
            let [eventName] = names;
            eventName = this.getSubscriptionNameFromConnection(eventName, connection);
            const subscriptionOperationKey = helpers_1.prefixRedisKey(`subscriptionOperation:${subscriptionId}`);
            const subscriptionKey = helpers_1.prefixRedisKey(`subscription:${subscriptionId}:{${eventName}}`);
            await Promise.all([
                this.redisClient.set(subscriptionKey, JSON.stringify({
                    connection,
                    operation,
                    event: eventName,
                    subscriptionId,
                    operationId: operation.operationId,
                })),
                this.redisClient.set(subscriptionOperationKey, eventName),
                this.redisClient.lpush(helpers_1.prefixRedisKey(`eventSubscriptionsList:${eventName}`), subscriptionKey),
                this.redisClient.lpush(helpers_1.prefixRedisKey(`connectionSubscriptionsList:${connection.id}`), subscriptionKey),
            ]);
        };
        this.unsubscribe = async () => {
            /*
              Seems like this method is no longer used (it is invoked only in tests)
              `unsubscribeOperation` is used instead
            */
        };
        this.unsubscribeOperation = async (connectionId, operationId) => {
            const subscriptionId = this.generateSubscriptionId(connectionId, operationId);
            const subscriptionOperationKey = helpers_1.prefixRedisKey(`subscriptionOperation:${subscriptionId}`);
            const eventName = await this.redisClient.get(subscriptionOperationKey);
            const subscriptionKey = helpers_1.prefixRedisKey(`subscription:${subscriptionId}:{${eventName}}`);
            let subscriber;
            const result = await this.redisClient.get(subscriptionKey);
            if (result) {
                subscriber = JSON.parse(result);
                await Promise.all([
                    this.redisClient.del(subscriptionOperationKey),
                    this.redisClient.del(subscriptionKey),
                    this.redisClient.lrem(helpers_1.prefixRedisKey(`eventSubscriptionsList:${subscriber.event}`), 0, subscriptionKey),
                    this.redisClient.lrem(helpers_1.prefixRedisKey(`connectionSubscriptionsList:${subscriber.connection.id}`), 0, subscriptionKey),
                ]);
            }
        };
        this.unsubscribeAllByConnectionId = async (connectionId) => {
            let done = false;
            const limit = 50;
            let offset = 0;
            const subscriptionListKey = helpers_1.prefixRedisKey(`connectionSubscriptionsList:${connectionId}`);
            do {
                const keys = await this.redisClient.lrange(subscriptionListKey, offset, offset + limit);
                offset += limit;
                if (!keys || keys.length === 0) {
                    done = true;
                }
                else {
                    await Promise.all(keys.map(async (key) => {
                        if (key) {
                            let subscriber;
                            const result = await this.redisClient.get(key);
                            if (result) {
                                subscriber = JSON.parse(result);
                                const subscriptionId = this.generateSubscriptionId(connectionId, subscriber.operationId);
                                const subscriptionOperationKey = helpers_1.prefixRedisKey(`subscriptionOperation:${subscriptionId}`);
                                await Promise.all([
                                    this.redisClient.del(subscriptionOperationKey),
                                    this.redisClient.lrem(subscriptionListKey, 0, key),
                                    this.redisClient.lrem(helpers_1.prefixRedisKey(`eventSubscriptionsList:${subscriber.event}`), 0, key),
                                ]);
                            }
                        }
                    }));
                    await this.redisClient.del(...keys);
                }
            } while (!done);
            await this.redisClient.del(subscriptionListKey);
        };
        this.generateSubscriptionId = (connectionId, operationId) => {
            return `${connectionId}:${operationId}`;
        };
        assert_1.default.ok(redisClient == null || typeof redisClient === 'object', 'Please provide redisClient as an instance of ioredis.Redis');
        this.redisClient = redisClient;
        this.getSubscriptionNameFromEvent = getSubscriptionNameFromEvent;
        this.getSubscriptionNameFromConnection = getSubscriptionNameFromConnection;
    }
}
exports.RedisSubscriptionManager = RedisSubscriptionManager;
//# sourceMappingURL=RedisSubscriptionManager.js.map