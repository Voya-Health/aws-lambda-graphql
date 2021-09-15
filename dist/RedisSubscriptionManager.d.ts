import { Redis } from 'ioredis';
import { IConnection, ISubscriber, ISubscriptionManager, IdentifiedOperationRequest, ISubscriptionEvent } from './types';
interface RedisSubscriptionManagerOptions {
    /**
     * IORedis client instance
     */
    redisClient: Redis;
    /**
     * Optional function that can get subscription name from event
     *
     * Default is (event: ISubscriptionEvent) => event.event
     *
     * Useful for multi-tenancy
     */
    getSubscriptionNameFromEvent?: (event: ISubscriptionEvent) => string;
    /**
     * Optional function that can get subscription name from subscription connection
     *
     * Default is (name: string, connection: IConnection) => name
     *
     * Useful for multi-tenancy
     */
    getSubscriptionNameFromConnection?: (name: string, connection: IConnection) => string;
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
export declare class RedisSubscriptionManager implements ISubscriptionManager {
    private redisClient;
    private getSubscriptionNameFromEvent;
    private getSubscriptionNameFromConnection;
    constructor({ redisClient, getSubscriptionNameFromEvent, getSubscriptionNameFromConnection, }: RedisSubscriptionManagerOptions);
    subscribersByEvent: (event: ISubscriptionEvent) => AsyncIterable<ISubscriber[]> & AsyncIterator<ISubscriber[]>;
    subscribe: (names: string[], connection: IConnection, operation: IdentifiedOperationRequest) => Promise<void>;
    unsubscribe: () => Promise<void>;
    unsubscribeOperation: (connectionId: string, operationId: string) => Promise<void>;
    unsubscribeAllByConnectionId: (connectionId: string) => Promise<void>;
    generateSubscriptionId: (connectionId: string, operationId: string) => string;
}
export {};
//# sourceMappingURL=RedisSubscriptionManager.d.ts.map