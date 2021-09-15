/// <reference types="node" />
import { ApiGatewayManagementApi } from 'aws-sdk';
import { Redis } from 'ioredis';
import { IConnection, IConnectEvent, IConnectionManager, ISubscriptionManager, IConnectionData, HydrateConnectionOptions } from './types';
interface RedisConnectionManagerOptions {
    /**
     * Use this to override ApiGatewayManagementApi (for example in usage with serverless-offline)
     *
     * If not provided it will be created with endpoint from connections
     */
    apiGatewayManager?: ApiGatewayManagementApi;
    /**
     * IORedis client instance
     */
    redisClient: Redis;
    subscriptions: ISubscriptionManager;
}
/**
 * RedisConnectionManager
 *
 * Stores connections in Redis store
 */
export declare class RedisConnectionManager implements IConnectionManager {
    private apiGatewayManager;
    private redisClient;
    private subscriptions;
    constructor({ apiGatewayManager, redisClient, subscriptions, }: RedisConnectionManagerOptions);
    hydrateConnection: (connectionId: string, options: HydrateConnectionOptions) => Promise<IConnection>;
    setConnectionData: (data: IConnectionData, connection: IConnection) => Promise<void>;
    registerConnection: ({ connectionId, endpoint, }: IConnectEvent) => Promise<IConnection>;
    sendToConnection: (connection: IConnection, payload: string | Buffer) => Promise<void>;
    unregisterConnection: ({ id }: IConnection) => Promise<void>;
    closeConnection: ({ id, data }: IConnection) => Promise<void>;
    /**
     * Creates api gateway manager
     *
     * If custom api gateway manager is provided, uses it instead
     */
    private createApiGatewayManager;
}
export {};
//# sourceMappingURL=RedisConnectionManager.d.ts.map