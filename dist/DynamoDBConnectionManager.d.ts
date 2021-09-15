/// <reference types="node" />
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { ApiGatewayManagementApiClient } from '@aws-sdk/client-apigatewaymanagementapi';
import { IConnection, IConnectEvent, IConnectionManager, ISubscriptionManager, IConnectionData, HydrateConnectionOptions } from './types';
interface DynamoDBConnection extends IConnection {
    /**
     * TTL in UNIX seconds
     */
    ttl?: number;
}
interface DynamoDBConnectionManagerOptions {
    /**
     * Use this to override ApiGatewayManagementApi (for example in usage with serverless-offline)
     *
     * If not provided it will be created with endpoint from connections
     */
    apiGatewayManager?: ApiGatewayManagementApiClient;
    region: string;
    /**
     * Connections table name (default is Connections)
     */
    connectionsTable?: string;
    /**
     * Use this to override default document client (for example if you want to use local dynamodb)
     */
    dynamoDbClient?: DynamoDBClient;
    subscriptions: ISubscriptionManager;
    /**
     * Optional TTL for connections (stored in ttl field) in seconds
     *
     * Default value is 2 hours
     *
     * Set to false to turn off TTL
     */
    ttl?: number | false;
    /**
     * Enable console.log
     */
    debug?: boolean;
}
/**
 * DynamoDBConnectionManager
 *
 * Stores connections in DynamoDB table (default table name is Connections, you can override that)
 */
export declare class DynamoDBConnectionManager implements IConnectionManager {
    private apiGatewayManager;
    private region;
    private connectionsTable;
    private db;
    private subscriptions;
    private ttl;
    private debug;
    constructor({ apiGatewayManager, region, connectionsTable, dynamoDbClient, subscriptions, ttl, debug, }: DynamoDBConnectionManagerOptions);
    hydrateConnection: (connectionId: string, options?: HydrateConnectionOptions | undefined) => Promise<DynamoDBConnection>;
    setConnectionData: (data: IConnectionData, { id }: DynamoDBConnection) => Promise<void>;
    registerConnection: ({ connectionId, endpoint, }: IConnectEvent) => Promise<DynamoDBConnection>;
    sendToConnection: (connection: DynamoDBConnection, payload: Buffer) => Promise<void>;
    unregisterConnection: ({ id }: DynamoDBConnection) => Promise<void>;
    closeConnection: ({ id, data }: DynamoDBConnection) => Promise<void>;
    /**
     * Creates api gateway manager
     *
     * If custom api gateway manager is provided, uses it instead
     */
    private createApiGatewayManager;
}
export {};
//# sourceMappingURL=DynamoDBConnectionManager.d.ts.map