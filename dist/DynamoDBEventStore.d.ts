import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { IEventStore, ISubscriptionEvent } from './types';
export interface IDynamoDBSubscriptionEvent extends ISubscriptionEvent {
    /**
     * TTL in UNIX seconds
     */
    ttl?: number;
}
interface DynamoDBEventStoreOptions {
    /**
     * Use this to override default document client (for example if you want to use local dynamodb)
     */
    dynamoDbClient?: DynamoDBClient;
    /**
     * Events table name (default is Events)
     */
    eventsTable?: string;
    /**
     * Optional TTL for events (stored in ttl field) in seconds
     *
     * Default value is 2 hours
     *
     * Set to false to turn off TTL
     */
    ttl?: number | false;
}
/**
 * DynamoDB event store
 *
 * This event store stores published events in DynamoDB table
 *
 * The server needs to expose DynamoDBEventProcessor handler in order to process these events
 */
export declare class DynamoDBEventStore implements IEventStore {
    private db;
    private tableName;
    private ttl;
    constructor({ dynamoDbClient, eventsTable, ttl, }?: DynamoDBEventStoreOptions);
    publish: (event: ISubscriptionEvent) => Promise<void>;
}
export {};
//# sourceMappingURL=DynamoDBEventStore.d.ts.map