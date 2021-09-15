import { IConnection, ISubscriber, ISubscriptionEvent, ISubscriptionManager, OperationRequest } from './types';
interface MemorySubscriptionManagerOptions {
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
export declare class MemorySubscriptionManager implements ISubscriptionManager {
    private subscriptions;
    private getSubscriptionNameFromEvent;
    private getSubscriptionNameFromConnection;
    constructor({ getSubscriptionNameFromEvent, getSubscriptionNameFromConnection, }?: MemorySubscriptionManagerOptions);
    subscribersByEvent: (event: ISubscriptionEvent) => AsyncIterable<ISubscriber[]> & AsyncIterator<ISubscriber[]>;
    subscribe: (names: string[], connection: IConnection, operation: OperationRequest & {
        operationId: string;
    }) => Promise<void>;
    unsubscribe: (subscriber: ISubscriber) => Promise<void>;
    unsubscribeOperation: (connectionId: string, operationId: string) => Promise<void>;
    unsubscribeAllByConnectionId: (connectionId: string) => Promise<void>;
}
export {};
//# sourceMappingURL=MemorySubscriptionManager.d.ts.map