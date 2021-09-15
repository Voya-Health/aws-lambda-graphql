import { IEventStore, SubcribeResolveFn } from './types';
interface PubSubOptions {
    /**
     * Event store instance where events will be published
     */
    eventStore: IEventStore;
    /**
     * Serialize event payload to JSON, default is true.
     */
    serializeEventPayload?: boolean;
    /**
     * Enable console.log
     */
    debug?: boolean;
}
export declare class PubSub {
    private eventStore;
    private serializeEventPayload;
    private debug;
    constructor({ eventStore, serializeEventPayload, debug, }: PubSubOptions);
    subscribe: (eventNames: string | string[]) => SubcribeResolveFn;
    /**
     * Notice that this propagates event through storage
     * So you should not expect to fire in same process
     */
    publish: (eventName: string, payload: any) => Promise<void>;
}
export {};
//# sourceMappingURL=PubSub.d.ts.map