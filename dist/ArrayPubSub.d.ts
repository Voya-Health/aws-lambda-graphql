import { ISubscriptionEvent } from './types';
/**
 * Array PubSub works as local PubSub that is already fed with all the events that were published
 *
 * Each time you call asyncIterator it will create an iterator that iterates over events
 *
 * This PubSub instance is used internally in event processor to simulate
 * source of events. Basically it acts as you were publishing events,
 * which were loaded from event store.
 *
 * If event payload is string, then it's parsed from JSON, otherwise it's used
 * as is and sent to your GraphQL schema.
 */
export declare class ArrayPubSub {
    private events;
    constructor(events: ISubscriptionEvent[]);
    publish(): Promise<void>;
    subscribe(): Promise<number>;
    unsubscribe(): Promise<void>;
    asyncIterator(eventNames: string | string[]): AsyncIterator<any, any, undefined>;
}
//# sourceMappingURL=ArrayPubSub.d.ts.map