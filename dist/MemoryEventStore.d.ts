import { IEventStore, ISubscriptionEvent } from './types';
export declare class MemoryEventStore implements IEventStore {
    events: ISubscriptionEvent[];
    constructor();
    publish: (event: ISubscriptionEvent) => Promise<void>;
}
//# sourceMappingURL=MemoryEventStore.d.ts.map