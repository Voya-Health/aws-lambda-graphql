import { ISubscriptionEvent, IEventProcessor } from './types';
import { Server } from './Server';
export declare type EventProcessorFn = (events: ISubscriptionEvent[], lambdaContext?: any) => Promise<void>;
export declare class MemoryEventProcessor<TServer extends Server = Server> implements IEventProcessor<TServer, EventProcessorFn> {
    createHandler(server: TServer): EventProcessorFn;
}
//# sourceMappingURL=MemoryEventProcessor.d.ts.map