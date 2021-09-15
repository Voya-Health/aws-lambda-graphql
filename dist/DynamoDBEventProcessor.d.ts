import { DynamoDBStreamHandler } from 'aws-lambda';
import { IEventProcessor } from './types';
import { Server } from './Server';
interface DynamoDBEventProcessorOptions {
    onError?: (err: any) => void;
    /**
     * Enable log
     */
    debug?: boolean;
    /**
     * Allow injecting a logging function
     */
    log?: (message: any, ...optionalParams: any[]) => void;
}
/**
 * DynamoDBEventProcessor
 *
 * Processes DynamoDB stream event in order to send events to subscribed clients
 */
export declare class DynamoDBEventProcessor<TServer extends Server = Server> implements IEventProcessor<TServer, DynamoDBStreamHandler> {
    private onError;
    private debug;
    private log;
    constructor(options?: DynamoDBEventProcessorOptions);
    createHandler(server: TServer): DynamoDBStreamHandler;
}
export {};
//# sourceMappingURL=DynamoDBEventProcessor.d.ts.map