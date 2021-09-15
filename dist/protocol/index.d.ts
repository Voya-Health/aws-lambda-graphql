import { DocumentNode, ExecutionResult } from 'graphql';
export declare enum CLIENT_EVENT_TYPES {
    GQL_START = "start",
    GQL_STOP = "stop",
    GQL_CONNECTION_INIT = "connection_init",
    GQL_CONNECTION_TERMINATE = "connection_terminate"
}
export declare enum SERVER_EVENT_TYPES {
    GQL_CONNECTION_ACK = "connection_ack",
    GQL_ERROR = "error",
    GQL_DATA = "data",
    GQL_COMPLETE = "complete"
}
/**
 * Client -> Server
 *
 * Starts an operation (query, mutation, subscription)
 *
 * https://github.com/apollographql/subscriptions-transport-ws/blob/master/src/client.ts#L324
 */
export interface GQLOperation {
    id: string;
    payload: {
        [key: string]: any;
        extensions?: {
            [key: string]: any;
        };
        operationName?: string;
        query: string | DocumentNode;
        variables?: {
            [key: string]: any;
        };
    };
    type: CLIENT_EVENT_TYPES.GQL_START;
}
export declare function isGQLOperation(event: any): event is GQLOperation;
/**
 * Client -> Server
 *
 * Stops subscription
 */
export interface GQLStopOperation {
    /** The ID of GQLOperation used to subscribe */
    id: string;
    type: CLIENT_EVENT_TYPES.GQL_STOP;
}
export declare function isGQLStopOperation(event: any): event is GQLStopOperation;
/**
 * Client -> Server
 */
export interface GQLConnectionInit {
    payload?: {
        [key: string]: any;
    };
    type: CLIENT_EVENT_TYPES.GQL_CONNECTION_INIT;
}
export declare function isGQLConnectionInit(event: any): event is GQLConnectionInit;
/**
 * Client -> Server
 */
export interface GQLConnectionTerminate {
    payload?: {
        [key: string]: any;
    };
    type: CLIENT_EVENT_TYPES.GQL_CONNECTION_TERMINATE;
}
export declare function isGQLConnectionTerminate(event: any): event is GQLConnectionTerminate;
/**
 * Server -> Client
 *
 * Subscription is done
 */
export interface GQLComplete {
    /** The ID of GQLOperation used to subscribe */
    id: string;
    type: SERVER_EVENT_TYPES.GQL_COMPLETE;
}
/**
 *  Server -> Client as response to GQLConnectionInit
 */
export interface GQLConnectionACK {
    id?: string;
    payload?: {
        [key: string]: any;
    };
    type: SERVER_EVENT_TYPES.GQL_CONNECTION_ACK;
}
/**
 * Server -> Client as response to operation or just generic error
 */
export interface GQLErrorEvent {
    id?: string;
    payload: {
        message: string;
    };
    type: SERVER_EVENT_TYPES.GQL_ERROR;
}
/**
 * Server -> Client - response to operation
 */
export interface GQLData {
    /**
     * Same ID as the ID of an operation that we are returning a result for
     */
    id: string;
    payload: ExecutionResult;
    type: SERVER_EVENT_TYPES.GQL_DATA;
}
export declare type GQLClientAllEvents = GQLConnectionInit | GQLOperation | GQLStopOperation;
export declare type GQLServerAllEvents = GQLConnectionACK | GQLErrorEvent | GQLData | GQLComplete;
//# sourceMappingURL=index.d.ts.map