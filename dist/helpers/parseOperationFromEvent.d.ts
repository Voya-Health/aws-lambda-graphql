import { GQLStopOperation, GQLConnectionInit } from '../protocol';
import { ExtendableError } from '../errors';
import { APIGatewayWebSocketEvent, IdentifiedOperationRequest } from '../types';
export declare class MalformedOperationError extends ExtendableError {
    constructor(reason?: string);
}
export declare class InvalidOperationError extends ExtendableError {
    constructor(reason?: string);
}
export declare function parseOperationFromEvent(event: APIGatewayWebSocketEvent): GQLConnectionInit | GQLStopOperation | IdentifiedOperationRequest;
//# sourceMappingURL=parseOperationFromEvent.d.ts.map