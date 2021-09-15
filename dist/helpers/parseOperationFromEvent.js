"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.parseOperationFromEvent = exports.InvalidOperationError = exports.MalformedOperationError = void 0;
const protocol_1 = require("../protocol");
const errors_1 = require("../errors");
class MalformedOperationError extends errors_1.ExtendableError {
    constructor(reason) {
        super(reason ? `Malformed operation: ${reason}` : 'Malformed operation');
    }
}
exports.MalformedOperationError = MalformedOperationError;
class InvalidOperationError extends errors_1.ExtendableError {
    constructor(reason) {
        super(reason ? `Invalid operation: ${reason}` : 'Invalid operation');
    }
}
exports.InvalidOperationError = InvalidOperationError;
function parseOperationFromEvent(event) {
    const operation = JSON.parse(event.body);
    if (typeof operation !== 'object' && operation !== null) {
        throw new MalformedOperationError();
    }
    if (operation.type == null) {
        throw new MalformedOperationError('Type is missing');
    }
    if (protocol_1.isGQLConnectionInit(operation)) {
        return operation;
    }
    if (protocol_1.isGQLStopOperation(operation)) {
        return operation;
    }
    if (protocol_1.isGQLConnectionTerminate(operation)) {
        return operation;
    }
    if (protocol_1.isGQLOperation(operation)) {
        if (operation.id == null) {
            throw new MalformedOperationError('Property id is missing');
        }
        if (typeof operation.payload !== 'object' || operation.payload == null) {
            throw new MalformedOperationError('Property payload is missing or is not an object');
        }
        return Object.assign(Object.assign({}, operation.payload), { operationId: operation.id });
    }
    throw new InvalidOperationError('Only GQL_CONNECTION_INIT, GQL_CONNECTION_TERMINATE, GQL_START or GQL_STOP operations are accepted');
}
exports.parseOperationFromEvent = parseOperationFromEvent;
//# sourceMappingURL=parseOperationFromEvent.js.map