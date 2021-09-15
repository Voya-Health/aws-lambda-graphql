"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.extractEndpointFromEvent = void 0;
function extractEndpointFromEvent(event) {
    return `https://${event.requestContext.domainName}/${event.requestContext.stage}`;
}
exports.extractEndpointFromEvent = extractEndpointFromEvent;
//# sourceMappingURL=extractEndpointFromEvent.js.map