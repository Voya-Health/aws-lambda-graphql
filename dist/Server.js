"use strict";
var __rest = (this && this.__rest) || function (s, e) {
    var t = {};
    for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0)
        t[p] = s[p];
    if (s != null && typeof Object.getOwnPropertySymbols === "function")
        for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
            if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i]))
                t[p[i]] = s[p[i]];
        }
    return t;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Server = void 0;
const apollo_server_lambda_1 = require("apollo-server-lambda");
const assert_1 = __importDefault(require("assert"));
const iterall_1 = require("iterall");
const graphql_subscriptions_1 = require("graphql-subscriptions");
const helpers_1 = require("./helpers");
const protocol_1 = require("./protocol");
const formatMessage_1 = require("./formatMessage");
const execute_1 = require("./execute");
class Server extends apollo_server_lambda_1.ApolloServer {
    constructor(_a) {
        var { connectionManager, context, eventProcessor, onError, subscriptionManager, subscriptions } = _a, restConfig = __rest(_a, ["connectionManager", "context", "eventProcessor", "onError", "subscriptionManager", "subscriptions"]);
        super(Object.assign(Object.assign({}, restConfig), { context: 
            // if context is function, pass integration context from graphql server options and then merge the result
            // if it's object, merge it with integrationContext
            typeof context === 'function'
                ? (integrationContext) => Promise.resolve(context(integrationContext)).then((ctx) => (Object.assign(Object.assign({}, ctx), integrationContext)))
                : (integrationContext) => (Object.assign(Object.assign({}, context), integrationContext)) }));
        assert_1.default.ok(connectionManager, 'Please provide connectionManager and ensure it implements IConnectionManager');
        assert_1.default.ok(eventProcessor, 'Please provide eventProcessor and ensure it implements IEventProcessor');
        assert_1.default.ok(subscriptionManager, 'Please provide subscriptionManager and ensure it implements ISubscriptionManager');
        assert_1.default.ok(typeof onError === 'function' || onError == null, 'onError must be a function');
        assert_1.default.ok(subscriptions == null || typeof subscriptions === 'object', 'Property subscriptions must be an object');
        this.connectionManager = connectionManager;
        this.eventProcessor = eventProcessor;
        this.onError = onError || ((err) => console.error(err));
        this.subscriptionManager = subscriptionManager;
        this.subscriptionOptions = subscriptions;
    }
    getConnectionManager() {
        return this.connectionManager;
    }
    getSubscriptionManager() {
        return this.subscriptionManager;
    }
    createGraphQLServerOptions(event, context, internal) {
        const $$internal = Object.assign(Object.assign({}, internal), { connectionManager: this.connectionManager, subscriptionManager: this.subscriptionManager });
        return super
            .graphQLServerOptions(Object.assign({ event, lambdaContext: context, $$internal }, ($$internal.connection && $$internal.connection.data
            ? $$internal.connection.data.context
            : {})))
            .then((options) => (Object.assign(Object.assign({}, options), { $$internal })));
    }
    /**
     * Event handler is responsible for processing published events and sending them
     * to all subscribed connections
     */
    createEventHandler() {
        return this.eventProcessor.createHandler(this);
    }
    /**
     * HTTP event handler is responsible for processing AWS API Gateway v1 events
     */
    createHttpHandler(options) {
        const handler = this.createHandler(options);
        return (event, context) => {
            return new Promise((resolve, reject) => {
                try {
                    handler(event, context, (err, result) => {
                        if (err) {
                            reject(err);
                        }
                        else {
                            resolve(result);
                        }
                    });
                }
                catch (e) {
                    reject(e);
                }
            });
        };
    }
    /**
     * WebSocket handler is responsible for processing AWS API Gateway v2 events
     */
    createWebSocketHandler() {
        return async (event, lambdaContext) => {
            var _a, _b, _c;
            console.debug('=== event', event);
            try {
                // based on routeKey, do actions
                switch (event.requestContext.routeKey) {
                    case '$connect': {
                        const { onWebsocketConnect, connectionEndpoint } = this.subscriptionOptions || {};
                        console.debug('=== connectionEndpoint', connectionEndpoint);
                        console.debug('=== subscriptionOptions', this.subscriptionOptions);
                        console.debug('=== onWebsocketConnect', onWebsocketConnect);
                        // register connection
                        // if error is thrown during registration, connection is rejected
                        // we can implement some sort of authorization here
                        const endpoint = connectionEndpoint || helpers_1.extractEndpointFromEvent(event);
                        const connection = await this.connectionManager.registerConnection({
                            endpoint,
                            connectionId: event.requestContext.connectionId,
                        });
                        let newConnectionContext = {};
                        if (onWebsocketConnect) {
                            try {
                                const result = await onWebsocketConnect(connection, event, lambdaContext);
                                if (result === false) {
                                    throw new Error('Prohibited connection!');
                                }
                                else if (result !== null && typeof result === 'object') {
                                    newConnectionContext = result;
                                }
                            }
                            catch (err) {
                                const errorResponse = formatMessage_1.formatMessage({
                                    type: protocol_1.SERVER_EVENT_TYPES.GQL_ERROR,
                                    payload: { message: err.message },
                                });
                                await this.connectionManager.unregisterConnection(connection);
                                return {
                                    body: errorResponse,
                                    statusCode: 401,
                                };
                            }
                        }
                        // set connection context which will be available during graphql execution
                        const connectionData = Object.assign(Object.assign({}, connection.data), { context: newConnectionContext });
                        console.info('=== connectionData', connectionData);
                        await this.connectionManager.setConnectionData(connectionData, connection);
                        return {
                            body: '',
                            headers: ((_b = (_a = event.headers) === null || _a === void 0 ? void 0 : _a['Sec-WebSocket-Protocol']) === null || _b === void 0 ? void 0 : _b.includes('graphql-ws')) ? {
                                'Sec-WebSocket-Protocol': 'graphql-ws',
                            }
                                : undefined,
                            statusCode: 200,
                        };
                    }
                    case '$disconnect': {
                        const { onDisconnect } = this.subscriptionOptions || {};
                        // this event is called eventually by AWS APIGateway v2
                        // we actualy don't care about a result of this operation because client is already
                        // disconnected, it is meant only for clean up purposes
                        // hydrate connection
                        const connection = await this.connectionManager.hydrateConnection(event.requestContext.connectionId);
                        if (onDisconnect) {
                            onDisconnect(connection);
                        }
                        await this.connectionManager.unregisterConnection(connection);
                        return {
                            body: '',
                            statusCode: 200,
                        };
                    }
                    case '$default': {
                        // here we are processing messages received from a client
                        // if we respond here and the route has integration response assigned
                        // it will send the body back to client, so it is easy to respond with operation results
                        const { connectionId } = event.requestContext;
                        const { onConnect, onOperation, onOperationComplete, waitForInitialization: { retryCount: waitRetryCount = 10, timeout: waitTimeout = 50, } = {}, } = this.subscriptionOptions || {};
                        // parse operation from body
                        const operation = helpers_1.parseOperationFromEvent(event);
                        // hydrate connection
                        let connection = await this.connectionManager.hydrateConnection(connectionId, {
                            retryCount: 1,
                            timeout: waitTimeout,
                        });
                        if (protocol_1.isGQLConnectionInit(operation)) {
                            let newConnectionContext = operation.payload;
                            if (onConnect) {
                                try {
                                    const result = await onConnect(operation.payload, connection, event, lambdaContext);
                                    if (result === false) {
                                        throw new Error('Prohibited connection!');
                                    }
                                    else if (result !== null && typeof result === 'object') {
                                        newConnectionContext = result;
                                    }
                                }
                                catch (err) {
                                    const errorResponse = formatMessage_1.formatMessage({
                                        type: protocol_1.SERVER_EVENT_TYPES.GQL_ERROR,
                                        payload: { message: err.message },
                                    });
                                    await this.connectionManager.sendToConnection(connection, errorResponse);
                                    await this.connectionManager.closeConnection(connection);
                                    return {
                                        body: errorResponse,
                                        statusCode: 401,
                                    };
                                }
                            }
                            // set connection context which will be available during graphql execution
                            const connectionData = Object.assign(Object.assign({}, connection.data), { context: Object.assign(Object.assign({}, (_c = connection.data) === null || _c === void 0 ? void 0 : _c.context), newConnectionContext), isInitialized: true });
                            await this.connectionManager.setConnectionData(connectionData, connection);
                            // send GQL_CONNECTION_INIT message to client
                            const response = formatMessage_1.formatMessage({
                                type: protocol_1.SERVER_EVENT_TYPES.GQL_CONNECTION_ACK,
                            });
                            await this.connectionManager.sendToConnection(connection, response);
                            return {
                                body: response,
                                statusCode: 200,
                            };
                        }
                        // wait for connection to be initialized
                        connection = await (async () => {
                            let freshConnection = connection;
                            if (freshConnection.data.isInitialized) {
                                return freshConnection;
                            }
                            for (let i = 0; i <= waitRetryCount; i++) {
                                freshConnection = await this.connectionManager.hydrateConnection(connectionId);
                                if (freshConnection.data.isInitialized) {
                                    return freshConnection;
                                }
                                // wait for another round
                                await new Promise((r) => setTimeout(r, waitTimeout));
                            }
                            return freshConnection;
                        })();
                        if (!connection.data.isInitialized) {
                            // refuse connection which did not send GQL_CONNECTION_INIT operation
                            const errorResponse = formatMessage_1.formatMessage({
                                type: protocol_1.SERVER_EVENT_TYPES.GQL_ERROR,
                                payload: { message: 'Prohibited connection!' },
                            });
                            await this.connectionManager.sendToConnection(connection, errorResponse);
                            await this.connectionManager.closeConnection(connection);
                            return {
                                body: errorResponse,
                                statusCode: 401,
                            };
                        }
                        if (protocol_1.isGQLStopOperation(operation)) {
                            // unsubscribe client
                            if (onOperationComplete) {
                                onOperationComplete(connection, operation.id);
                            }
                            const response = formatMessage_1.formatMessage({
                                id: operation.id,
                                type: protocol_1.SERVER_EVENT_TYPES.GQL_COMPLETE,
                            });
                            await this.connectionManager.sendToConnection(connection, response);
                            await this.subscriptionManager.unsubscribeOperation(connection.id, operation.id);
                            return {
                                body: response,
                                statusCode: 200,
                            };
                        }
                        if (protocol_1.isGQLConnectionTerminate(operation)) {
                            // unregisterConnection will be handled by $disconnect, return straightaway
                            return {
                                body: '',
                                statusCode: 200,
                            };
                        }
                        const pubSub = new graphql_subscriptions_1.PubSub();
                        // following line is really redundant but we need to
                        // this makes sure that if you invoke the event
                        // and you use Context creator function
                        // then it'll be called with $$internal context according to spec
                        const options = await this.createGraphQLServerOptions(event, lambdaContext, {
                            // this allows createGraphQLServerOptions() to append more extra data
                            // to context from connection.data.context
                            connection,
                            operation,
                            pubSub,
                            registerSubscriptions: true,
                        });
                        const result = await execute_1.execute(Object.assign(Object.assign({}, options), { connection, connectionManager: this.connectionManager, event,
                            lambdaContext,
                            onOperation,
                            operation,
                            pubSub, 
                            // tell execute to register subscriptions
                            registerSubscriptions: true, subscriptionManager: this.subscriptionManager }));
                        if (!iterall_1.isAsyncIterable(result)) {
                            // send response to client so it can finish operation in case of query or mutation
                            if (onOperationComplete) {
                                onOperationComplete(connection, operation.operationId);
                            }
                            const response = formatMessage_1.formatMessage({
                                id: operation.operationId,
                                payload: result,
                                type: protocol_1.SERVER_EVENT_TYPES.GQL_DATA,
                            });
                            await this.connectionManager.sendToConnection(connection, response);
                            return {
                                body: response,
                                statusCode: 200,
                            };
                        }
                        // this is just to make sure
                        // when you deploy this using serverless cli
                        // then integration response is not assigned to $default route
                        // so this won't make any difference
                        // but the sendToConnection above will send the response to client
                        // so client'll receive the response for his operation
                        return {
                            body: '',
                            statusCode: 200,
                        };
                    }
                    default: {
                        throw new Error(`Invalid event ${event.requestContext.routeKey} received`);
                    }
                }
            }
            catch (e) {
                this.onError(e);
                return {
                    body: e.message || 'Internal server error',
                    statusCode: 500,
                };
            }
        };
    }
    installSubscriptionHandlers() {
        throw new Error(`Please don't use this method as this server handles subscriptions in it's own way in createWebSocketHandler()`);
    }
}
exports.Server = Server;
//# sourceMappingURL=Server.js.map