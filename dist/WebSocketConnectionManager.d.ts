/// <reference types="node" />
import * as WebSocket from 'ws';
import { ExtendableError } from './errors';
import { IConnection, IConnectEvent, IConnectionManager, IConnectionData } from './types';
export declare class ConnectionNotFoundError extends ExtendableError {
}
interface WSConnectEvent extends IConnectEvent {
    socket: WebSocket;
}
interface WSConnection extends IConnection {
    socket: WebSocket;
}
export declare class WebSocketConnectionManager implements IConnectionManager {
    connections: Map<string, WSConnection>;
    constructor();
    hydrateConnection: (connectionId: string) => Promise<IConnection>;
    setConnectionData: (data: IConnectionData, connection: WSConnection) => Promise<void>;
    registerConnection: ({ connectionId, endpoint, socket, }: WSConnectEvent) => Promise<WSConnection>;
    sendToConnection: (connection: WSConnection, payload: string | Buffer) => Promise<void>;
    unregisterConnection: (connection: IConnection) => Promise<void>;
    closeConnection: (connection: WSConnection) => Promise<void>;
}
export {};
//# sourceMappingURL=WebSocketConnectionManager.d.ts.map