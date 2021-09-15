"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.WebSocketConnectionManager = exports.ConnectionNotFoundError = void 0;
const errors_1 = require("./errors");
class ConnectionNotFoundError extends errors_1.ExtendableError {
}
exports.ConnectionNotFoundError = ConnectionNotFoundError;
class WebSocketConnectionManager {
    constructor() {
        this.hydrateConnection = async (connectionId) => {
            // if connection is not found, throw so we can terminate connection
            const connection = this.connections.get(connectionId);
            if (connection == null) {
                throw new ConnectionNotFoundError(`Connection ${connectionId} not found`);
            }
            return connection;
        };
        this.setConnectionData = async (data, connection) => {
            this.connections.set(connection.id, {
                socket: connection.socket,
                id: connection.id,
                data,
            });
        };
        this.registerConnection = async ({ connectionId, endpoint, socket, }) => {
            const connection = {
                socket,
                id: connectionId,
                data: { endpoint, context: {}, isInitialized: false },
            };
            this.connections.set(connectionId, connection);
            return connection;
        };
        this.sendToConnection = (connection, payload) => {
            return new Promise((resolve, reject) => {
                try {
                    connection.socket.send(payload, (err) => err ? reject(err) : resolve());
                }
                catch (e) {
                    reject(e);
                }
            });
        };
        this.unregisterConnection = async (connection) => {
            this.connections.delete(connection.id);
        };
        this.closeConnection = async (connection) => {
            setTimeout(() => {
                // wait so we can send error message first
                connection.socket.close(1011);
            }, 10);
        };
        this.connections = new Map();
    }
}
exports.WebSocketConnectionManager = WebSocketConnectionManager;
//# sourceMappingURL=WebSocketConnectionManager.js.map