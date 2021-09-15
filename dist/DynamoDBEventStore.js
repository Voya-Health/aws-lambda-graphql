"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DynamoDBEventStore = void 0;
const assert_1 = __importDefault(require("assert"));
const client_dynamodb_1 = require("@aws-sdk/client-dynamodb");
const ulid_1 = require("ulid");
const util_dynamodb_1 = require("@aws-sdk/util-dynamodb");
const helpers_1 = require("./helpers");
const DEFAULT_TTL = 7200;
/**
 * DynamoDB event store
 *
 * This event store stores published events in DynamoDB table
 *
 * The server needs to expose DynamoDBEventProcessor handler in order to process these events
 */
class DynamoDBEventStore {
    constructor({ dynamoDbClient, eventsTable = 'Events', ttl = DEFAULT_TTL, } = {}) {
        this.publish = async (event) => {
            await this.db.send(new client_dynamodb_1.PutItemCommand({
                TableName: this.tableName,
                Item: util_dynamodb_1.marshall(Object.assign(Object.assign({ id: ulid_1.ulid() }, event), (this.ttl === false || this.ttl == null
                    ? {}
                    : { ttl: helpers_1.computeTTL(this.ttl) }))),
            }));
        };
        assert_1.default.ok(ttl === false || (typeof ttl === 'number' && ttl > 0), 'Please provide ttl as a number greater than 0 or false to turn it off');
        assert_1.default.ok(dynamoDbClient == null || typeof dynamoDbClient === 'object', 'Please provide dynamoDbClient as an instance of DynamoDB.DocumentClient');
        assert_1.default.ok(typeof eventsTable === 'string', 'Please provide eventsTable as a string');
        this.db = dynamoDbClient || new client_dynamodb_1.DynamoDBClient({});
        this.tableName = eventsTable;
        this.ttl = ttl;
    }
}
exports.DynamoDBEventStore = DynamoDBEventStore;
//# sourceMappingURL=DynamoDBEventStore.js.map