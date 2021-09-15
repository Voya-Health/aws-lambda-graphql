import { APIGatewayEvent, Context as LambdaContext } from 'aws-lambda';
import { ASTVisitor, DocumentNode, GraphQLSchema, ExecutionArgs, SubscriptionArgs, ExecutionResult, ValidationContext } from 'graphql';
import { PubSubEngine } from 'graphql-subscriptions';
import { APIGatewayWebSocketEvent, IConnection, IConnectionManager, ISubscriptionManager, OperationRequest } from './types';
export interface ExecutionParams {
    query: DocumentNode;
    variables?: {
        [key: string]: any;
    } | null | undefined;
    operationName?: string | null | undefined;
    context: any;
    schema?: GraphQLSchema;
}
export interface ExecuteOptions {
    connection: IConnection;
    connectionManager: IConnectionManager;
    context?: any;
    event: APIGatewayEvent | APIGatewayWebSocketEvent;
    fieldResolver?: ExecutionArgs['fieldResolver'];
    lambdaContext?: LambdaContext;
    /**
     * Optional function to modify execute options for specific operations
     */
    onOperation?: (message: OperationRequest, params: ExecutionParams, connection: IConnection) => Promise<ExecutionParams> | ExecutionParams;
    operation: OperationRequest;
    pubSub: PubSubEngine;
    /**
     * This is internal param used to indicate if we should register subscriptions to storage
     * Basically this is used by WebSocket handler to manage subscriptions
     * But in case of event processor this is must be false always, because we don't want to register
     * new subscriptions in event processor
     *
     * default is false
     */
    registerSubscriptions?: boolean;
    rootValue?: ExecutionArgs['rootValue'];
    schema: GraphQLSchema;
    subscribeFieldResolver?: SubscriptionArgs['subscribeFieldResolver'];
    subscriptionManager: ISubscriptionManager;
    typeResolver?: ExecutionArgs['typeResolver'];
    /**
     * An optional array of validation rules that will be applied on the document
     * in additional to those defined by the GraphQL spec.
     */
    validationRules?: ((context: ValidationContext) => ASTVisitor)[];
}
/**
 * Executes graphql operation
 *
 * In case of mutation/query it returns ExecutionResult
 * In case of subscriptions it returns AsyncIterator of ExecutionResults (only if useSubscriptions is true)
 */
export declare function execute({ connection, connectionManager, context, event, fieldResolver, lambdaContext, onOperation, operation, pubSub, rootValue, schema, subscriptionManager, registerSubscriptions, typeResolver, validationRules, }: ExecuteOptions): Promise<ExecutionResult | AsyncIterator<ExecutionResult>>;
//# sourceMappingURL=execute.d.ts.map