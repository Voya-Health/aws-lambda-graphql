import { GQLOperation, GQLConnectionACK, GQLErrorEvent, GQLData, GQLComplete, GQLStopOperation, GQLConnectionInit } from './protocol';
declare type AllowedProtocolEvents = GQLOperation | GQLConnectionACK | GQLErrorEvent | GQLData | GQLComplete | GQLConnectionInit | GQLStopOperation;
export declare function formatMessage(event: AllowedProtocolEvents): string;
export {};
//# sourceMappingURL=formatMessage.d.ts.map