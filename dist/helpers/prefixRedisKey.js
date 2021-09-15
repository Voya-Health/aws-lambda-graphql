"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.prefixRedisKey = void 0;
function prefixRedisKey(key) {
    return `aws-lambda-graphql:${key}`;
}
exports.prefixRedisKey = prefixRedisKey;
//# sourceMappingURL=prefixRedisKey.js.map