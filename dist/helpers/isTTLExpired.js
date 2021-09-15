"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isTTLExpired = void 0;
function isTTLExpired(ttl) {
    if (ttl == null || ttl === false) {
        return false;
    }
    return ttl * 1000 < Date.now();
}
exports.isTTLExpired = isTTLExpired;
//# sourceMappingURL=isTTLExpired.js.map