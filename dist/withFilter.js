"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.withFilter = void 0;
const iterall_1 = require("iterall");
function withFilter(asyncIteratorFn, filterFn) {
    return async (rootValue, args, context, info) => {
        const asyncIterator = await asyncIteratorFn(rootValue, args, context, info);
        const getNextPromise = () => {
            return asyncIterator.next().then((payload) => {
                if (payload.done === true) {
                    return payload;
                }
                return Promise.resolve(filterFn(payload.value, args, context, info)).then((filterResult) => {
                    if (filterResult === true) {
                        return payload;
                    }
                    // Skip the current value and wait for the next one
                    return getNextPromise();
                });
            });
        };
        return {
            next() {
                return getNextPromise();
            },
            return() {
                return asyncIterator.return();
            },
            throw(error) {
                return asyncIterator.throw(error);
            },
            [iterall_1.$$asyncIterator]() {
                return this;
            },
        };
    };
}
exports.withFilter = withFilter;
exports.default = withFilter;
//# sourceMappingURL=withFilter.js.map