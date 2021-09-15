import { GraphQLResolveInfo } from 'graphql';
import { IContext, SubcribeResolveFn } from './types';
export declare type FilterFn = (rootValue?: any, args?: any, context?: IContext, info?: GraphQLResolveInfo) => boolean | Promise<boolean>;
declare function withFilter(asyncIteratorFn: SubcribeResolveFn, filterFn: FilterFn): SubcribeResolveFn;
export { withFilter };
export default withFilter;
//# sourceMappingURL=withFilter.d.ts.map