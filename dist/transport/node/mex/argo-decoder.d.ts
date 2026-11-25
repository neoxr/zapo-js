import { type WaArgoWire } from '../../node/mex/argo-wire';
export interface WaMexArgoErrorLocation {
    readonly line: number;
    readonly column: number;
}
export interface WaMexArgoError {
    readonly message: string;
    readonly locations: readonly WaMexArgoErrorLocation[] | null;
    /** Response path, resolved to field names where the wire type allows it. */
    readonly path: readonly (string | number)[] | null;
    readonly extensions: Record<string, unknown> | null;
}
export interface WaMexArgoResponse {
    readonly data: unknown;
    readonly errors: readonly WaMexArgoError[];
}
/**
 * Decodes a `format="argo"` MEX result.
 *
 * `wire` is the operation's argo wire type - the payload carries no schema
 * unless the self-describing header flag is set, in which case it is ignored.
 * Every byte of the core and of each claimed block must be consumed, which is
 * what turns a wire type that does not match the response into a hard error
 * instead of a silently wrong value tree.
 */
export declare function decodeMexArgoResponse(bytes: Uint8Array, wire: WaArgoWire): WaMexArgoResponse;
