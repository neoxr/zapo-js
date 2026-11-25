"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.runMexQuery = runMexQuery;
exports.dispatchMexQuery = dispatchMexQuery;
const _mex_1 = require("../../../mex");
const constants_1 = require("../../../protocol/constants");
const helpers_1 = require("../../node/helpers");
const argo_decoder_1 = require("../../node/mex/argo-decoder");
const argo_wire_1 = require("../../node/mex/argo-wire");
const query_1 = require("../../node/query");
const bytes_1 = require("../../../util/bytes");
const primitives_1 = require("../../../util/primitives");
const MEX_DEFAULT_TIMEOUT_MS = 32000;
/**
 * Typed wrapper around `dispatchMexQuery` that resolves persist IDs from the
 * generated `WA_MEX_PERSIST_IDS` table. The op key constrains the `variables`
 * shape via `WaMexOperationVariables` and defaults the response to
 * `WaMexOperationResponses[K] | null` (the response tree the extractor walked
 * from the Relay `operation.selections`; leaves are `unknown` since Relay
 * strips scalar types at codegen).
 */
async function runMexQuery(socket, opKey, variables) {
    const persist = _mex_1.WA_MEX_PERSIST_IDS[opKey];
    if (!persist) {
        throw new Error(`mex/${String(opKey)} persist IDs not found`);
    }
    const { data } = await dispatchMexQuery(socket, {
        docId: persist.docId,
        clientDocId: persist.clientDocId,
        opName: opKey,
        variables: variables
    });
    return data;
}
async function dispatchMexQuery(socket, args) {
    const body = JSON.stringify({
        queryId: args.clientDocId,
        variables: args.variables
    });
    const node = {
        tag: constants_1.WA_NODE_TAGS.IQ,
        attrs: {
            ...(args.iqId ? { id: args.iqId } : {}),
            to: constants_1.WA_DEFAULTS.HOST_DOMAIN,
            type: constants_1.WA_IQ_TYPES.GET,
            xmlns: constants_1.WA_XMLNS.MEX
        },
        content: [
            {
                tag: 'query',
                attrs: { query_id: args.docId },
                content: body
            }
        ]
    };
    const response = await socket.query(node, args.timeoutMs ?? MEX_DEFAULT_TIMEOUT_MS);
    (0, query_1.assertIqResult)(response, `mex/${args.opName}`);
    const data = parseMexResultPayload(response, args.opName);
    return { raw: response, data };
}
function parseMexResultPayload(node, opName) {
    const result = (0, helpers_1.findNodeChild)(node, 'result');
    if (!result) {
        throw new Error(`mex/${opName} response missing <result>`);
    }
    const format = result.attrs.format ?? 'json';
    const rawBytes = (0, helpers_1.decodeNodeContentUtf8OrBytes)(result.content, `mex/${opName}/result`);
    if (format === 'argo') {
        return parseMexArgoPayload(rawBytes, opName);
    }
    return extractGraphQlData(JSON.parse(bytes_1.TEXT_DECODER.decode(rawBytes)), opName);
}
function parseMexArgoPayload(rawBytes, opName) {
    const wire = (0, argo_wire_1.resolveMexArgoWireType)(opName);
    if (!wire) {
        throw new Error(`mex/${opName} argo response has no known wire type; register one with ` +
            `registerMexArgoWireType('${opName}', ...); ${rawBytes.length}B; ` +
            `strings: ${extractVisibleStrings(rawBytes)}`);
    }
    let decoded;
    try {
        decoded = (0, argo_decoder_1.decodeMexArgoResponse)(rawBytes, wire);
    }
    catch (error) {
        throw new Error(`mex/${opName} argo decode failed: ${(0, primitives_1.toError)(error).message}; ` +
            `${rawBytes.length}B; strings: ${extractVisibleStrings(rawBytes)}`);
    }
    if (decoded.errors.length > 0) {
        const summary = decoded.errors
            .map((error) => {
            const code = error.extensions?.error_code ?? error.extensions?.code ?? '?';
            const severity = error.extensions?.severity;
            const path = error.path?.join('.') ?? '';
            return `${code}${severity ? `/${String(severity)}` : ''}: ${error.message} @${path}`;
        })
            .join('; ');
        throw new Error(`mex/${opName} errors: ${summary}`);
    }
    return decoded.data;
}
function extractVisibleStrings(bytes) {
    return bytes_1.TEXT_DECODER.decode(bytes)
        .replace(/[\x00-\x1f\x7f-\xff]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}
function extractGraphQlData(payload, opName) {
    const json = (payload ?? {});
    if (json.errors && json.errors.length > 0) {
        const summary = json.errors
            .map((e) => `${e.extensions?.error_code ?? '?'}: ${e.message ?? '?'} @${(e.path ?? []).join('.')}`)
            .join('; ');
        throw new Error(`mex/${opName} errors: ${summary}`);
    }
    return json.data ?? null;
}
