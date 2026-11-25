import { WA_MEX_ARGO_WIRE_POOL, WA_MEX_ARGO_WIRE_TYPES } from '../../node/mex/argo-wire-types.js';
/**
 * Whether the type writes its own leading label in the core.
 *
 * Labeled types are self-delimiting, so an enclosing `NULLABLE` or omittable
 * field peeks the label instead of consuming a dedicated marker. Only `STRING`,
 * `BYTES` and `BOOLEAN` scalars are labeled, plus every `ARRAY` and `NULLABLE`.
 */
export function isLabeledArgoWire(wire) {
    switch (wire.type) {
        case 'STRING':
        case 'BYTES':
        case 'BOOLEAN':
        case 'ARRAY':
        case 'NULLABLE':
            return true;
        case 'BLOCK':
            return isLabeledArgoWire(wire.of);
        default:
            return false;
    }
}
const SCALAR_TOKENS = Object.freeze({
    S: { type: 'STRING' },
    B: { type: 'BOOLEAN' },
    I: { type: 'VARINT' },
    F: { type: 'FLOAT64' },
    Y: { type: 'BYTES' },
    D: { type: 'DESC' },
    E: { type: 'ERROR' },
    P: { type: 'PATH' },
    X: { type: 'EXTENSIONS' }
});
let pool = null;
function poolEntry(index) {
    pool ?? (pool = WA_MEX_ARGO_WIRE_POOL.split('|'));
    const value = pool[index];
    if (value === undefined)
        throw new Error(`mex argo: wire pool index ${index} is out of range`);
    return value;
}
/** Reads a base 36 pool index, leaving its `;`/`?` terminator in place. */
function readPoolRef(cursor) {
    const start = cursor.pos;
    while (cursor.pos < cursor.stream.length) {
        const char = cursor.stream[cursor.pos];
        if (char === ';' || char === '?')
            break;
        cursor.pos++;
    }
    return poolEntry(Number.parseInt(cursor.stream.slice(start, cursor.pos), 36));
}
/** Reads a decimal count and consumes its `;` terminator. */
function readCount(cursor) {
    const start = cursor.pos;
    while (cursor.stream[cursor.pos] !== ';') {
        if (cursor.pos >= cursor.stream.length)
            throw new Error('mex argo: unterminated wire count');
        cursor.pos++;
    }
    const value = Number.parseInt(cursor.stream.slice(start, cursor.pos), 10);
    cursor.pos++;
    return value;
}
/**
 * Expands the token stream emitted by `scripts/generate-mex-argo-wire-types.cjs`.
 *
 * Grammar: scalars are a single letter, `?x` is nullable, `*x` an array,
 * `f<len>;` a fixed blob, `b<key>;x` / `n<key>;x` a deduped / plain block, and
 * `r<count>;(<name>[;?]x)*` a record whose fields carry `;` when required and
 * `?` when omittable.
 */
function expandArgoWire(cursor) {
    const token = cursor.stream[cursor.pos++];
    const scalar = SCALAR_TOKENS[token];
    if (scalar)
        return scalar;
    switch (token) {
        case '?':
            return { type: 'NULLABLE', of: expandArgoWire(cursor) };
        case '*':
            return { type: 'ARRAY', of: expandArgoWire(cursor) };
        case 'f':
            return { type: 'FIXED', length: readCount(cursor) };
        case 'b':
        case 'n': {
            const key = readPoolRef(cursor);
            cursor.pos++;
            return { type: 'BLOCK', of: expandArgoWire(cursor), key, dedupe: token === 'b' };
        }
        case 'r': {
            const count = readCount(cursor);
            const fields = new Array(count);
            for (let i = 0; i < count; i++) {
                const name = readPoolRef(cursor);
                const omittable = cursor.stream[cursor.pos++] === '?';
                fields[i] = { name, of: expandArgoWire(cursor), omittable };
            }
            return { type: 'RECORD', fields };
        }
        default:
            throw new Error(`mex argo: unknown wire type token '${token}'`);
    }
}
const registered = new Map();
const expanded = new Map();
function loadBundled(opName) {
    const cached = expanded.get(opName);
    if (cached)
        return cached;
    const stream = WA_MEX_ARGO_WIRE_TYPES[opName];
    if (stream === undefined)
        return null;
    const cursor = { stream, pos: 0 };
    const wire = expandArgoWire(cursor);
    if (cursor.pos !== stream.length) {
        throw new Error(`mex argo: wire stream for ${opName} has trailing tokens`);
    }
    expanded.set(opName, wire);
    return wire;
}
/**
 * Teaches the argo decoder the wire type of an operation the bundled table does
 * not cover, so `format="argo"` responses for it stop failing. Takes precedence
 * over the bundled entry when both exist.
 */
export function registerMexArgoWireType(opName, wire) {
    registered.set(opName, wire);
}
/** Wire type for `opName`, or `null` when neither registered nor bundled. */
export function resolveMexArgoWireType(opName) {
    return registered.get(opName) ?? loadBundled(opName);
}
/** Operation names the decoder can currently handle, registered ones included. */
export function listMexArgoWireTypeNames() {
    return [...new Set([...Object.keys(WA_MEX_ARGO_WIRE_TYPES), ...registered.keys()])].sort();
}
