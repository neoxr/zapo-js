/**
 * Argo wire type for a single value.
 *
 * An argo message carries no schema of its own unless the self-describing
 * header flag is set, so the core is just a stream of labels: `1` is a boolean,
 * an array length or a string length depending on the wire type that is being
 * read. Mirrors the type model WhatsApp Android decodes from its bundled wire
 * type store.
 */
export type WaArgoWire = {
    readonly type: 'STRING';
} | {
    readonly type: 'BOOLEAN';
} | {
    readonly type: 'VARINT';
} | {
    readonly type: 'FLOAT64';
} | {
    readonly type: 'BYTES';
} | {
    readonly type: 'FIXED';
    readonly length: number;
} | {
    readonly type: 'DESC';
} | {
    readonly type: 'ERROR';
} | {
    readonly type: 'PATH';
} | {
    readonly type: 'EXTENSIONS';
} | {
    readonly type: 'NULLABLE';
    readonly of: WaArgoWire;
} | {
    readonly type: 'ARRAY';
    readonly of: WaArgoWire;
} | {
    readonly type: 'RECORD';
    readonly fields: readonly WaArgoWireField[];
} | {
    readonly type: 'BLOCK';
    readonly of: WaArgoWire;
    readonly key: string;
    readonly dedupe: boolean;
};
export interface WaArgoWireField {
    readonly name: string;
    readonly of: WaArgoWire;
    readonly omittable: boolean;
}
/**
 * Whether the type writes its own leading label in the core.
 *
 * Labeled types are self-delimiting, so an enclosing `NULLABLE` or omittable
 * field peeks the label instead of consuming a dedicated marker. Only `STRING`,
 * `BYTES` and `BOOLEAN` scalars are labeled, plus every `ARRAY` and `NULLABLE`.
 */
export declare function isLabeledArgoWire(wire: WaArgoWire): boolean;
/**
 * Teaches the argo decoder the wire type of an operation the bundled table does
 * not cover, so `format="argo"` responses for it stop failing. Takes precedence
 * over the bundled entry when both exist.
 */
export declare function registerMexArgoWireType(opName: string, wire: WaArgoWire): void;
/** Wire type for `opName`, or `null` when neither registered nor bundled. */
export declare function resolveMexArgoWireType(opName: string): WaArgoWire | null;
/** Operation names the decoder can currently handle, registered ones included. */
export declare function listMexArgoWireTypeNames(): string[];
