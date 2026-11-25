"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CONVERSATION_FIELDS = exports.HISTORY_SYNC_FIELDS = void 0;
exports.runHistorySyncNotification = runHistorySyncNotification;
exports.processHistorySyncNotification = processHistorySyncNotification;
const history_blob_1 = require("../persistence/history-blob");
const incoming_1 = require("../../message/primitives/incoming");
const _proto_1 = require("../../proto");
const jid_1 = require("../../protocol/jid");
const message_1 = require("../../protocol/message");
const username_1 = require("../../protocol/username");
const bytes_1 = require("../../util/bytes");
const primitives_1 = require("../../util/primitives");
const proto_stream_1 = require("../../util/proto-stream");
const protoscan_1 = require("../../util/protoscan");
const HANDLED_SYNC_TYPES = new Set([
    _proto_1.proto.Message.HistorySyncType.INITIAL_BOOTSTRAP,
    _proto_1.proto.Message.HistorySyncType.RECENT,
    _proto_1.proto.Message.HistorySyncType.FULL,
    _proto_1.proto.Message.HistorySyncType.PUSH_NAME,
    _proto_1.proto.Message.HistorySyncType.ON_DEMAND,
    _proto_1.proto.Message.HistorySyncType.NON_BLOCKING_DATA
]);
const HISTORY_SYNC_MAX_PENDING_WRITES = 1024;
const HISTORY_SYNC_MAX_RECORD_BYTES = 16 * 1024 * 1024;
/**
 * Ceiling on messages held while waiting for their `Conversation.id`. Field order
 * puts the id first, so this only fills if a serializer reverses it, and letting
 * it grow would rebuild the very peak the streaming pass removes. Exceeding it
 * fails the chunk rather than dropping records: an aborted chunk stays unacked
 * and gets resent, while a dropped one would be acked and lost for good.
 */
const HISTORY_SYNC_MAX_PARKED_MESSAGES = 1024;
exports.HISTORY_SYNC_FIELDS = Object.freeze({
    CONVERSATIONS: 2,
    CHUNK_ORDER: 5,
    PROGRESS: 6,
    PUSHNAMES: 7,
    PHONE_NUMBER_TO_LID_MAPPINGS: 15,
    NCT_SALT: 19,
    INLINE_CONTACTS: 20
});
const HISTORY_SYNC_SINK_KEYS = new Map([
    [3, 'statusV3Messages'],
    [5, 'chunkOrder'],
    [6, 'progress'],
    [7, 'pushnames'],
    [8, 'globalSettings'],
    [9, 'threadIdUserSecret'],
    [10, 'threadDsTimeframeOffset'],
    [11, 'recentStickers'],
    [12, 'pastParticipants'],
    [13, 'callLogRecords'],
    [14, 'aiWaitListState'],
    [15, 'phoneNumberToLidMappings'],
    [16, 'companionMetaNonce'],
    [17, 'shareableChatIdentifierEncryptionKey'],
    [18, 'accounts'],
    [19, 'nctSalt'],
    [20, 'inlineContacts'],
    [21, 'inlineContactsProvided']
]);
exports.CONVERSATION_FIELDS = Object.freeze({
    ID: 1,
    MESSAGES: 2
});
async function runHistorySyncNotification(deps, notification) {
    try {
        await processHistorySyncNotification(deps, notification);
    }
    catch (error) {
        deps.logger.warn('failed to process history sync notification', {
            syncType: notification.syncType,
            chunkOrder: notification.chunkOrder,
            message: (0, primitives_1.toError)(error).message
        });
    }
}
/**
 * Applies one history sync chunk while it downloads, descending into
 * `conversations` so peak memory tracks the largest single message and not the
 * chunk. Chunk-wide records settle after the stream, once every LID mapping is in.
 * `INITIAL_STATUS_V3` is processed only for a sink and never emits `history_sync_chunk`.
 */
async function processHistorySyncNotification(deps, notification) {
    const syncType = notification.syncType;
    if (syncType === null || syncType === undefined) {
        deps.logger.debug('skipping history sync notification without syncType');
        return;
    }
    const isStatusV3 = syncType === _proto_1.proto.Message.HistorySyncType.INITIAL_STATUS_V3;
    if (!HANDLED_SYNC_TYPES.has(syncType) && !(isStatusV3 && deps.sink)) {
        deps.logger.debug('skipping unhandled history sync type', { syncType });
        if (isStatusV3 && deps.onProcessed) {
            await deps.onProcessed(syncType);
        }
        return;
    }
    const blob = await (0, history_blob_1.openHistoryBlobStream)(deps.mediaTransfer, notification, 'history', 'history sync', notification.initialHistBootstrapInlinePayload);
    const state = {
        chunk: { id: deps.chunkId ?? '', syncType },
        nowMs: Date.now(),
        pendingWrites: [],
        pushnames: [],
        inlineContacts: [],
        pnToLid: new Map(),
        tokenConversations: [],
        nctSalt: null,
        chunkOrder: null,
        progress: null,
        conversationsCount: 0,
        messagesCount: 0
    };
    let event;
    try {
        event = await applyHistorySyncChunk(deps, blob, state);
    }
    catch (error) {
        const failure = (0, primitives_1.toError)(error);
        await abortHistorySink(deps, state.chunk, failure);
        throw failure;
    }
    if (!isStatusV3) {
        deps.emitEvent('history_sync_chunk', event);
    }
    if (deps.onProcessed) {
        await deps.onProcessed(syncType);
    }
}
async function applyHistorySyncChunk(deps, blob, state) {
    try {
        await consumeHistorySyncStream(deps, blob.inflated, state);
    }
    catch (error) {
        blob.inflated.destroy((0, primitives_1.toError)(error));
        await Promise.allSettled(state.pendingWrites);
        await blob.verified.catch(() => undefined);
        throw (0, primitives_1.toError)(error);
    }
    await blob.verified;
    await settleHistorySyncChunk(deps, state);
    deps.logger.info('decoded history sync chunk', {
        syncType: state.chunk.syncType,
        chunkOrder: state.chunkOrder,
        progress: state.progress,
        conversations: state.conversationsCount,
        messages: state.messagesCount,
        pushnames: state.pushnames.length,
        inlineContacts: state.inlineContacts.length
    });
    const event = {
        id: state.chunk.id,
        syncType: state.chunk.syncType,
        messagesCount: state.messagesCount,
        conversationsCount: state.conversationsCount,
        pushnamesCount: state.pushnames.length,
        inlineContactsCount: state.inlineContacts.length,
        chunkOrder: state.chunkOrder ?? undefined,
        progress: state.progress ?? undefined
    };
    await (0, history_blob_1.flushPendingWrites)(state.pendingWrites);
    if (deps.sink?.commit) {
        await deps.sink.commit(event);
    }
    return event;
}
/** A throwing `abort` is logged so it never hides the failure that triggered it. */
async function abortHistorySink(deps, chunk, error) {
    if (!deps.sink?.abort) {
        return;
    }
    try {
        await deps.sink.abort(chunk, error);
    }
    catch (abortError) {
        deps.logger.warn('history sync sink abort failed', {
            id: chunk.id,
            message: (0, primitives_1.toError)(abortError).message
        });
    }
}
/** Only a thenable result is surfaced, so a synchronous sink never adds an await. */
function deliverHistoryItem(sink, chunk, item) {
    const result = sink.item(item, chunk);
    return typeof result?.then === 'function'
        ? Promise.resolve(result)
        : undefined;
}
function deliverHistoryValue(sink, chunk, key, value) {
    return deliverHistoryItem(sink, chunk, { kind: key, value });
}
/**
 * Delivers a `HistorySync` field the stores do not consume. The field is decoded
 * from a copy because the reader buffer is reused and `bytes` values are views of
 * their input; one occurrence of a repeated field yields a single element.
 */
function deliverHistoryField(sink, chunk, key, event) {
    const decoded = _proto_1.proto.HistorySync.decode(event.raw.slice())[key];
    const value = Array.isArray(decoded) ? decoded[0] : decoded;
    return value === null || value === undefined
        ? undefined
        : deliverHistoryValue(sink, chunk, key, value);
}
async function consumeHistorySyncStream(deps, inflated, state) {
    let headerParts = null;
    let threadJid = null;
    let parked = null;
    const sink = deps.sink;
    const onEvent = (event) => {
        if (event.kind === proto_stream_1.PROTO_STREAM_EVENT_KINDS.ENTER) {
            headerParts = [];
            threadJid = null;
            parked = null;
            state.conversationsCount += 1;
            return undefined;
        }
        if (event.kind === proto_stream_1.PROTO_STREAM_EVENT_KINDS.LEAVE) {
            const parts = headerParts;
            const jid = threadJid;
            const pending = parked;
            headerParts = null;
            threadJid = null;
            parked = null;
            return parts ? closeConversation(deps, state, parts, jid, pending) : undefined;
        }
        if (event.depth === 1) {
            if (event.fieldNumber === exports.CONVERSATION_FIELDS.MESSAGES &&
                event.wireType === protoscan_1.PROTO_WIRE_TYPES.LEN) {
                const record = _proto_1.proto.HistorySyncMsg.decode(sink ? event.value.slice() : event.value);
                const webMsg = record.message;
                if (!webMsg && !sink) {
                    return undefined;
                }
                const stored = webMsg ? toStoredHistoryMessage(webMsg) : null;
                if (!threadJid) {
                    if (!stored && !sink) {
                        return undefined;
                    }
                    parked ?? (parked = []);
                    if (parked.length >= HISTORY_SYNC_MAX_PARKED_MESSAGES) {
                        throw new Error(`history sync parked ${parked.length} messages with no thread jid, exceeding the ${HISTORY_SYNC_MAX_PARKED_MESSAGES} limit`);
                    }
                    parked[parked.length] = { stored, record: sink ? record : null };
                    return undefined;
                }
                if (stored) {
                    persistHistoryMessage(deps, state, stored, threadJid);
                }
                const delivery = sink
                    ? deliverHistoryItem(sink, state.chunk, {
                        kind: 'conversations.messages',
                        conversationId: threadJid,
                        value: record
                    })
                    : undefined;
                const flush = maybeFlush(state);
                return delivery && flush
                    ? Promise.all([delivery, flush]).then(() => undefined)
                    : (delivery ?? flush);
            }
            if (headerParts) {
                if (event.fieldNumber === exports.CONVERSATION_FIELDS.ID &&
                    event.wireType === protoscan_1.PROTO_WIRE_TYPES.LEN) {
                    threadJid = bytes_1.TEXT_DECODER.decode(event.value);
                }
                headerParts[headerParts.length] = event.raw.slice();
            }
            return undefined;
        }
        if (event.depth !== 0) {
            return undefined;
        }
        if (event.wireType === protoscan_1.PROTO_WIRE_TYPES.VARINT) {
            if (event.fieldNumber === exports.HISTORY_SYNC_FIELDS.CHUNK_ORDER) {
                state.chunkOrder = event.varintValue;
                return sink
                    ? deliverHistoryValue(sink, state.chunk, 'chunkOrder', event.varintValue)
                    : undefined;
            }
            else if (event.fieldNumber === exports.HISTORY_SYNC_FIELDS.PROGRESS) {
                state.progress = event.varintValue;
                return sink
                    ? deliverHistoryValue(sink, state.chunk, 'progress', event.varintValue)
                    : undefined;
            }
        }
        else if (event.wireType === protoscan_1.PROTO_WIRE_TYPES.LEN) {
            if (event.fieldNumber === exports.HISTORY_SYNC_FIELDS.PUSHNAMES) {
                const pushname = _proto_1.proto.Pushname.decode(sink ? event.value.slice() : event.value);
                state.pushnames[state.pushnames.length] = pushname;
                return sink
                    ? deliverHistoryValue(sink, state.chunk, 'pushnames', pushname)
                    : undefined;
            }
            else if (event.fieldNumber === exports.HISTORY_SYNC_FIELDS.PHONE_NUMBER_TO_LID_MAPPINGS) {
                const mapping = _proto_1.proto.PhoneNumberToLIDMapping.decode(sink ? event.value.slice() : event.value);
                if (mapping.pnJid && mapping.lidJid) {
                    state.pnToLid.set(mapping.pnJid, mapping.lidJid);
                }
                return sink
                    ? deliverHistoryValue(sink, state.chunk, 'phoneNumberToLidMappings', mapping)
                    : undefined;
            }
            else if (event.fieldNumber === exports.HISTORY_SYNC_FIELDS.INLINE_CONTACTS) {
                const contact = _proto_1.proto.InlineContact.decode(sink ? event.value.slice() : event.value);
                state.inlineContacts[state.inlineContacts.length] = contact;
                if (contact.pnJid && contact.lidJid) {
                    state.pnToLid.set(contact.pnJid, contact.lidJid);
                }
                return sink
                    ? deliverHistoryValue(sink, state.chunk, 'inlineContacts', contact)
                    : undefined;
            }
            else if (event.fieldNumber === exports.HISTORY_SYNC_FIELDS.NCT_SALT) {
                const salt = event.value.slice();
                state.nctSalt = salt;
                return sink ? deliverHistoryValue(sink, state.chunk, 'nctSalt', salt) : undefined;
            }
        }
        const key = sink ? HISTORY_SYNC_SINK_KEYS.get(event.fieldNumber) : undefined;
        return sink && key ? deliverHistoryField(sink, state.chunk, key, event) : undefined;
    };
    await (0, proto_stream_1.streamProtoFields)(inflated, onEvent, {
        shouldDescend: (fieldNumber, depth) => depth === 0 && fieldNumber === exports.HISTORY_SYNC_FIELDS.CONVERSATIONS,
        maxFieldBytes: HISTORY_SYNC_MAX_RECORD_BYTES
    });
}
async function closeConversation(deps, state, headerParts, threadJid, parked) {
    const conversation = _proto_1.proto.Conversation.decode((0, bytes_1.concatBytes)(headerParts));
    const resolvedJid = threadJid ?? conversation.id;
    if (!resolvedJid) {
        deps.logger.debug('skipping history sync conversation without thread jid');
        return;
    }
    if (conversation.pnJid) {
        const lidJid = conversation.lidJid ?? conversation.accountLid;
        if (lidJid) {
            state.pnToLid.set(conversation.pnJid, lidJid);
        }
    }
    if (deps.onPrivacyTokens &&
        (conversation.tcToken ||
            conversation.tcTokenTimestamp ||
            conversation.tcTokenSenderTimestamp)) {
        state.tokenConversations[state.tokenConversations.length] = {
            jid: resolvedJid,
            tcToken: conversation.tcToken ? new Uint8Array(conversation.tcToken) : undefined,
            tcTokenTimestamp: (0, primitives_1.longToNumber)(conversation.tcTokenTimestamp) || undefined,
            tcTokenSenderTimestamp: (0, primitives_1.longToNumber)(conversation.tcTokenSenderTimestamp) || undefined
        };
    }
    const ephemeralExpiration = conversation.ephemeralExpiration ?? undefined;
    const ephemeralSettingTimestamp = (0, message_1.normalizeEphemeralSettingSeconds)((0, primitives_1.longToNumber)(conversation.ephemeralSettingTimestamp)) ||
        undefined;
    if (deps.chatMetadataStore && ephemeralExpiration !== undefined) {
        pushWrite(state, deps.chatMetadataStore
            .upsertChatMetadata({
            chatJid: resolvedJid,
            ephemeralExpiration,
            ...(ephemeralSettingTimestamp !== undefined
                ? { ephemeralSettingTimestamp }
                : {}),
            updatedAtMs: state.nowMs
        })
            .catch((error) => {
            deps.logger.debug('failed to cache history sync chat metadata', {
                jid: resolvedJid,
                message: (0, primitives_1.toError)(error).message
            });
        }));
    }
    pushWrite(state, deps.writeBehind.persistThreadAsync({
        jid: resolvedJid,
        name: conversation.name ?? undefined,
        unreadCount: conversation.unreadCount ?? undefined,
        archived: conversation.archived ?? undefined,
        pinned: conversation.pinned ?? undefined,
        muteEndMs: (0, primitives_1.longToNumber)(conversation.muteEndTime) || undefined,
        markedAsUnread: conversation.markedAsUnread ?? undefined,
        ephemeralExpiration,
        ephemeralSettingTimestamp
    }));
    if ((0, jid_1.isUserJid)(resolvedJid) || (conversation.lidJid ?? conversation.accountLid)) {
        const contactDisplay = conversation.displayName || undefined;
        const contactUsername = conversation.username
            ? (0, username_1.normalizeUsername)(conversation.username)
            : undefined;
        const contactPn = conversation.pnJid ?? undefined;
        const contactLid = conversation.lidJid ?? conversation.accountLid ?? undefined;
        if (contactDisplay || contactUsername || contactPn || contactLid) {
            pushWrite(state, deps.writeBehind.persistContactAsync({
                jid: contactLid ?? resolvedJid,
                displayName: contactDisplay,
                username: contactUsername,
                phoneNumber: contactPn,
                lastUpdatedMs: state.nowMs
            }));
        }
    }
    const sink = deps.sink;
    for (const entry of parked ?? []) {
        if (entry.stored) {
            persistHistoryMessage(deps, state, entry.stored, resolvedJid);
        }
        if (sink && entry.record) {
            const delivery = deliverHistoryItem(sink, state.chunk, {
                kind: 'conversations.messages',
                conversationId: resolvedJid,
                value: entry.record
            });
            if (delivery) {
                await delivery;
            }
        }
    }
    if (sink) {
        const delivery = deliverHistoryItem(sink, state.chunk, {
            kind: 'conversations',
            value: conversation
        });
        if (delivery) {
            await delivery;
        }
    }
    await maybeFlush(state);
}
/**
 * Resolves the author here rather than at decode time because a message may be
 * read before its `Conversation.id`, and the thread type decides both the
 * self-sent fallback and the shape of the record.
 *
 * `participantJid` carries the group/broadcast author and stays absent in 1:1
 * threads, where `senderJid` falls back to the thread JID - the same shape the
 * live message path persists. A group author that stays unresolved leaves both
 * fields empty: writing the group JID there would read downstream as if the
 * group itself had sent the message.
 */
function persistHistoryMessage(deps, state, message, threadJid) {
    state.messagesCount += 1;
    const authorJid = (0, incoming_1.resolveWebMessageInfoAuthor)(message.authorInfo, deps.meJid, threadJid);
    const isGroupOrBroadcast = (0, jid_1.isGroupJid)(threadJid) || (0, jid_1.isBroadcastJid)(threadJid);
    pushWrite(state, deps.writeBehind.persistMessageAsync({
        id: message.id,
        threadJid,
        senderJid: isGroupOrBroadcast ? authorJid : (authorJid ?? threadJid),
        participantJid: isGroupOrBroadcast ? authorJid : undefined,
        fromMe: message.fromMe,
        timestampMs: message.timestampMs,
        messageBytes: message.messageBytes
    }));
}
async function settleHistorySyncChunk(deps, state) {
    for (const pushname of state.pushnames) {
        if (!pushname.id) {
            continue;
        }
        const lidJid = state.pnToLid.get(pushname.id);
        pushWrite(state, deps.writeBehind.persistContactAsync(lidJid
            ? {
                jid: lidJid,
                pushName: pushname.pushname ?? undefined,
                phoneNumber: pushname.id,
                lastUpdatedMs: state.nowMs
            }
            : {
                jid: pushname.id,
                pushName: pushname.pushname ?? undefined,
                lastUpdatedMs: state.nowMs
            }));
        await maybeFlush(state);
    }
    for (const contact of state.inlineContacts) {
        const jid = contact.lidJid ?? contact.pnJid;
        if (!jid) {
            continue;
        }
        const displayName = contact.fullName || contact.firstName || undefined;
        if (!displayName && !contact.pnJid) {
            continue;
        }
        pushWrite(state, deps.writeBehind.persistContactAsync({
            jid,
            displayName,
            phoneNumber: contact.pnJid ?? undefined,
            lastUpdatedMs: state.nowMs
        }));
        await maybeFlush(state);
    }
    for (const [pnJid, lidJid] of state.pnToLid) {
        pushWrite(state, deps.writeBehind.persistContactAsync({
            jid: lidJid,
            phoneNumber: pnJid,
            lastUpdatedMs: state.nowMs
        }));
        await maybeFlush(state);
    }
    if (deps.onPrivacyTokens && state.tokenConversations.length > 0) {
        pushWrite(state, deps.onPrivacyTokens(state.tokenConversations));
    }
    if (deps.onNctSalt && state.nctSalt) {
        pushWrite(state, deps.onNctSalt(state.nctSalt));
    }
}
/** `null` for content-less stubs, which duplicate live notification stanzas. */
function toStoredHistoryMessage(webMsg) {
    if (!webMsg.key?.id || !webMsg.message) {
        return null;
    }
    const timestampMs = (0, primitives_1.longToNumber)(webMsg.messageTimestamp) * 1000;
    return {
        id: webMsg.key.id,
        authorInfo: {
            key: { fromMe: webMsg.key.fromMe, participant: webMsg.key.participant },
            participant: webMsg.participant,
            originalSelfAuthorUserJidString: webMsg.originalSelfAuthorUserJidString
        },
        fromMe: webMsg.key.fromMe === true,
        timestampMs: timestampMs || undefined,
        messageBytes: _proto_1.proto.Message.encode(webMsg.message).finish()
    };
}
/** The no-op handler keeps a queued rejection observed until the flush rejects on it. */
function pushWrite(state, write) {
    write.catch(() => undefined);
    state.pendingWrites[state.pendingWrites.length] = write;
}
function maybeFlush(state) {
    if (state.pendingWrites.length >= HISTORY_SYNC_MAX_PENDING_WRITES) {
        return (0, history_blob_1.flushPendingWrites)(state.pendingWrites);
    }
    return undefined;
}
