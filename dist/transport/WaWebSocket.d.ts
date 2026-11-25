import type { Logger } from '../infra/log/types';
import type { SocketCloseInfo, SocketOpenInfo, WaSocketConfig, WaSocketHandlers } from './types';
/**
 * Thin wrapper around a `RawWebSocket` (or Node `ws` constructor) used by
 * {@link WaComms}. Iterates the configured URL list on failure and applies a
 * connect/idle timeout. Message, error and close events are forwarded only
 * while their socket is the current one; once `close()` is called, messages
 * from that socket are dropped.
 */
export declare class WaWebSocket {
    private readonly config;
    private readonly socketUrls;
    private readonly logger;
    private readonly webSocketCtor;
    private readonly customWebSocketCtor;
    private readonly socketRuntime;
    private readonly connectingSockets;
    private handlers;
    private readonly closeWaiters;
    private socket;
    private socketClosing;
    private nodeWsCtorPromise;
    constructor(config: WaSocketConfig, logger?: Logger);
    setHandlers(handlers: WaSocketHandlers): void;
    isOpen(): boolean;
    isConnecting(): boolean;
    open(): Promise<SocketOpenInfo>;
    close(code?: number, reason?: string): Promise<SocketCloseInfo | null>;
    send(data: string | ArrayBuffer | Uint8Array): Promise<void>;
    private openSingle;
    private openConcurrently;
    private releasePendingSocketsAfterSetupFailure;
    private bindRuntimeHandlers;
    private isDeliverable;
    private handleMessage;
    private normalizePayload;
    private createPendingSocket;
    private bindPendingSocket;
    private settlePendingSocket;
    private releasePendingSocket;
    private activatePendingSocket;
    private toCloseInfo;
    private handleRuntimeError;
    private handleRuntimeClose;
    private settleCloseWaiters;
    private removeCloseWaiter;
    private closeSocketSafe;
    private createRawSocket;
    private resolveNodeWsConstructor;
}
