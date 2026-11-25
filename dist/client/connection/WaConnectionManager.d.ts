import type { WaAuthClient } from '../../auth/WaAuthClient';
import type { WaPassiveTasksCoordinator } from '../coordinators/WaPassiveTasksCoordinator';
import type { WaClientOptions } from '../types';
import type { Logger } from '../../infra/log/types';
import type { WaMediaConn } from '../../media/types';
import type { WaKeepAlive } from '../../transport/keepalive/WaKeepAlive';
import type { WaNodeOrchestrator } from '../../transport/node/WaNodeOrchestrator';
import type { WaNodeTransport } from '../../transport/node/WaNodeTransport';
import { WaComms } from '../../transport/WaComms';
interface WaConnectionManagerOptions {
    readonly logger: Logger;
    readonly options: Readonly<WaClientOptions>;
    readonly authClient: WaAuthClient;
    readonly keepAlive: WaKeepAlive;
    readonly nodeOrchestrator: WaNodeOrchestrator;
    readonly nodeTransport: WaNodeTransport;
    readonly getPassiveTasks: () => WaPassiveTasksCoordinator | null;
    readonly clearStoredCredentials: () => Promise<void>;
    readonly onPostPairReconnected: () => void;
}
export declare class WaConnectionManager {
    private readonly logger;
    private readonly options;
    private readonly authClient;
    private readonly keepAlive;
    private readonly nodeOrchestrator;
    private readonly nodeTransport;
    private readonly getPassiveTasks;
    private readonly clearStoredCredentialsCallback;
    private readonly onPostPairReconnected;
    private comms;
    private clockSkewMs;
    private mediaConnCache;
    private connectPromise;
    private pairingReconnectPromise;
    private pairRestartTimer;
    private frameHandler;
    private pendingComms;
    private lifecycleGeneration;
    private lifecycleQueue;
    constructor(options: WaConnectionManagerOptions);
    connect(frameHandler: (frame: Uint8Array) => Promise<void>): Promise<void>;
    /**
     * Starts the post-pairing restart window: the server ends the unregistered connection
     * itself (`stream:error` 515, as WA Web waits for), so the socket stays open until that
     * signal, a server close, or the fallback timer, whichever comes first.
     */
    awaitServerRestartAfterPairing(): void;
    /**
     * Claims a `stream:error` 515 for the post-pairing restart. Returns `true` while the
     * window is open or its restart is running, so the generic force-login path must skip it.
     */
    handleServerRestartRequest(): boolean;
    disconnect(): Promise<void>;
    clearStoredCredentials(): Promise<void>;
    isConnected(): boolean;
    getComms(): WaComms | null;
    getClockSkewMs(): number | null;
    getMediaConnCache(): WaMediaConn | null;
    setMediaConnCache(value: WaMediaConn | null): void;
    updateClockSkewFromSuccess(serverUnixSeconds: number): void;
    setClockSkewMs(clockSkewMs: number, source: string): void;
    private connectInternal;
    private startPairRestart;
    private clearPairRestartTimer;
    private reconnectAsRegisteredAfterPairing;
    private reconnectAsRegisteredAfterPairingInternal;
    private disconnectInternal;
    private startCommsWithCredentials;
    private clearCommsBinding;
    private stopCommsQuietly;
    private nextLifecycleGeneration;
    private runLifecycleOperation;
    private invalidatePendingComms;
    private isLifecycleCurrent;
    private assertLifecycleCurrent;
    private createLifecycleSupersededError;
    private isLifecycleSupersededError;
}
export {};
