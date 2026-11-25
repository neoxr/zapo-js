/** Field names and block keys shared by every encoded wire type, `|` separated. */
export declare const WA_MEX_ARGO_WIRE_POOL = "data|xwa2_message_capping_info|total_quota|Int|used_quota|cycle_start_timestamp|String|cycle_end_timestamp|server_sent_timestamp|ote_status|XWA2MessageCappingOTEStatus|mv_status|XWA2MessageCappingMVStatus|capping_status|XWA2MessageCappingStatus|errors|extensions|xwa2_fetch_account_reachout_timelock|is_active|time_enforcement_ends|enforcement_type|XWA2ReachoutTimelockType";
/** Encoded wire type per zapo operation name, expanded lazily on first use. */
export declare const WA_MEX_ARGO_WIRE_TYPES: Readonly<Record<string, string>>;
