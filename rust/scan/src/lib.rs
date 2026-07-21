use core::ffi::c_char;
use std::collections::BTreeMap;

use incrementalmerkletree::frontier::CommitmentTree;
use incrementalmerkletree::witness::IncrementalWitness;
use incrementalmerkletree::{Hashable, Position, Retention};
use orchard::keys::{FullViewingKey, Scope};
use orchard::note::ExtractedNoteCommitment;
use orchard::note_encryption::{CompactAction, OrchardDomain};
use orchard::tree::MerkleHashOrchard;
use serde::{Deserialize, Serialize};
use shardtree::store::memory::MemoryShardStore;
use shardtree::ShardTree;
use thiserror::Error;
use zcash_note_encryption::{
    batch, try_note_decryption, try_output_recovery_with_ovk, ShieldedOutput,
};
use zcash_primitives::transaction::Transaction;
use zcash_protocol::consensus::BranchId;
use zeroize::Zeroize;

mod zip316;

const HRP_JUNO_UFVK_PREFIX: &str = "jview";
const TYPECODE_ORCHARD: u64 = 3;
const ORCHARD_FVK_LEN: usize = 96;

#[derive(Debug, Error)]
enum ScanError {
    #[error("req_json_invalid")]
    ReqJSONInvalid,
    #[error("tx_hex_invalid")]
    TxHexInvalid,
    #[error("tx_parse_failed")]
    TxParseFailed,
    #[error("ufvk_invalid")]
    UFVKInvalid,
    #[error("ufvk_missing_orchard_receiver")]
    UFVKMissingOrchardReceiver,
    #[error("ufvk_orchard_fvk_len_invalid")]
    UFVKOrchardFVKLenInvalid,
    #[error("ufvk_orchard_fvk_bytes_invalid")]
    UFVKOrchardFVKBytesInvalid,
    #[error("ua_hrp_invalid")]
    UAHrpInvalid,
    #[error("invalid_request")]
    InvalidRequest,
    #[error("internal")]
    Internal,
    #[error("panic")]
    Panic,
}

#[derive(Debug, Deserialize)]
struct WalletIn {
    wallet_id: String,
    ufvk: String,
}

#[derive(Debug, Deserialize)]
struct ScanTxRequest {
    ua_hrp: String,
    wallets: Vec<WalletIn>,
    tx_hex: String,
}

#[derive(Debug, Deserialize)]
struct ScanBlockRequest {
    ua_hrp: String,
    wallets: Vec<WalletIn>,
    tx_hexes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ActionOut {
    action_index: u32,
    action_nullifier: String,
    cmx: String,
    ephemeral_key: String,
    enc_ciphertext: String,
}

#[derive(Debug, Serialize)]
struct NoteOut {
    wallet_id: String,
    action_index: u32,
    diversifier_index: u32,
    recipient_address: String,
    value_zat: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    memo_hex: Option<String>,
    note_nullifier: String,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum ScanTxResponse {
    Ok {
        actions: Vec<ActionOut>,
        notes: Vec<NoteOut>,
    },
    Err {
        error: String,
    },
}

#[derive(Debug, Serialize)]
struct ScanResultOut {
    actions: Vec<ActionOut>,
    notes: Vec<NoteOut>,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum ScanBlockResponse {
    Ok { results: Vec<ScanResultOut> },
    Err { error: String },
}

struct PreparedWallets {
    wallet_ids: Vec<String>,
    fvks: Vec<FullViewingKey>,
    ivks: Vec<orchard::keys::PreparedIncomingViewingKey>,
    ivk_wallet_index: Vec<usize>,
    ivk_full: Vec<orchard::keys::IncomingViewingKey>,
}

#[derive(Debug, Serialize)]
struct OutgoingOutputOut {
    wallet_id: String,
    action_index: u32,
    recipient_address: String,
    value_zat: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    memo_hex: Option<String>,
    ovk_scope: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    recipient_scope: Option<String>,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum RecoverOutgoingTxResponse {
    Ok { outputs: Vec<OutgoingOutputOut> },
    Err { error: String },
}

#[derive(Debug, Deserialize)]
struct ValidateUFVKRequest {
    ufvk: String,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum ValidateUFVKResponse {
    Ok,
    Err { error: String },
}

#[derive(Debug, Deserialize)]
struct WitnessRequest {
    #[serde(default)]
    cmx_hex: Vec<String>,
    #[serde(default)]
    positions: Vec<u32>,
    #[serde(default)]
    anchor_height: Option<u32>,
    #[serde(default)]
    targets: Option<Vec<WitnessTargetReq>>,
    #[serde(default)]
    ops: Option<Vec<WitnessOpReq>>,
}

#[derive(Debug, Deserialize)]
struct WitnessTargetReq {
    position: u32,
    cmx_hex: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WitnessOpReq {
    AppendBatch { cmx_hex: Vec<String> },
    InsertSubtreeRoots { subtree_roots: Vec<String> },
    InsertShardRoots { shard_roots: Vec<String> },
}

#[derive(Debug, Deserialize)]
struct SubtreeRootRequest {
    cmx_hex: Vec<String>,
    #[serde(default = "default_root_level")]
    level: u8,
}

fn default_root_level() -> u8 {
    16
}

#[derive(Debug, Serialize, Clone)]
struct WitnessPathOut {
    position: u32,
    auth_path: Vec<String>,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum WitnessResponse {
    Ok {
        root: String,
        paths: Vec<WitnessPathOut>,
    },
    Err {
        error: String,
    },
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum SubtreeRootResponse {
    Ok { root: String },
    Err { error: String },
}

struct WitnessState<const SHARD_HEIGHT: u8> {
    anchor_height: u32,
    expected_by_position: BTreeMap<u32, MerkleHashOrchard>,
    tree: ShardTree<MemoryShardStore<MerkleHashOrchard, u32>, 32, SHARD_HEIGHT>,
    leaf_count: u64,
    error: Option<String>,
}

impl<const SHARD_HEIGHT: u8> WitnessState<SHARD_HEIGHT> {
    fn new(anchor_height: u32, targets: Vec<WitnessTargetReq>) -> Result<Self, String> {
        if targets.is_empty() {
            return Err("targets_required".to_string());
        }
        if targets.len() > 1000 {
            return Err("too_many_targets".to_string());
        }

        let mut expected_by_position = BTreeMap::new();
        for t in targets {
            let cmx = t.cmx_hex.trim();
            if cmx.is_empty() {
                return Err("invalid_cmx".to_string());
            }
            let leaf = parse_cmx_hex(cmx).map_err(|_| "invalid_cmx".to_string())?;
            if expected_by_position.insert(t.position, leaf).is_some() {
                return Err("invalid_request".to_string());
            }
        }

        Ok(Self {
            anchor_height,
            expected_by_position,
            tree: ShardTree::new(MemoryShardStore::empty(), 2),
            leaf_count: 0,
            error: None,
        })
    }

    fn append(&mut self, cmx_hex: &str) {
        if self.error.is_some() {
            return;
        }

        let leaf = match parse_cmx_hex(cmx_hex) {
            Ok(v) => v,
            Err(_) => {
                self.error = Some("invalid_cmx".to_string());
                return;
            }
        };

        if self.leaf_count > u64::from(u32::MAX) {
            self.error = Some("tree_overflow".to_string());
            return;
        }
        let pos_u32 = self.leaf_count as u32;

        let mut retention = Retention::Ephemeral;
        if let Some(expected) = self.expected_by_position.get(&pos_u32) {
            if expected != &leaf {
                self.error = Some("cmx_mismatch".to_string());
                return;
            }
            retention = Retention::Marked;
        }

        if self.tree.append(leaf, retention).is_err() {
            self.error = Some("tree_append_failed".to_string());
            return;
        }
        self.leaf_count += 1;
    }

    fn insert_cached_root(&mut self, root_hex: &str) {
        if self.error.is_some() {
            return;
        }

        let subtree_leaves: u64 = 1 << SHARD_HEIGHT;
        if self.leaf_count % subtree_leaves != 0 {
            self.error = Some("cached_root_unaligned".to_string());
            return;
        }
        if self.leaf_count > u64::from(u32::MAX) {
            self.error = Some("tree_overflow".to_string());
            return;
        }

        let bytes = match hex::decode(root_hex) {
            Ok(v) => v,
            Err(_) => {
                self.error = Some("invalid_cached_root".to_string());
                return;
            }
        };
        let bytes: [u8; 32] = match bytes.try_into() {
            Ok(v) => v,
            Err(_) => {
                self.error = Some("invalid_cached_root".to_string());
                return;
            }
        };
        let root = match Option::from(MerkleHashOrchard::from_bytes(&bytes)) {
            Some(v) => v,
            None => {
                self.error = Some("invalid_cached_root".to_string());
                return;
            }
        };

        let pos = Position::from(self.leaf_count);
        let addr =
            ShardTree::<MemoryShardStore<MerkleHashOrchard, u32>, 32, SHARD_HEIGHT>::subtree_addr(
                pos,
            );
        if self.tree.insert(addr, root).is_err() {
            self.error = Some("cached_root_insert_failed".to_string());
            return;
        }
        self.leaf_count += subtree_leaves;
    }

    fn finish(mut self) -> Result<(String, Vec<WitnessPathOut>, u64), String> {
        if let Some(err) = self.error.take() {
            return Err(err);
        }

        if !self
            .tree
            .checkpoint(self.anchor_height)
            .map_err(|_| "checkpoint_failed".to_string())?
        {
            return Err("checkpoint_failed".to_string());
        }

        let root = self
            .tree
            .root_at_checkpoint_id(&self.anchor_height)
            .map_err(|_| "tree_root_failed".to_string())?
            .ok_or_else(|| "tree_root_failed".to_string())?;
        let root_hex = hex::encode(root.to_bytes());

        let mut out = Vec::with_capacity(self.expected_by_position.len());
        for (pos_u32, expected_leaf) in &self.expected_by_position {
            if u64::from(*pos_u32) >= self.leaf_count {
                return Err("target_out_of_range".to_string());
            }

            let pos = Position::from(u64::from(*pos_u32));
            let got_leaf = self
                .tree
                .get_marked_leaf(pos)
                .map_err(|_| "witness_missing".to_string())?
                .ok_or_else(|| "witness_missing".to_string())?;
            if &got_leaf != expected_leaf {
                return Err("cmx_mismatch".to_string());
            }

            let path = self
                .tree
                .witness_at_checkpoint_id(pos, &self.anchor_height)
                .map_err(|_| "witness_missing".to_string())?
                .ok_or_else(|| "witness_missing".to_string())?;

            let mut elems = Vec::with_capacity(path.path_elems().len());
            for h in path.path_elems() {
                elems.push(hex::encode(h.to_bytes()));
            }
            out.push(WitnessPathOut {
                position: *pos_u32,
                auth_path: elems,
            });
        }

        Ok((root_hex, out, self.leaf_count))
    }
}

fn to_c_string<T: Serialize>(v: T) -> *mut c_char {
    let json = serde_json::to_string(&v).expect("json");
    std::ffi::CString::new(json).expect("cstr").into_raw()
}

#[no_mangle]
pub extern "C" fn juno_scan_string_free(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    unsafe {
        drop(std::ffi::CString::from_raw(s));
    }
}

#[no_mangle]
pub extern "C" fn juno_scan_scan_tx_json(req_json: *const c_char) -> *mut c_char {
    let res = std::panic::catch_unwind(|| scan_tx_json_inner(req_json));
    match res {
        Ok(Ok(v)) => to_c_string(v),
        Ok(Err(e)) => to_c_string(ScanTxResponse::Err {
            error: e.to_string(),
        }),
        Err(_) => to_c_string(ScanTxResponse::Err {
            error: ScanError::Panic.to_string(),
        }),
    }
}

#[no_mangle]
pub extern "C" fn juno_scan_scan_block_json(req_json: *const c_char) -> *mut c_char {
    let res = std::panic::catch_unwind(|| scan_block_json_inner(req_json));
    match res {
        Ok(Ok(v)) => to_c_string(v),
        Ok(Err(e)) => to_c_string(ScanBlockResponse::Err {
            error: e.to_string(),
        }),
        Err(_) => to_c_string(ScanBlockResponse::Err {
            error: ScanError::Panic.to_string(),
        }),
    }
}

#[no_mangle]
pub extern "C" fn juno_scan_recover_outgoing_tx_json(req_json: *const c_char) -> *mut c_char {
    let res = std::panic::catch_unwind(|| recover_outgoing_tx_json_inner(req_json));
    match res {
        Ok(Ok(v)) => to_c_string(v),
        Ok(Err(e)) => to_c_string(RecoverOutgoingTxResponse::Err {
            error: e.to_string(),
        }),
        Err(_) => to_c_string(RecoverOutgoingTxResponse::Err {
            error: ScanError::Panic.to_string(),
        }),
    }
}

#[no_mangle]
pub extern "C" fn juno_scan_validate_ufvk_json(req_json: *const c_char) -> *mut c_char {
    let res = std::panic::catch_unwind(|| validate_ufvk_json_inner(req_json));
    match res {
        Ok(Ok(v)) => to_c_string(v),
        Ok(Err(e)) => to_c_string(ValidateUFVKResponse::Err {
            error: e.to_string(),
        }),
        Err(_) => to_c_string(ValidateUFVKResponse::Err {
            error: ScanError::Panic.to_string(),
        }),
    }
}

#[no_mangle]
pub extern "C" fn juno_scan_orchard_witness_json(req_json: *const c_char) -> *mut c_char {
    let res = std::panic::catch_unwind(|| orchard_witness_json_inner(req_json));
    match res {
        Ok(Ok(v)) => to_c_string(v),
        Ok(Err(e)) => to_c_string(WitnessResponse::Err {
            error: e.to_string(),
        }),
        Err(_) => to_c_string(WitnessResponse::Err {
            error: ScanError::Panic.to_string(),
        }),
    }
}

#[no_mangle]
pub extern "C" fn juno_scan_orchard_subtree_root_json(req_json: *const c_char) -> *mut c_char {
    let res = std::panic::catch_unwind(|| orchard_subtree_root_json_inner(req_json));
    match res {
        Ok(Ok(v)) => to_c_string(v),
        Ok(Err(e)) => to_c_string(SubtreeRootResponse::Err {
            error: e.to_string(),
        }),
        Err(_) => to_c_string(SubtreeRootResponse::Err {
            error: ScanError::Panic.to_string(),
        }),
    }
}

fn parse_orchard_fvk_from_ufvk(ufvk: &str) -> Result<FullViewingKey, ScanError> {
    let ufvk = ufvk.trim();
    if ufvk.is_empty() {
        return Err(ScanError::UFVKInvalid);
    }

    let (ufvk_hrp, _) = ufvk.split_once('1').ok_or(ScanError::UFVKInvalid)?;
    if !ufvk_hrp.starts_with(HRP_JUNO_UFVK_PREFIX) {
        return Err(ScanError::UFVKInvalid);
    }

    let items = zip316::decode_tlv_container(ufvk_hrp, ufvk).map_err(|_| ScanError::UFVKInvalid)?;
    let orchard_item = items
        .into_iter()
        .find(|(typecode, _)| *typecode == TYPECODE_ORCHARD)
        .ok_or(ScanError::UFVKMissingOrchardReceiver)?;

    if orchard_item.1.len() != ORCHARD_FVK_LEN {
        return Err(ScanError::UFVKOrchardFVKLenInvalid);
    }

    let mut fvk_bytes = [0u8; ORCHARD_FVK_LEN];
    fvk_bytes.copy_from_slice(&orchard_item.1);

    let fvk =
        FullViewingKey::from_bytes(&fvk_bytes).ok_or(ScanError::UFVKOrchardFVKBytesInvalid)?;
    fvk_bytes.zeroize();

    Ok(fvk)
}

fn validate_ufvk_json_inner(req_json: *const c_char) -> Result<ValidateUFVKResponse, ScanError> {
    if req_json.is_null() {
        return Err(ScanError::ReqJSONInvalid);
    }

    let s = unsafe { std::ffi::CStr::from_ptr(req_json) }
        .to_string_lossy()
        .to_string();
    let req: ValidateUFVKRequest =
        serde_json::from_str(&s).map_err(|_| ScanError::ReqJSONInvalid)?;

    let _ = parse_orchard_fvk_from_ufvk(&req.ufvk)?;
    Ok(ValidateUFVKResponse::Ok)
}

fn v5_branch_id_from_tx_bytes(tx_bytes: &[u8]) -> Result<Option<BranchId>, ScanError> {
    if tx_bytes.len() < 4 {
        return Err(ScanError::TxParseFailed);
    }

    let header = u32::from_le_bytes(
        tx_bytes[0..4]
            .try_into()
            .map_err(|_| ScanError::TxParseFailed)?,
    );
    let version = header & 0x7fff_ffff;
    if version != 5 {
        return Ok(None);
    }

    if tx_bytes.len() < 12 {
        return Err(ScanError::TxParseFailed);
    }

    let branch_id = u32::from_le_bytes(
        tx_bytes[8..12]
            .try_into()
            .map_err(|_| ScanError::TxParseFailed)?,
    );
    BranchId::try_from(branch_id)
        .map(Some)
        .map_err(|_| ScanError::TxParseFailed)
}

fn scan_tx_json_inner(req_json: *const c_char) -> Result<ScanTxResponse, ScanError> {
    if req_json.is_null() {
        return Err(ScanError::ReqJSONInvalid);
    }

    let s = unsafe { std::ffi::CStr::from_ptr(req_json) }
        .to_string_lossy()
        .to_string();
    let req: ScanTxRequest = serde_json::from_str(&s).map_err(|_| ScanError::ReqJSONInvalid)?;

    let ua_hrp = validate_ua_hrp(&req.ua_hrp)?;
    let prepared = prepare_wallets(&req.wallets)?;
    let result = scan_tx_prepared(&ua_hrp, &prepared, &req.tx_hex)?;
    Ok(ScanTxResponse::Ok {
        actions: result.actions,
        notes: result.notes,
    })
}

fn scan_block_json_inner(req_json: *const c_char) -> Result<ScanBlockResponse, ScanError> {
    if req_json.is_null() {
        return Err(ScanError::ReqJSONInvalid);
    }
    let s = unsafe { std::ffi::CStr::from_ptr(req_json) }
        .to_string_lossy()
        .to_string();
    let req: ScanBlockRequest = serde_json::from_str(&s).map_err(|_| ScanError::ReqJSONInvalid)?;
    let ua_hrp = validate_ua_hrp(&req.ua_hrp)?;
    let prepared = prepare_wallets(&req.wallets)?;
    let mut results = Vec::with_capacity(req.tx_hexes.len());
    for tx_hex in &req.tx_hexes {
        results.push(scan_tx_prepared(&ua_hrp, &prepared, tx_hex)?);
    }
    Ok(ScanBlockResponse::Ok { results })
}

fn validate_ua_hrp(value: &str) -> Result<String, ScanError> {
    let ua_hrp = value.trim().to_string();
    if ua_hrp.is_empty() || ua_hrp.len() > 16 {
        return Err(ScanError::UAHrpInvalid);
    }
    Ok(ua_hrp)
}

fn prepare_wallets(wallets: &[WalletIn]) -> Result<PreparedWallets, ScanError> {
    let mut prepared = PreparedWallets {
        wallet_ids: Vec::with_capacity(wallets.len()),
        fvks: Vec::with_capacity(wallets.len()),
        ivks: Vec::with_capacity(wallets.len() * 2),
        ivk_wallet_index: Vec::with_capacity(wallets.len() * 2),
        ivk_full: Vec::with_capacity(wallets.len() * 2),
    };
    for w in wallets {
        if w.wallet_id.trim().is_empty() {
            return Err(ScanError::UFVKInvalid);
        }
        let fvk = parse_orchard_fvk_from_ufvk(&w.ufvk)?;
        let widx = prepared.wallet_ids.len();
        prepared.wallet_ids.push(w.wallet_id.trim().to_string());
        prepared.fvks.push(fvk);
        let ivk_external = prepared.fvks[widx].to_ivk(Scope::External);
        let ivk_internal = prepared.fvks[widx].to_ivk(Scope::Internal);
        prepared.ivks.push(ivk_external.prepare());
        prepared.ivk_wallet_index.push(widx);
        prepared.ivk_full.push(ivk_external);
        prepared.ivks.push(ivk_internal.prepare());
        prepared.ivk_wallet_index.push(widx);
        prepared.ivk_full.push(ivk_internal);
    }
    Ok(prepared)
}

fn scan_tx_prepared(
    ua_hrp: &str,
    prepared: &PreparedWallets,
    tx_hex: &str,
) -> Result<ScanResultOut, ScanError> {
    let mut tx_bytes = hex::decode(tx_hex.trim()).map_err(|_| ScanError::TxHexInvalid)?;
    let Some(branch_id) = v5_branch_id_from_tx_bytes(&tx_bytes)? else {
        tx_bytes.zeroize();
        return Ok(ScanResultOut {
            actions: vec![],
            notes: vec![],
        });
    };
    let tx = Transaction::read(&tx_bytes[..], branch_id).map_err(|_| ScanError::TxParseFailed)?;
    tx_bytes.zeroize();
    let orchard_bundle = match tx.orchard_bundle() {
        Some(b) => b,
        None => {
            return Ok(ScanResultOut {
                actions: vec![],
                notes: vec![],
            })
        }
    };

    let mut actions_out = Vec::new();
    let mut outputs = Vec::new();

    for (i, action) in orchard_bundle.actions().iter().enumerate() {
        let compact = CompactAction::from(action);
        let domain = OrchardDomain::for_compact_action(&compact);

        actions_out.push(ActionOut {
            action_index: i as u32,
            action_nullifier: hex::encode(compact.nullifier().to_bytes()),
            cmx: hex::encode(compact.cmx().to_bytes()),
            ephemeral_key: hex::encode(compact.ephemeral_key().0),
            enc_ciphertext: hex::encode(compact.enc_ciphertext()),
        });

        outputs.push((domain, compact));
    }

    if outputs.is_empty() || prepared.ivks.is_empty() {
        return Ok(ScanResultOut {
            actions: actions_out,
            notes: vec![],
        });
    }

    let decrypted = batch::try_compact_note_decryption(&prepared.ivks, &outputs);
    let mut notes_out = Vec::new();

    for (action_index, maybe) in decrypted.into_iter().enumerate() {
        let Some(((note, recipient), ivk_index)) = maybe else {
            continue;
        };

        let wallet_index = *prepared
            .ivk_wallet_index
            .get(ivk_index)
            .ok_or(ScanError::Internal)?;
        let wallet_id = prepared
            .wallet_ids
            .get(wallet_index)
            .ok_or(ScanError::Internal)?
            .clone();

        let di = prepared
            .ivk_full
            .get(ivk_index)
            .and_then(|ivk| ivk.diversifier_index(&recipient))
            .and_then(|di| u32::try_from(di).ok())
            .unwrap_or(0);

        let addr_bytes = recipient.to_raw_address_bytes();
        let recipient_address =
            zip316::encode_unified_container(ua_hrp, TYPECODE_ORCHARD, &addr_bytes)
                .map_err(|_| ScanError::UAHrpInvalid)?;

        let nf = note.nullifier(&prepared.fvks[wallet_index]).to_bytes();
        let action = orchard_bundle
            .actions()
            .get(action_index)
            .ok_or(ScanError::Internal)?;
        let domain = OrchardDomain::for_action(action);
        let memo_hex = match try_note_decryption(&domain, &prepared.ivks[ivk_index], action) {
            Some((_, _, memo)) if !is_empty_memo(&memo) => Some(hex::encode(memo)),
            _ => None,
        };

        notes_out.push(NoteOut {
            wallet_id,
            action_index: action_index as u32,
            diversifier_index: di,
            recipient_address,
            value_zat: note.value().inner().to_string(),
            memo_hex,
            note_nullifier: hex::encode(nf),
        });
    }

    Ok(ScanResultOut {
        actions: actions_out,
        notes: notes_out,
    })
}

fn recover_outgoing_tx_json_inner(
    req_json: *const c_char,
) -> Result<RecoverOutgoingTxResponse, ScanError> {
    if req_json.is_null() {
        return Err(ScanError::ReqJSONInvalid);
    }

    let s = unsafe { std::ffi::CStr::from_ptr(req_json) }
        .to_string_lossy()
        .to_string();
    let req: ScanTxRequest = serde_json::from_str(&s).map_err(|_| ScanError::ReqJSONInvalid)?;

    let ua_hrp = req.ua_hrp.trim().to_string();
    if ua_hrp.is_empty() || ua_hrp.len() > 16 {
        return Err(ScanError::UAHrpInvalid);
    }

    let mut tx_bytes = hex::decode(req.tx_hex.trim()).map_err(|_| ScanError::TxHexInvalid)?;
    let Some(branch_id) = v5_branch_id_from_tx_bytes(&tx_bytes)? else {
        tx_bytes.zeroize();
        return Ok(RecoverOutgoingTxResponse::Ok { outputs: vec![] });
    };
    let tx = Transaction::read(&tx_bytes[..], branch_id).map_err(|_| ScanError::TxParseFailed)?;
    tx_bytes.zeroize();

    let orchard_bundle = match tx.orchard_bundle() {
        Some(b) => b,
        None => {
            return Ok(RecoverOutgoingTxResponse::Ok { outputs: vec![] });
        }
    };

    if orchard_bundle.actions().is_empty() || req.wallets.is_empty() {
        return Ok(RecoverOutgoingTxResponse::Ok { outputs: vec![] });
    }

    let mut outputs_out = Vec::new();

    for w in &req.wallets {
        if w.wallet_id.trim().is_empty() {
            return Err(ScanError::UFVKInvalid);
        }

        let fvk = parse_orchard_fvk_from_ufvk(&w.ufvk)?;

        let ovk_external = fvk.to_ovk(Scope::External);
        let ovk_internal = fvk.to_ovk(Scope::Internal);

        for (action_index, action) in orchard_bundle.actions().iter().enumerate() {
            let domain = OrchardDomain::for_action(action);

            let mut recovered: Option<([u8; 512], orchard::note::Note, orchard::Address, &str)> =
                None;

            if let Some((note, addr, memo)) = try_output_recovery_with_ovk(
                &domain,
                &ovk_external,
                action,
                action.cv_net(),
                &action.encrypted_note().out_ciphertext,
            ) {
                recovered = Some((memo, note, addr, "external"));
            } else if let Some((note, addr, memo)) = try_output_recovery_with_ovk(
                &domain,
                &ovk_internal,
                action,
                action.cv_net(),
                &action.encrypted_note().out_ciphertext,
            ) {
                recovered = Some((memo, note, addr, "internal"));
            }

            let Some((memo, note, addr, ovk_scope)) = recovered else {
                continue;
            };

            let memo_hex = if is_empty_memo(&memo) {
                None
            } else {
                Some(hex::encode(memo))
            };

            let addr_bytes = addr.to_raw_address_bytes();
            let recipient_address =
                zip316::encode_unified_container(&ua_hrp, TYPECODE_ORCHARD, &addr_bytes)
                    .map_err(|_| ScanError::UAHrpInvalid)?;

            let recipient_scope = match fvk.scope_for_address(&addr) {
                Some(Scope::External) => Some("external".to_string()),
                Some(Scope::Internal) => Some("internal".to_string()),
                None => None,
            };

            outputs_out.push(OutgoingOutputOut {
                wallet_id: w.wallet_id.trim().to_string(),
                action_index: action_index as u32,
                recipient_address,
                value_zat: note.value().inner().to_string(),
                memo_hex,
                ovk_scope: ovk_scope.to_string(),
                recipient_scope,
            });
        }
    }

    Ok(RecoverOutgoingTxResponse::Ok {
        outputs: outputs_out,
    })
}

fn orchard_witness_json_inner(req_json: *const c_char) -> Result<WitnessResponse, ScanError> {
    if req_json.is_null() {
        return Err(ScanError::ReqJSONInvalid);
    }

    let s = unsafe { std::ffi::CStr::from_ptr(req_json) }
        .to_string_lossy()
        .to_string();
    let req: WitnessRequest = serde_json::from_str(&s).map_err(|_| ScanError::ReqJSONInvalid)?;

    if req.ops.is_some() || req.targets.is_some() || req.anchor_height.is_some() {
        return orchard_witness_with_ops(req);
    }
    orchard_witness_legacy(req)
}

fn orchard_witness_legacy(req: WitnessRequest) -> Result<WitnessResponse, ScanError> {
    if req.positions.len() > 1000 {
        return Err(ScanError::InvalidRequest);
    }

    let mut leaves = Vec::with_capacity(req.cmx_hex.len());
    for cmx_hex in req.cmx_hex {
        let bytes = parse_hex_32(&cmx_hex).map_err(|_| ScanError::InvalidRequest)?;
        let cmx_ct = ExtractedNoteCommitment::from_bytes(&bytes);
        if bool::from(cmx_ct.is_none()) {
            return Err(ScanError::InvalidRequest);
        }
        let cmx = cmx_ct.unwrap();
        leaves.push(MerkleHashOrchard::from_cmx(&cmx));
    }

    let leaf_count_u32 = u32::try_from(leaves.len()).map_err(|_| ScanError::InvalidRequest)?;
    for &p in &req.positions {
        if p >= leaf_count_u32 {
            return Err(ScanError::InvalidRequest);
        }
    }

    // Build the tree and maintain witnesses for requested positions.
    let mut want = std::collections::HashMap::<u32, usize>::new();
    for (i, p) in req.positions.iter().enumerate() {
        if want.insert(*p, i).is_some() {
            return Err(ScanError::InvalidRequest);
        }
    }

    let mut tree = CommitmentTree::<MerkleHashOrchard, 32>::empty();
    let mut active: Vec<(usize, IncrementalWitness<MerkleHashOrchard, 32>)> = Vec::new();

    for (i, leaf) in leaves.iter().enumerate() {
        tree.append(*leaf).map_err(|_| ScanError::Internal)?;

        for (_, w) in active.iter_mut() {
            w.append(*leaf).map_err(|_| ScanError::Internal)?;
        }

        if let Some(&out_idx) = want.get(&(i as u32)) {
            let w = IncrementalWitness::from_tree(tree.clone()).ok_or(ScanError::Internal)?;
            active.push((out_idx, w));
        }
    }

    let root = tree.root().to_bytes();
    let root_hex = hex::encode(root);

    let mut paths: Vec<Option<WitnessPathOut>> = vec![None; req.positions.len()];
    for (out_idx, w) in active {
        let mp = w.path().ok_or(ScanError::Internal)?;
        let auth_path = mp
            .path_elems()
            .iter()
            .map(|h| hex::encode(h.to_bytes()))
            .collect::<Vec<_>>();
        paths[out_idx] = Some(WitnessPathOut {
            position: req.positions[out_idx],
            auth_path,
        });
    }

    let mut out = Vec::with_capacity(paths.len());
    for p in paths {
        out.push(p.ok_or(ScanError::Internal)?);
    }

    Ok(WitnessResponse::Ok {
        root: root_hex,
        paths: out,
    })
}

fn orchard_witness_with_ops(req: WitnessRequest) -> Result<WitnessResponse, ScanError> {
    let anchor_height = req.anchor_height.ok_or(ScanError::InvalidRequest)?;
    let targets = req.targets.ok_or(ScanError::InvalidRequest)?;
    let ops = req.ops.ok_or(ScanError::InvalidRequest)?;

    if ops
        .iter()
        .any(|op| matches!(op, WitnessOpReq::InsertShardRoots { .. }))
    {
        orchard_witness_with_ops_mode::<12>(anchor_height, targets, ops)
    } else {
        orchard_witness_with_ops_mode::<16>(anchor_height, targets, ops)
    }
}

fn orchard_witness_with_ops_mode<const SHARD_HEIGHT: u8>(
    anchor_height: u32,
    targets: Vec<WitnessTargetReq>,
    ops: Vec<WitnessOpReq>,
) -> Result<WitnessResponse, ScanError> {
    let mut st = match WitnessState::<SHARD_HEIGHT>::new(anchor_height, targets) {
        Ok(s) => s,
        Err(err) => {
            return Ok(WitnessResponse::Err { error: err });
        }
    };

    for op in ops {
        match op {
            WitnessOpReq::AppendBatch { cmx_hex } => {
                for cmx in cmx_hex {
                    st.append(&cmx);
                }
            }
            WitnessOpReq::InsertSubtreeRoots { subtree_roots } => {
                for root in subtree_roots {
                    st.insert_cached_root(&root);
                }
            }
            WitnessOpReq::InsertShardRoots { shard_roots } => {
                for root in shard_roots {
                    st.insert_cached_root(&root);
                }
            }
        }
    }

    let (root, paths, _) = match st.finish() {
        Ok(v) => v,
        Err(err) => return Ok(WitnessResponse::Err { error: err }),
    };

    Ok(WitnessResponse::Ok { root, paths })
}

fn orchard_subtree_root_json_inner(
    req_json: *const c_char,
) -> Result<SubtreeRootResponse, ScanError> {
    if req_json.is_null() {
        return Err(ScanError::ReqJSONInvalid);
    }

    let s = unsafe { std::ffi::CStr::from_ptr(req_json) }
        .to_string_lossy()
        .to_string();
    let req: SubtreeRootRequest =
        serde_json::from_str(&s).map_err(|_| ScanError::ReqJSONInvalid)?;

    match compute_root(&req.cmx_hex, req.level) {
        Ok(root) => Ok(SubtreeRootResponse::Ok { root }),
        Err(err) => Ok(SubtreeRootResponse::Err { error: err }),
    }
}

fn parse_cmx_hex(cmx_hex: &str) -> Result<MerkleHashOrchard, ScanError> {
    let bytes = parse_hex_32(cmx_hex).map_err(|_| ScanError::InvalidRequest)?;
    let cmx_ct = ExtractedNoteCommitment::from_bytes(&bytes);
    if bool::from(cmx_ct.is_none()) {
        return Err(ScanError::InvalidRequest);
    }
    let cmx = cmx_ct.unwrap();
    Ok(MerkleHashOrchard::from_cmx(&cmx))
}

fn compute_root(cmxs: &[String], level: u8) -> Result<String, String> {
    if level != 12 && level != 16 {
        return Err("invalid_root_level".to_string());
    }
    let expected_leaves: usize = 1usize << level;
    if cmxs.len() != expected_leaves {
        return Err("invalid_root_leaf_count".to_string());
    }

    let mut nodes = Vec::with_capacity(expected_leaves);
    for cmx in cmxs {
        let leaf = parse_cmx_hex(cmx).map_err(|_| "invalid_cmx".to_string())?;
        nodes.push(leaf);
    }

    for height in 0u8..level {
        if nodes.len() % 2 != 0 {
            return Err("subtree_build_failed".to_string());
        }
        let mut next = Vec::with_capacity(nodes.len() / 2);
        let mut idx = 0usize;
        while idx < nodes.len() {
            let left = nodes[idx];
            let right = nodes[idx + 1];
            next.push(MerkleHashOrchard::combine(height.into(), &left, &right));
            idx += 2;
        }
        nodes = next;
    }
    if nodes.len() != 1 {
        return Err("subtree_build_failed".to_string());
    }
    Ok(hex::encode(nodes[0].to_bytes()))
}

fn parse_hex_32(s: &str) -> Result<[u8; 32], ()> {
    let b = hex::decode(s).map_err(|_| ())?;
    if b.len() != 32 {
        return Err(());
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&b);
    Ok(out)
}

fn is_empty_memo(memo: &[u8; 512]) -> bool {
    memo[0] == 0xF6 && memo[1..].iter().all(|b| *b == 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use zcash_protocol::constants::{V5_TX_VERSION, V5_VERSION_GROUP_ID};

    fn empty_v5_tx(branch_id: BranchId) -> Vec<u8> {
        let mut tx = Vec::new();
        tx.extend_from_slice(&((1 << 31) | V5_TX_VERSION).to_le_bytes());
        tx.extend_from_slice(&V5_VERSION_GROUP_ID.to_le_bytes());
        tx.extend_from_slice(&u32::from(branch_id).to_le_bytes());
        tx.extend_from_slice(&0u32.to_le_bytes());
        tx.extend_from_slice(&0u32.to_le_bytes());
        tx.push(0);
        tx.push(0);
        tx.push(0);
        tx.push(0);
        tx.push(0);
        tx
    }

    fn empty_v4_tx() -> Vec<u8> {
        let mut tx = Vec::new();
        tx.extend_from_slice(&((1 << 31) | 4u32).to_le_bytes());
        tx.extend_from_slice(&0x892f_2085u32.to_le_bytes());
        tx.extend_from_slice(&0u32.to_le_bytes());
        tx.push(0);
        tx.push(0);
        tx.extend_from_slice(&0u32.to_le_bytes());
        tx
    }

    #[test]
    fn detects_v5_transaction_branch_id() {
        assert_eq!(
            v5_branch_id_from_tx_bytes(&empty_v5_tx(BranchId::Nu6_1)).expect("NU6.1 branch"),
            Some(BranchId::Nu6_1)
        );
        assert_eq!(
            v5_branch_id_from_tx_bytes(&empty_v5_tx(BranchId::Nu6_2)).expect("NU6.2 branch"),
            Some(BranchId::Nu6_2)
        );
        assert_eq!(
            v5_branch_id_from_tx_bytes(&empty_v4_tx()).expect("v4 branch"),
            None
        );
    }

    #[test]
    fn parses_nu6_2_v5_transaction() {
        let tx = Transaction::read(&empty_v5_tx(BranchId::Nu6_2)[..], BranchId::Nu6_2)
            .expect("NU6.2 v5 transaction parses");

        assert_eq!(tx.into_data().consensus_branch_id(), BranchId::Nu6_2);
    }

    #[test]
    fn scan_tx_accepts_nu6_2_v5_transaction() {
        let req = CString::new(format!(
            r#"{{"ua_hrp":"j","wallets":[],"tx_hex":"{}"}}"#,
            hex::encode(empty_v5_tx(BranchId::Nu6_2))
        ))
        .unwrap();

        match scan_tx_json_inner(req.as_ptr()).expect("scan response") {
            ScanTxResponse::Ok { actions, notes } => {
                assert!(actions.is_empty());
                assert!(notes.is_empty());
            }
            ScanTxResponse::Err { error } => panic!("unexpected scan error: {error}"),
        }
    }

    #[test]
    fn scan_tx_ignores_non_v5_transaction() {
        let req = CString::new(format!(
            r#"{{"ua_hrp":"j","wallets":[],"tx_hex":"{}"}}"#,
            hex::encode(empty_v4_tx())
        ))
        .unwrap();

        match scan_tx_json_inner(req.as_ptr()).expect("scan response") {
            ScanTxResponse::Ok { actions, notes } => {
                assert!(actions.is_empty());
                assert!(notes.is_empty());
            }
            ScanTxResponse::Err { error } => panic!("unexpected scan error: {error}"),
        }
    }

    #[test]
    fn scan_block_reuses_prepared_wallet_set_across_transactions() {
        let req = CString::new(format!(
            r#"{{"ua_hrp":"j","wallets":[],"tx_hexes":["{}","{}"]}}"#,
            hex::encode(empty_v5_tx(BranchId::Nu6_2)),
            hex::encode(empty_v4_tx())
        ))
        .unwrap();

        match scan_block_json_inner(req.as_ptr()).expect("scan block response") {
            ScanBlockResponse::Ok { results } => {
                assert_eq!(results.len(), 2);
                assert!(results
                    .iter()
                    .all(|r| r.actions.is_empty() && r.notes.is_empty()));
            }
            ScanBlockResponse::Err { error } => panic!("unexpected scan error: {error}"),
        }
    }

    fn deterministic_valid_cmx(start: u64) -> String {
        for value in start.. {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&value.to_le_bytes());
            if bool::from(ExtractedNoteCommitment::from_bytes(&bytes).is_some()) {
                return hex::encode(bytes);
            }
        }
        unreachable!("the Orchard base field contains valid encodings")
    }

    #[test]
    fn level_12_cached_shard_matches_legacy_witness() {
        const SHARD_LEAVES: usize = 1 << 12;
        let cmx = deterministic_valid_cmx(0);
        let commitments = vec![cmx.clone(); 2 * SHARD_LEAVES];
        let target_position = 7u32;

        let legacy = orchard_witness_legacy(WitnessRequest {
            cmx_hex: commitments.clone(),
            positions: vec![target_position],
            anchor_height: None,
            targets: None,
            ops: None,
        })
        .expect("legacy witness");
        let cached_root = compute_root(&commitments[SHARD_LEAVES..], 12)
            .expect("deterministic cached shard root");
        let cached = orchard_witness_with_ops(WitnessRequest {
            cmx_hex: vec![],
            positions: vec![],
            anchor_height: Some(42),
            targets: Some(vec![WitnessTargetReq {
                position: target_position,
                cmx_hex: cmx.clone(),
            }]),
            ops: Some(vec![
                WitnessOpReq::AppendBatch {
                    cmx_hex: commitments[..SHARD_LEAVES].to_vec(),
                },
                WitnessOpReq::InsertShardRoots {
                    shard_roots: vec![cached_root],
                },
            ]),
        })
        .expect("cached witness");

        let (legacy_root, legacy_paths) = match legacy {
            WitnessResponse::Ok { root, paths } => (root, paths),
            WitnessResponse::Err { error } => panic!("legacy witness failed: {error}"),
        };
        let (cached_root, cached_paths) = match cached {
            WitnessResponse::Ok { root, paths } => (root, paths),
            WitnessResponse::Err { error } => panic!("cached witness failed: {error}"),
        };
        assert_eq!(cached_root, legacy_root);
        assert_eq!(cached_paths.len(), 1);
        assert_eq!(cached_paths[0].position, legacy_paths[0].position);
        assert_eq!(cached_paths[0].auth_path, legacy_paths[0].auth_path);
    }

    #[test]
    fn cached_witness_rejects_target_cmx_mismatch() {
        let expected = deterministic_valid_cmx(0);
        let mut actual_start = 1u64;
        let actual = loop {
            let candidate = deterministic_valid_cmx(actual_start);
            if candidate != expected {
                break candidate;
            }
            actual_start += 1;
        };
        let response = orchard_witness_with_ops(WitnessRequest {
            cmx_hex: vec![],
            positions: vec![],
            anchor_height: Some(42),
            targets: Some(vec![WitnessTargetReq {
                position: 0,
                cmx_hex: expected,
            }]),
            ops: Some(vec![WitnessOpReq::AppendBatch {
                cmx_hex: vec![actual],
            }]),
        })
        .expect("structured mismatch response");

        match response {
            WitnessResponse::Err { error } => assert_eq!(error, "cmx_mismatch"),
            WitnessResponse::Ok { .. } => panic!("mismatched target CMX was accepted"),
        }
    }
}
