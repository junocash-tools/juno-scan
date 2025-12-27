use core::ffi::c_char;

use orchard::keys::{FullViewingKey, Scope};
use orchard::note_encryption::{CompactAction, OrchardDomain};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zcash_note_encryption::{batch, ShieldedOutput};
use zcash_primitives::transaction::Transaction;
use zcash_protocol::consensus::BranchId;
use zeroize::Zeroize;

mod zip316;

const HRP_JUNO_UFVK: &str = "jview";
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

fn scan_tx_json_inner(req_json: *const c_char) -> Result<ScanTxResponse, ScanError> {
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
    let tx = Transaction::read(&tx_bytes[..], BranchId::Nu6_1).map_err(|_| ScanError::TxParseFailed)?;
    tx_bytes.zeroize();

    let orchard_bundle = match tx.orchard_bundle() {
        Some(b) => b,
        None => {
            return Ok(ScanTxResponse::Ok {
                actions: vec![],
                notes: vec![],
            })
        }
    };

    let mut wallet_ids = Vec::with_capacity(req.wallets.len());
    let mut fvks = Vec::with_capacity(req.wallets.len());
    let mut ivks = Vec::with_capacity(req.wallets.len() * 2);
    let mut ivk_wallet_index = Vec::with_capacity(req.wallets.len() * 2);
    let mut ivk_full = Vec::with_capacity(req.wallets.len() * 2);

    for w in &req.wallets {
        if w.wallet_id.trim().is_empty() {
            return Err(ScanError::UFVKInvalid);
        }

        let items = zip316::decode_tlv_container(HRP_JUNO_UFVK, w.ufvk.trim())
            .map_err(|_| ScanError::UFVKInvalid)?;
        let orchard_item = items
            .into_iter()
            .find(|(typecode, _)| *typecode == TYPECODE_ORCHARD)
            .ok_or(ScanError::UFVKMissingOrchardReceiver)?;

        if orchard_item.1.len() != ORCHARD_FVK_LEN {
            return Err(ScanError::UFVKOrchardFVKLenInvalid);
        }

        let mut fvk_bytes = [0u8; ORCHARD_FVK_LEN];
        fvk_bytes.copy_from_slice(&orchard_item.1);

        let fvk = FullViewingKey::from_bytes(&fvk_bytes).ok_or(ScanError::UFVKOrchardFVKBytesInvalid)?;
        fvk_bytes.zeroize();

        let widx = wallet_ids.len();
        wallet_ids.push(w.wallet_id.trim().to_string());
        fvks.push(fvk);

        let ivk_external = fvks[widx].to_ivk(Scope::External);
        let ivk_internal = fvks[widx].to_ivk(Scope::Internal);
        let pivk_external = ivk_external.prepare();
        let pivk_internal = ivk_internal.prepare();

        ivks.push(pivk_external);
        ivk_wallet_index.push(widx);
        ivk_full.push(ivk_external);
        ivks.push(pivk_internal);
        ivk_wallet_index.push(widx);
        ivk_full.push(ivk_internal);
    }

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

    if outputs.is_empty() || ivks.is_empty() {
        return Ok(ScanTxResponse::Ok {
            actions: actions_out,
            notes: vec![],
        });
    }

    let decrypted = batch::try_compact_note_decryption(&ivks, &outputs);
    let mut notes_out = Vec::new();

    for (action_index, maybe) in decrypted.into_iter().enumerate() {
        let Some(((note, recipient), ivk_index)) = maybe else {
            continue;
        };

        let wallet_index = *ivk_wallet_index
            .get(ivk_index)
            .ok_or(ScanError::Internal)?;
        let wallet_id = wallet_ids
            .get(wallet_index)
            .ok_or(ScanError::Internal)?
            .clone();

        let di = ivk_full
            .get(ivk_index)
            .and_then(|ivk| ivk.diversifier_index(&recipient))
            .and_then(|di| u32::try_from(di).ok())
            .unwrap_or(0);

        let addr_bytes = recipient.to_raw_address_bytes();
        let recipient_address = zip316::encode_unified_container(&ua_hrp, TYPECODE_ORCHARD, &addr_bytes)
            .map_err(|_| ScanError::UAHrpInvalid)?;

        let nf = note.nullifier(&fvks[wallet_index]).to_bytes();

        notes_out.push(NoteOut {
            wallet_id,
            action_index: action_index as u32,
            diversifier_index: di,
            recipient_address,
            value_zat: note.value().inner().to_string(),
            note_nullifier: hex::encode(nf),
        });
    }

    Ok(ScanTxResponse::Ok {
        actions: actions_out,
        notes: notes_out,
    })
}
