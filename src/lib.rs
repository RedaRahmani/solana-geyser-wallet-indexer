use std::{any::Any, fmt::Debug, fs};

use log::{info, LevelFilter};
use env_logger;
use anyhow::{Context, Result};

use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin,
    Result as GeyserResult,
    ReplicaAccountInfoVersions,
    SlotStatus,
};

use serde::Deserialize;

#[derive(Deserialize)]
struct ConfigRoot {
    // Accept either "params" or "args" for flexibility
    #[serde(default)]
    params: Option<Params>,
    #[serde(default)]
    args: Option<Params>,
}

#[derive(Deserialize, Default)]
struct Params {
    #[serde(default)]
    target_wallet: Option<String>,
}

// #[derive(Debug)]
// pub struct LoggerPlugin;
// ---- plugin ----
#[derive(Debug)]
pub struct LoggerPlugin {
    target_wallet: Option<[u8; 32]>,
}

impl LoggerPlugin {
    pub fn new() -> Self {
        let _ = env_logger::builder()
            .format_timestamp_secs()
            .try_init();
        LoggerPlugin { target_wallet: None }
    }

    fn set_target_wallet_from_b58(&mut self, b58: &str) -> Result<()> {
        let bytes = bs58::decode(b58).into_vec()
            .with_context(|| format!("Invalid base58 pubkey: {b58}"))?;
        if bytes.len() != 32 {
            anyhow::bail!("Pubkey must be 32 bytes, got {}", bytes.len());
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        self.target_wallet = Some(arr);
        Ok(())
    }

    fn load_target_from_config(&mut self, path: &str) -> Result<()> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("Cannot read config file: {path}"))?;
        let cfg: ConfigRoot = serde_json::from_str(&raw)
            .with_context(|| "Invalid geyser config JSON")?;

        let params = cfg.params.or(cfg.args).unwrap_or_default();
        if let Some(s) = params.target_wallet {
            self.set_target_wallet_from_b58(&s)?;
            eprintln!("[PLUGIN] target_wallet set to {s}");
        } else {
            eprintln!("[PLUGIN] WARNING: no target_wallet in config; emitting all accounts");
        }
        Ok(())
    }

    #[inline]
    fn matches_target(&self, key: &[u8]) -> bool {
        match self.target_wallet {
            Some(t) => *key == t,
            None => true, // if no target configured, pass through
        }
    }
}


impl GeyserPlugin for LoggerPlugin {

    fn name(&self) -> &'static str {
        "wallet-logger"
    }

    fn setup_logger(
        &self,
        logger: &'static dyn log::Log,
        level: LevelFilter,
    ) -> GeyserResult<()> {

        log::set_max_level(level);

        if let Err(err) = log::set_logger(logger) {

            eprintln!("Failed to set validator logger: {err}");

        }
        Ok(())
    }

    fn on_load(&mut self, config_file: &str, is_reload: bool) -> GeyserResult<()> {
        eprintln!("LoggerPlugin loaded. config_file={config_file}, is_reload={is_reload}");
        if let Err(err) = self.load_target_from_config(config_file) {
            eprintln!("[PLUGIN] ERROR loading target wallet: {err}");
        }
        Ok(())
    }


    fn on_unload(&mut self) {
        eprintln!("LoggerPlugin unloaded");
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn account_data_snapshot_notifications_enabled(&self) -> bool {
        true
    }

    fn transaction_notifications_enabled(&self) -> bool {
        false
    }

    fn entry_notifications_enabled(&self) -> bool {
        false
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions<'_>,
        slot: u64,
        is_startup: bool,
    ) -> GeyserResult<()> {
        if is_startup {
            return Ok(());
        }

         match account {
        ReplicaAccountInfoVersions::V0_0_1(info) => {
                let key: &[u8] = info.pubkey;
                if !self.matches_target(key) { return Ok(()); }
                let pubkey_str = bs58::encode(key).into_string();
                eprintln!("[GEYSER-WALLET-ACCOUNT] v0.0.1: slot={slot}, pubkey={pubkey_str}, lamports={}", info.lamports);
            }
            ReplicaAccountInfoVersions::V0_0_2(info) => {
                let key: &[u8] = info.pubkey;
                if !self.matches_target(key) { return Ok(()); }
                let pubkey_str = bs58::encode(key).into_string();
                eprintln!("[GEYSER-WALLET-ACCOUNT] v0.0.2: slot={slot}, pubkey={pubkey_str}, lamports={}, write_version={}, has_sig={}",
                    info.lamports, info.write_version, info.txn_signature.is_some());
            }
            ReplicaAccountInfoVersions::V0_0_3(info) => {
                let key: &[u8] = info.pubkey;
                if !self.matches_target(key) { return Ok(()); }
                let pubkey_str = bs58::encode(key).into_string();
                eprintln!("[GEYSER-WALLET-ACCOUNT] v0.0.3: slot={slot}, pubkey={pubkey_str}, lamports={}, write_version={}",
                    info.lamports, info.write_version);
            }
        }
        Ok(())
    }

    fn notify_end_of_startup(&self) -> GeyserResult<()> {
        eprintln!("End of startup: all snapshot accounts delivered");
        Ok(())
    }


    fn update_slot_status(
        &self,
        slot: u64,
        parent: Option<u64>,
        status: &SlotStatus,
    ) -> GeyserResult<()> {
        eprintln!(
            "Slot status: slot={slot}, parent={:?}, status={:?}",
            parent, status
        );
        Ok(())
    }
}

/// This is the C entrypoint the validator looks for.
/// Docs show you MUST export `_create_plugin` that returns `*mut dyn GeyserPlugin`.
#[unsafe(no_mangle)]
#[allow(improper_ctypes_definitions)]
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = LoggerPlugin::new();
    let boxed: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(boxed)
}
