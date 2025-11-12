use std::any::Any;
use std::fmt::Debug;

use log::{info, LevelFilter};
use env_logger;

use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin,
    Result as GeyserResult,
    ReplicaAccountInfoVersions,
    SlotStatus,
};

#[derive(Debug)]
pub struct LoggerPlugin;

impl LoggerPlugin {
    pub fn new() -> Self {
        let _ = env_logger::builder()
            .format_timestamp_secs()
            .try_init();
        LoggerPlugin
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
        eprintln!(
            "LoggerPlugin loaded. config_file={config_file}, is reload={is_reload}"
        );
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
            eprintln!(
                "[GEYSER-WALLET-ACCOUNT] v0.0.1: slot={slot}, pubkey={:?}, lamports={}",
                info.pubkey,
                info.lamports,
            );
        }
        ReplicaAccountInfoVersions::V0_0_2(info) => {
            eprintln!(
                "[GEYSER-WALLET-ACCOUNT] v0.0.2: slot={slot}, pubkey={:?}, lamports={}, write_version={}, has_sig={}",
                info.pubkey,
                info.lamports,
                info.write_version,
                info.txn_signature.is_some(),
            );
        }
        ReplicaAccountInfoVersions::V0_0_3(info) => {
        // Convert raw 32-byte pubkey to base58
        let pubkey_str = bs58::encode(info.pubkey).into_string();

        eprintln!(
            "[GEYSER-WALLET-ACCOUNT] v0.0.3: slot={slot}, pubkey={pubkey_str}, lamports={}, write_version={}",
            info.lamports,
            info.write_version,
        );
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
