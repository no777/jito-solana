use {
    log::*,
    solana_gossip::{
        cluster_info::{ClusterInfo, Node},
    },
    solana_ledger::{
        blockstore::Blockstore,
        shred::Shred,
        genesis_utils::create_genesis_config,
    },
    solana_sdk::{
        clock::Slot,
        native_token::LAMPORTS_PER_SOL,
        sysvar::epoch_schedule::EpochSchedule,
    },
    solana_runtime::{
        bank::Bank,
        bank_forks::BankForks,
        genesis_utils::GenesisConfigInfo,
        runtime_config::RuntimeConfig,
    },
    solana_accounts_db::{
        accounts_index::AccountSecondaryIndexes,
        accounts_db::AccountShrinkThreshold,
    },
    solana_core::{
        repair::{
            repair_service::{RepairService, RepairInfo, OutstandingShredRepairs},
        },
        cluster_slots_service::cluster_slots::ClusterSlots,
    },
    std::{
        collections::HashSet,
        net::UdpSocket,
        sync::{atomic::{AtomicBool, AtomicU64, Ordering}, Arc, RwLock},
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
    tokio::sync::mpsc::channel as tokio_channel,
    crossbeam_channel::unbounded as crossbeam_unbounded,
    rand::thread_rng,
};

pub struct ShredCollectorService {
    thread_hdl: JoinHandle<()>,
}

impl ShredCollectorService {
    pub fn new(
        node: Arc<Node>,
        blockstore: Arc<Blockstore>,
        repair_socket: Arc<UdpSocket>,
        cluster_info: Arc<ClusterInfo>,
        start_slot: Slot,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let thread_hdl = Builder::new()
            .name("shredCollector".to_string())
            .spawn(move || {
                let mut buf = [0u8; 2048];

                debug!("ShredCollectorService::new");

                // Create genesis config with validator stake
                let validator_lamports = 42 * LAMPORTS_PER_SOL;
                debug!("Creating genesis config with {} lamports", validator_lamports);

                let GenesisConfigInfo {
                    genesis_config,
                    mint_keypair: _,
                    voting_keypair: _,
                    validator_pubkey,
                } = create_genesis_config(validator_lamports);

                debug!("Genesis config created, validator pubkey: {}", validator_pubkey);

                // Create bank with the genesis config
                let bank = Bank::new_with_paths(
                    &genesis_config,
                    Arc::new(RuntimeConfig::default()),
                    Vec::new(),
                    None,
                    None,
                    AccountSecondaryIndexes::default(),
                    AccountShrinkThreshold::default(),
                    false,
                    None,
                    None,
                    None,
                    Arc::new(AtomicBool::new(false)),
                    None,
                );

                debug!("Bank created successfully");
                
                let bank_forks = BankForks::new_rw_arc(bank);
                
                // Create repair whitelist
                let repair_whitelist = Arc::new(RwLock::new(HashSet::default()));

                // Create channels for QUIC endpoint
                let (quic_endpoint_sender, _quic_endpoint_receiver) = tokio_channel(128);
                let (quic_endpoint_response_sender, _quic_endpoint_response_receiver) = crossbeam_unbounded();

                // Create verified vote receiver channel
                let (_verified_vote_sender, verified_vote_receiver) = crossbeam_unbounded();
                
                // Create ancestor hashes replay update channel
                let (_ancestor_hashes_replay_update_sender, ancestor_hashes_replay_update_receiver) = crossbeam_unbounded();

                // Create dumped slots channel
                let (_dumped_slots_sender, dumped_slots_receiver) = crossbeam_unbounded();

                // Create popular pruned forks channel
                let (popular_pruned_forks_sender, _popular_pruned_forks_receiver) = crossbeam_unbounded();

                // Create ancestor duplicate slots channel
                let (ancestor_duplicate_slots_sender, _ancestor_duplicate_slots_receiver) = crossbeam_unbounded();

                // Create outstanding requests tracker
                let outstanding_requests = Arc::new(RwLock::new(OutstandingShredRepairs::default()));

                // Create cluster slots
                let cluster_slots = Arc::new(ClusterSlots::default());

                // Initialize repair slots with start_slot
                let repair_slots = vec![start_slot];
                let wen_restart_repair_slots = Arc::new(RwLock::new(repair_slots));

                // Create RepairInfo
                let repair_info = RepairInfo {
                    bank_forks,
                    cluster_info: cluster_info.clone(),
                    cluster_slots: cluster_slots.clone(),
                    epoch_schedule: EpochSchedule::default(),
                    repair_validators: None,
                    repair_whitelist,
                    ancestor_duplicate_slots_sender,
                    wen_restart_repair_slots: Some(wen_restart_repair_slots.clone()),
                };

                // Get ancestor hashes socket from node
                let ancestor_hashes_socket = Arc::new(node.sockets.ancestor_hashes_requests.try_clone().unwrap_or_else(|e| {
                    error!("Failed to clone ancestor hashes socket: {}", e);
                    std::process::exit(1);
                }));

                // Create RepairService
                let _repair_service = RepairService::new(
                    blockstore.clone(),
                    exit.clone(),
                    repair_socket.clone(),
                    ancestor_hashes_socket,
                    quic_endpoint_sender,
                    quic_endpoint_response_sender,
                    repair_info,
                    verified_vote_receiver,
                    outstanding_requests,
                    ancestor_hashes_replay_update_receiver,
                    dumped_slots_receiver,
                    popular_pruned_forks_sender,
                );
                

                info!("Starting shred collection from slot {}", start_slot);

                debug!("cluster_info repair_peers: {:#?}", cluster_info.repair_peers(start_slot));

                // // Create counters for shred statistics
                // let total_shreds = Arc::new(AtomicU64::new(0));
                // let data_shreds = Arc::new(AtomicU64::new(0));
                // let coding_shreds = Arc::new(AtomicU64::new(0));
                // let total_shreds_for_stats = total_shreds.clone();
                // let data_shreds_for_stats = data_shreds.clone();
                // let coding_shreds_for_stats = coding_shreds.clone();

                // Start stats reporting thread
                // let exit_clone = exit.clone();
                // let blockstore_for_stats = blockstore.clone();
                // let _stats_thread = Builder::new()
                //     .name("repairStats".to_string())
                //     .spawn(move || {
                //         while !exit_clone.load(Ordering::Relaxed) {
                //             // Report shred statistics
                //             let total = total_shreds_for_stats.load(Ordering::Relaxed);
                //             let data = data_shreds_for_stats.load(Ordering::Relaxed);
                //             let coding = coding_shreds_for_stats.load(Ordering::Relaxed);
                //             info!(
                //                 "Shred statistics - Total: {}, Data: {}, Coding: {}",
                //                 total, data, coding
                //             );

                //             // Report processed slots info
                //             if let Ok(iter) = blockstore_for_stats.slot_meta_iterator(0) {
                //                 let slots_processed = iter.count();
                //                 info!("Total slots processed: {}", slots_processed);
                //             } else {
                //                 warn!("Failed to read slot meta iterator");
                //             }

                //             thread::sleep(Duration::from_secs(5));
                //         }
                //     })
                //     .unwrap();

                // loop {
                //     if exit.load(Ordering::Relaxed) {
                //         break;
                //     }

                //     // Receive shreds
                //     if let Ok((size, addr)) = repair_socket.recv_from(&mut buf) {
                //         if let Ok(shred) = Shred::new_from_serialized_shred(buf[..size].to_vec()) {
                //             let slot = shred.slot();
                //             let is_data = shred.is_data();

                //             // Update counters
                //             total_shreds.fetch_add(1, Ordering::Relaxed);
                //             if is_data {
                //                 data_shreds.fetch_add(1, Ordering::Relaxed);
                //             } else {
                //                 coding_shreds.fetch_add(1, Ordering::Relaxed);
                //             }

                //             info!(
                //                 "Received shred from {}: slot {} index {} type {}",
                //                 addr,
                //                 slot,
                //                 shred.index(),
                //                 if is_data { "data" } else { "coding" }
                //             );

                //             if let Err(err) = blockstore.insert_shreds(vec![shred], None, false) {
                //                 warn!("Failed to insert shred: {:?}", err);
                //             }
                //         }
                //     }
                // }
            })
            .unwrap();

        Self { thread_hdl }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
