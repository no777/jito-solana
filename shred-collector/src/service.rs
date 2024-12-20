use {
    log::*,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{
        blockstore::Blockstore,
        shred::Shred,
    },
    solana_sdk::{
        clock::{Clock, Slot},
        signature::Signer,
        sysvar::rent::Rent,
        fee_calculator::FeeRateGovernor,
        genesis_config::GenesisConfig,
        account::{Account, AccountSharedData},
        native_token::LAMPORTS_PER_SOL,
        system_program,
        sysvar::epoch_schedule::EpochSchedule,
        vote::{
            program as vote_program,
            state::{VoteInit, VoteState},
        },
    },
    solana_runtime::{
        bank::Bank,
        bank_forks::BankForks,
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
        collections::{BTreeMap, HashSet},
        net::UdpSocket,
        sync::{atomic::{AtomicBool, Ordering}, Arc, RwLock},
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
    tokio::sync::mpsc::channel as tokio_channel,
    crossbeam_channel::unbounded as crossbeam_unbounded,
};

pub struct ShredCollectorService {
    thread_hdl: JoinHandle<()>,
}

impl ShredCollectorService {
    pub fn new(
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

                // Create a minimal validator genesis config
                let validator_keypair = cluster_info.keypair().clone();
                let validator_pubkey = validator_keypair.pubkey();
                let validator_stake = 42 * LAMPORTS_PER_SOL;

                let mut accounts = BTreeMap::new();
                
                // Add validator stake account
                accounts.insert(
                    validator_pubkey,
                    Account::from(AccountSharedData::new(
                        validator_stake,
                        0,
                        &system_program::id(),
                    )),
                );

                // Create and add vote account
                let vote_pubkey = validator_pubkey;
                let vote_state = VoteState::new(
                    &VoteInit {
                        node_pubkey: validator_pubkey,
                        authorized_voter: validator_pubkey,
                        authorized_withdrawer: validator_pubkey,
                        commission: 0,
                    },
                    &Clock::default(),
                );
                
                let vote_account = Account::from(AccountSharedData::new_data(
                    validator_stake,
                    &vote_state,
                    &vote_program::id(),
                ).unwrap());

                accounts.insert(vote_pubkey, vote_account);

                let genesis_config = GenesisConfig {
                    accounts,
                    fee_rate_governor: FeeRateGovernor::default(),
                    rent: Rent::default(),
                    epoch_schedule: EpochSchedule::default(),
                    ticks_per_slot: 8,
                    native_instruction_processors: vec![],
                    rewards_pools: BTreeMap::new(),
                    poh_config: Default::default(),
                    ..GenesisConfig::default()
                };

                let runtime_config = Arc::new(RuntimeConfig::default());
                let bank = Bank::new_with_paths(
                    &genesis_config,
                    runtime_config,
                    vec![],
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

                // Create RepairInfo
                let repair_info = RepairInfo {
                    bank_forks,
                    cluster_info: cluster_info.clone(),
                    cluster_slots,
                    epoch_schedule: EpochSchedule::default(),
                    repair_validators: None,
                    repair_whitelist,
                    ancestor_duplicate_slots_sender,
                    wen_restart_repair_slots: Some(Arc::new(RwLock::new(vec![]))),
                };

                // Create ancestor hashes socket
                let ancestor_hashes_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").unwrap_or_else(|e| {
                    eprintln!("Failed to bind ancestor hashes socket: {}", e);
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

                // Start stats reporting thread
                let exit_clone = exit.clone();
                let blockstore_for_stats = blockstore.clone();
                let _stats_thread = Builder::new()
                    .name("repairStats".to_string())
                    .spawn(move || {
                        while !exit_clone.load(Ordering::Relaxed) {
                            // Report processed slots info
                            if let Ok(iter) = blockstore_for_stats.slot_meta_iterator(0) {
                                let slots_processed = iter.count();
                                info!("Total slots processed: {}", slots_processed);
                            } else {
                                warn!("Failed to read slot meta iterator");
                            }

                            thread::sleep(Duration::from_secs(5));
                        }
                    })
                    .unwrap();

                loop {
                    if exit.load(Ordering::Relaxed) {
                        break;
                    }

                    // Receive shreds
                    if let Ok((size, addr)) = repair_socket.recv_from(&mut buf) {
                        if let Ok(shred) = Shred::new_from_serialized_shred(buf[..size].to_vec()) {
                            let slot = shred.slot();

                            info!(
                                "Received shred from {}: slot {} index {} type {}",
                                addr,
                                slot,
                                shred.index(),
                                if shred.is_data() { "data" } else { "coding" }
                            );

                            if let Err(err) = blockstore.insert_shreds(vec![shred], None, false) {
                                warn!("Failed to insert shred: {:?}", err);
                            }
                        }
                    }
                }
            })
            .unwrap();

        Self { thread_hdl }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
