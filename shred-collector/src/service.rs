use {
    log::*,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{
        blockstore::Blockstore,
        shred::Shred,
        genesis_utils::create_genesis_config,
    },
    solana_sdk::{
        clock::{Clock, Slot},
        signature::Signer,
        account::{Account, AccountSharedData},
        native_token::LAMPORTS_PER_SOL,
        system_program,
        sysvar::epoch_schedule::EpochSchedule,
        vote::{
            program as vote_program,
            state::{VoteInit, VoteState},
        },
        stake::{
            program as stake_program,
            state::{Meta, StakeStateV2, Authorized, Lockup},
        },
    },
    solana_runtime::{
        bank::Bank,
        bank_forks::BankForks,
        genesis_utils::GenesisConfigInfo,

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
        sync::{atomic::{AtomicBool, AtomicU64, Ordering}, Arc, RwLock},
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

                debug!("ShredCollectorService::new");

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

                debug!("accounts.insert vote account");

                // Create and add stake account
                let stake_pubkey = validator_pubkey;
                let meta = Meta {
                    rent_exempt_reserve: 0,
                    authorized: Authorized {
                        staker: validator_pubkey,
                        withdrawer: validator_pubkey,
                    },
                    lockup: Lockup::default(),
                };

                let stake_state = StakeStateV2::Initialized(meta);

                let stake_account = Account::from(AccountSharedData::new_data(
                    validator_stake * 2, // Double the stake amount
                    &stake_state,
                    &stake_program::id(),
                ).unwrap());

                accounts.insert(stake_pubkey, stake_account);

                debug!("accounts.insert stake account");
                /*

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
                debug!("Bank::new_with_paths");
                let bank: Bank = Bank::new_with_paths(
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

                debug!("Bank::new_with_paths ok");
                
                let bank_forks = BankForks::new_rw_arc(bank);

                */

                 // Create genesis config and bank
        let validator_lamports = 42 * LAMPORTS_PER_SOL;

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair: _,
            voting_keypair: _,
            validator_pubkey: _,
        } = create_genesis_config(validator_lamports);

        let bank = Bank::new_with_paths(
            &genesis_config,
            Arc::new(solana_runtime::runtime_config::RuntimeConfig::default()),
            Vec::new(),
            None,
            None,
            AccountSecondaryIndexes::default(),
            AccountShrinkThreshold::default(),
            false,
            None,
            None,
            None,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
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

                // Create counters for shred statistics
                let total_shreds = Arc::new(AtomicU64::new(0));
                let data_shreds = Arc::new(AtomicU64::new(0));
                let coding_shreds = Arc::new(AtomicU64::new(0));
                let total_shreds_for_stats = total_shreds.clone();
                let data_shreds_for_stats = data_shreds.clone();
                let coding_shreds_for_stats = coding_shreds.clone();

                // Start stats reporting thread
                let exit_clone = exit.clone();
                let blockstore_for_stats = blockstore.clone();
                let _stats_thread = Builder::new()
                    .name("repairStats".to_string())
                    .spawn(move || {
                        while !exit_clone.load(Ordering::Relaxed) {
                            // Report shred statistics
                            let total = total_shreds_for_stats.load(Ordering::Relaxed);
                            let data = data_shreds_for_stats.load(Ordering::Relaxed);
                            let coding = coding_shreds_for_stats.load(Ordering::Relaxed);
                            info!(
                                "Shred statistics - Total: {}, Data: {}, Coding: {}",
                                total, data, coding
                            );

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
                            let is_data = shred.is_data();

                            // Update counters
                            total_shreds.fetch_add(1, Ordering::Relaxed);
                            if is_data {
                                data_shreds.fetch_add(1, Ordering::Relaxed);
                            } else {
                                coding_shreds.fetch_add(1, Ordering::Relaxed);
                            }

                            info!(
                                "Received shred from {}: slot {} index {} type {}",
                                addr,
                                slot,
                                shred.index(),
                                if is_data { "data" } else { "coding" }
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
