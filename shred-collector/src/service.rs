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
        shred_fetch_stage::ShredFetchStage,
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

    solana_streamer::streamer::{self, PacketBatchReceiver, StreamerReceiveStats},
    solana_perf::packet::{PacketBatch, PacketBatchRecycler, PacketFlags, PACKETS_PER_BATCH},

};

pub struct ShredCollectorService {
    thread_hdl: JoinHandle<()>,
}
const PACKET_COALESCE_DURATION: Duration = Duration::from_millis(1);

impl ShredCollectorService {
    pub fn new(
        node: Arc<Node>,
        blockstore: Arc<Blockstore>,
        repair_socket: Arc<UdpSocket>,
        fetch_sockets:Vec<UdpSocket>,
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
                
                let bank_forks: Arc<RwLock<BankForks>> = BankForks::new_rw_arc(bank);
                
                // Create repair whitelist
                let repair_whitelist = Arc::new(RwLock::new(HashSet::default()));

                // Create channels for QUIC endpoint
                // let (quic_endpoint_sender, _quic_endpoint_receiver) = tokio_channel(128);
                let (quic_endpoint_sender, quic_endpoint_receiver) = tokio::sync::mpsc::channel(1);
                let (quic_endpoint_response_sender, quic_endpoint_response_receiver) = crossbeam_unbounded();

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
                let epoch_schedule = bank_forks
                .read()
                .unwrap()
                .working_bank()
                .epoch_schedule()
                .clone();
                // Create RepairInfo

           

                
                let repair_info = RepairInfo {
                    bank_forks: bank_forks.clone(),
                    cluster_info: cluster_info.clone(),
                    cluster_slots: cluster_slots.clone(),
                    epoch_schedule,
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
                // let fetch_sockets: Vec<UdpSocket> = node.sockets.tvu;
                // let fetch_sockets: Vec<UdpSocket> = node.sockets.tvu.try_clone().unwrap();

                let fetch_sockets: Vec<Arc<UdpSocket>> = fetch_sockets.into_iter().map(Arc::new).collect();
        
                // let repair_socket = Arc::new(repair_socket);
                // let (fetch_sender, fetch_receiver) = crossbeam_unbounded();
                // let (turbine_quic_endpoint_sender, turbine_quic_endpoint_receiver) = crossbeam_unbounded();
                
                let turbine_disabled = Arc::new(AtomicBool::new(false));

                let fetch_sockets_clone = fetch_sockets.clone();
                // let fetch_stage = ShredFetchStage::new(
                //     fetch_sockets_clone,
                //     turbine_quic_endpoint_receiver,
                //     repair_socket.clone(),
                //     quic_endpoint_response_receiver,
                //     fetch_sender,
                //     50093,
                //     bank_forks.clone(),
                //     cluster_info.clone(),
                //     turbine_disabled,
                //     exit.clone(),
                // );

                
                // Spawn a thread to run the streamers
                let _streamer_thread = std::thread::spawn(move || {

                    let recycler: solana_perf::recycler::Recycler<solana_perf::cuda_runtime::PinnedVec<solana_sdk::packet::Packet>> = PacketBatchRecycler::warmed(100, 1024);

                    let (packet_sender, packet_receiver) = crossbeam_unbounded();
                    let sockets = vec![repair_socket.clone()];
                    let streamers: Vec<JoinHandle<()>> = sockets
                        .iter()
                        .enumerate()
                        .map(|(i, socket)| {
                            streamer::receiver(
                                format!("solRcvrShred{i:02}"),
                                socket.clone(),
                                exit.clone(),
                                packet_sender.clone(),
                                recycler.clone(),
                                Arc::new(StreamerReceiveStats::new("packet_modifier")),
                                PACKET_COALESCE_DURATION,
                                true, // use_pinned_memory
                                None, // in_vote_only_mode
                                false,
                            )
                        })
                        .collect();
                
                    debug!("streamers: {} fetch_sockets{}", streamers.len(), fetch_sockets.len());
                    

                    // Wait for all streamers to complete
                    for streamer in streamers {
                        let _ = streamer.join();
                    }


                    let modifier_hdl = Builder::new()
                    .name("modifier_thread_name".to_string())
                    .spawn(move || {
                        // let repair_context = repair_context
                        //     .as_ref()
                        //     .map(|(socket, cluster_info)| (socket.as_ref(), cluster_info.as_ref()));
                            loop{
                                for packet_batch in packet_receiver.clone() {
                                    debug!("packet_batch: {} ",packet_batch.len());
                                }
                            }
                       
                    })
                    .unwrap();

                    modifier_hdl.join().unwrap();
                
                });

                info!("Shred collection started");

            })
            .unwrap();

        Self { thread_hdl }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
