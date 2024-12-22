use {
    log::*,
    solana_gossip::{
        cluster_info::{ClusterInfo, Node},
    },
    crossbeam_channel::{unbounded, Receiver, RecvTimeoutError, Sender},
    bytes::Bytes,
    itertools::Itertools,

    solana_ledger::{
        blockstore::Blockstore,
        genesis_utils::create_genesis_config,
        leader_schedule_cache::LeaderScheduleCache,
    },

    solana_ledger::shred::{should_discard_shred, ShredFetchStats},


    solana_accounts_db::{
        accounts_index::AccountSecondaryIndexes,
        accounts_db::AccountShrinkThreshold,
    },
    solana_core::{
        repair::repair_service::{RepairService, RepairInfo, OutstandingShredRepairs},
        repair::serve_repair::{
             RepairProtocol, RepairRequestHeader, ServeRepair, ShredRepairType,
            
        },
        shred_fetch_stage::ShredFetchStage,
        
        cluster_slots_service::cluster_slots::ClusterSlots,
    },
    solana_sdk::{
        clock::{Slot, DEFAULT_MS_PER_SLOT},
        native_token::LAMPORTS_PER_SOL,
        epoch_schedule::EpochSchedule,
        feature_set::{self, FeatureSet},
        genesis_config::ClusterType,
        packet::{Meta, PACKET_DATA_SIZE},
        pubkey::Pubkey,
    },
    solana_turbine::retransmit_stage::RetransmitStage,
    solana_runtime::{
        bank::Bank,
        bank_forks::BankForks,
        genesis_utils::GenesisConfigInfo,
        runtime_config::RuntimeConfig,
    },

    std::{
        collections::HashSet,
        net::{SocketAddr, UdpSocket},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
    crossbeam_channel::unbounded as crossbeam_unbounded,

    solana_streamer::streamer::{self, PacketBatchReceiver, StreamerReceiveStats},
    solana_perf::packet::{PacketBatch, PacketBatchRecycler, PacketFlags, PACKETS_PER_BATCH},

};

pub struct ShredCollectorService {
    fetch_stage: ShredFetchStage,
    repair_service: RepairService,
    shred_sigverify: JoinHandle<()>,

}
const PACKET_COALESCE_DURATION: Duration = Duration::from_millis(1);
pub struct ShredCollectorServiceConfig {
    pub max_ledger_shreds: Option<u64>,
    pub shred_version: u16,
    // Validators from which repairs are requested
    pub repair_validators: Option<HashSet<Pubkey>>,
    // Validators which should be given priority when serving repairs
    pub repair_whitelist: Arc<RwLock<HashSet<Pubkey>>>,
    pub wait_for_vote_to_start_leader: bool,
    // pub replay_forks_threads: NonZeroUsize,
    // pub replay_transactions_threads: NonZeroUsize,
}

impl Default for ShredCollectorServiceConfig {
    fn default() -> Self {
        Self {
            max_ledger_shreds: None,
            shred_version: 0,
            repair_validators: None,
            repair_whitelist: Arc::new(RwLock::new(HashSet::default())),
            wait_for_vote_to_start_leader: false,
            // replay_forks_threads: NonZeroUsize::new(1).expect("1 is non-zero"),
            // replay_transactions_threads: NonZeroUsize::new(1).expect("1 is non-zero"),
        }
    }
}

pub fn spawn_shred_sigverify(
    cluster_info: Arc<ClusterInfo>,
    bank_forks: Arc<RwLock<BankForks>>,
    leader_schedule_cache: Arc<LeaderScheduleCache>,
    shred_fetch_receiver: Receiver<PacketBatch>,
    retransmit_sender: Sender<Vec</*shred:*/ Vec<u8>>>,
    verified_sender: Sender<Vec<PacketBatch>>,
) -> JoinHandle<()> {
    let test_loop = move || {
        for mut packet_batch in shred_fetch_receiver {
            debug!("spawn_shred_sigverify packet_batch: {} ",packet_batch.len());
        }
    };
    Builder::new()
        .name("solShredVerifr".to_string())
        .spawn(test_loop)
        .unwrap()


    // let recycler_cache = RecyclerCache::warmed();
    // let mut stats = ShredSigVerifyStats::new(Instant::now());
    // let cache = RwLock::new(LruCache::new(SIGVERIFY_LRU_CACHE_CAPACITY));
    // let cluster_nodes_cache = ClusterNodesCache::<RetransmitStage>::new(
    //     CLUSTER_NODES_CACHE_NUM_EPOCH_CAP,
    //     CLUSTER_NODES_CACHE_TTL,
    // );
    // let thread_pool = ThreadPoolBuilder::new()
    //     .num_threads(get_thread_count())
    //     .thread_name(|i| format!("solSvrfyShred{i:02}"))
    //     .build()
    //     .unwrap();
    // let run_shred_sigverify = move || {
    //     let mut rng = rand::thread_rng();
    //     let mut deduper = Deduper::<2, [u8]>::new(&mut rng, DEDUPER_NUM_BITS);
    //     loop {
    //         if deduper.maybe_reset(&mut rng, DEDUPER_FALSE_POSITIVE_RATE, DEDUPER_RESET_CYCLE) {
    //             stats.num_deduper_saturations += 1;
    //         }
    //         // We can't store the keypair outside the loop
    //         // because the identity might be hot swapped.
    //         let keypair: Arc<Keypair> = cluster_info.keypair().clone();
    //         match run_shred_sigverify(
    //             &thread_pool,
    //             &keypair,
    //             &cluster_info,
    //             &bank_forks,
    //             &leader_schedule_cache,
    //             &recycler_cache,
    //             &deduper,
    //             &shred_fetch_receiver,
    //             &retransmit_sender,
    //             &verified_sender,
    //             &cluster_nodes_cache,
    //             &cache,
    //             &mut stats,
    //         ) {
    //             Ok(()) => (),
    //             Err(Error::RecvTimeout) => (),
    //             Err(Error::RecvDisconnected) => break,
    //             Err(Error::SendError) => break,
    //         }
    //         stats.maybe_submit();
    //     }
    // };
    // Builder::new()
    //     .name("solShredVerifr".to_string())
    //     .spawn(run_shred_sigverify)
    //     .unwrap()
}

impl ShredCollectorService {
    
    pub fn new(
        config:ShredCollectorServiceConfig,
        node: Arc<Node>,
        blockstore: Arc<Blockstore>,
        repair_socket: Arc<UdpSocket>,
        fetch_sockets:Vec<UdpSocket>,
        cluster_info: Arc<ClusterInfo>,
        start_slot: Slot,
        exit: Arc<AtomicBool>,
    ) -> Self {
        

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

  // Create channels for QUIC endpoint
                // let (quic_endpoint_sender, _quic_endpoint_receiver) = tokio_channel(128);
                let (quic_endpoint_sender, quic_endpoint_receiver) = tokio::sync::mpsc::channel(1);
                // let (quic_endpoint_response_sender, quic_endpoint_response_receiver) = crossbeam_unbounded();


                let (repair_quic_endpoint_response_sender, repair_quic_endpoint_response_receiver) =
                unbounded();
                // Create RepairService
                let repair_service = RepairService::new(
                    blockstore.clone(),
                    exit.clone(),
                    repair_socket.clone(),
                    ancestor_hashes_socket,
                    quic_endpoint_sender,
                    repair_quic_endpoint_response_sender,
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

                let sockets = vec![repair_socket.clone()];
                // Spawn a thread to run the streamers
                // let _streamer_thread = std::thread::spawn(move || 
                    
                // const shred_version: u16 = 50093;

                let shred_version = config.shred_version;

                let recycler: solana_perf::recycler::Recycler<solana_perf::cuda_runtime::PinnedVec<solana_sdk::packet::Packet>> = PacketBatchRecycler::warmed(100, 1024);
                let (fetch_sender, fetch_receiver) = unbounded();
                
                let bank = bank_forks.read().unwrap().working_bank();

                let leader_schedule_cache: Arc<LeaderScheduleCache> = Arc::new(LeaderScheduleCache::new_from_bank(&bank));

                let (verified_sender, verified_receiver) = unbounded();
                let (retransmit_sender, retransmit_receiver) = unbounded();

                
                // let shred_sigverify = spawn_shred_sigverify(
                //     cluster_info.clone(),
                //     bank_forks.clone(),
                //     leader_schedule_cache.clone(),
                //     fetch_receiver,
                //     retransmit_sender.clone(),
                //     verified_sender,
                // );
                let shred_sigverify = solana_turbine::sigverify_shreds::spawn_shred_sigverify(
                    cluster_info.clone(),
                    bank_forks.clone(),
                    leader_schedule_cache.clone(),
                    fetch_receiver,
                    retransmit_sender.clone(),
                    verified_sender,
                );

                let (_turbine_quic_endpoint_sender, turbine_quic_endpoint_receiver) = unbounded();

                let fetch_stage = ShredFetchStage::new(
                    fetch_sockets,
                    turbine_quic_endpoint_receiver,
                    repair_socket.clone(),
                    repair_quic_endpoint_response_receiver,
                    fetch_sender,
                    shred_version,
                    bank_forks.clone(),
                    cluster_info.clone(),
                    turbine_disabled,
                    exit.clone(),
                );


        //         let sender = fetch_sender;
        //         let (mut tvu_threads, tvu_filter) = Self::packet_modifier(
        //             "solRcvrShred",
        //             "solTvuPktMod",
        //             sockets,
        //             exit.clone(),
        //             sender.clone(),
        //             recycler.clone(),
        //             bank_forks.clone(),
        //             shred_version,
        //             "shred_fetch",
        //             PacketFlags::empty(),
        //             None, // repair_context
        //             turbine_disabled.clone(),
        //         );

        //   // Turbine shreds fetched over QUIC protocol.

        
        
        //         let (repair_receiver, repair_handler) = Self::packet_modifier(
        //             "solRcvrShredRep",
        //             "solTvuRepPktMod",
        //             vec![repair_socket.clone()],
        //             exit.clone(),
        //             sender.clone(),
        //             recycler.clone(),
        //             bank_forks.clone(),
        //             shred_version,
        //             "shred_fetch_repair",
        //             PacketFlags::REPAIR,
        //             Some((repair_socket, cluster_info)),
        //             turbine_disabled.clone(),
        //         );
        

        //         // let (streamers, modifier_hdl) = Self::packet_modifier(
        //         //     "solRcvrShred",
        //         //     "solTvuPktMod",
        //         //     sockets,
        //         //     exit.clone(),
        //         //     fetch_sender.clone(),
        //         //     recycler.clone(),
        //         //     bank_forks.clone(),
        //         //     50093,
        //         //     "shred_fetch",
        //         //     PacketFlags::empty(),
        //         //     None, // repair_context
        //         //     turbine_disabled.clone(),
        //         // );
                

        //         tvu_threads.extend(repair_receiver);
        //         tvu_threads.push(tvu_filter);
        //         tvu_threads.push(repair_handler);
        //         // Repair shreds fetched over QUIC protocol.
        //         {
        //             let (packet_sender, packet_receiver) = unbounded();
        //             let bank_forks = bank_forks.clone();
        //             let recycler = recycler.clone();
        //             let exit = exit.clone();
        //             let sender = sender.clone();
        //             let turbine_disabled = turbine_disabled.clone();
        //             tvu_threads.extend([
        //                 Builder::new()
        //                     .name("solTvuRecvRpr".to_string())
        //                     .spawn(|| {
        //                         receive_repair_quic_packets(
        //                             repair_quic_endpoint_response_receiver,
        //                             packet_sender,
        //                             recycler,
        //                             exit,
        //                         )
        //                     })
        //                     .unwrap(),
        //                 Builder::new()
        //                     .name("solTvuFetchRpr".to_string())
        //                     .spawn(move || {
        //                         Self::modify_packets(
        //                             packet_receiver,
        //                             sender,
        //                             &bank_forks,
        //                             shred_version,
        //                             "shred_fetch_repair_quic",
        //                             PacketFlags::REPAIR,
        //                             None, // repair_context; no ping packets!
        //                             turbine_disabled,
        //                         )
        //                     })
        //                     .unwrap(),
        //             ]);
        //         }


               
        //         let (packet_sender, packet_receiver) = unbounded();
        //         tvu_threads.extend([
        //             Builder::new()
        //                 .name("solTvuRecvQuic".to_string())
        //                 .spawn(|| {
        //                     receive_quic_datagrams(
        //                         turbine_quic_endpoint_receiver,
        //                         packet_sender,
        //                         recycler,
        //                         exit,
        //                     )
        //                 })
        //                 .unwrap(),
        //             Builder::new()
        //                 .name("solTvuFetchQuic".to_string())
        //                 .spawn(move || {
        //                     Self::modify_packets(
        //                         packet_receiver,
        //                         sender,
        //                         &bank_forks,
        //                         shred_version,
        //                         "shred_fetch_quic",
        //                         PacketFlags::empty(),
        //                         None, // repair_context
        //                         turbine_disabled,
        //                     )
        //                 })
        //                 .unwrap(),
        //         ]);
                       


                //  // Wait for all streamers to complete
                //  for streamer in repair_receiver {
                //     let _ = streamer.join();
                // }



                // repair_handler.join().unwrap();

                info!("Shred collection started");


                Self {
                    fetch_stage,
                    repair_service,
                    shred_sigverify,
                }
    }

    pub(crate) fn join(self) -> thread::Result<()> {
        self.fetch_stage.join()?;
        self.repair_service.join()?;

        self.shred_sigverify.join()?;
        Ok(())
    }


}   
