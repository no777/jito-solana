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
    },
    solana_ledger::shred::{should_discard_shred, ShredFetchStats},

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
        repair::repair_service::{RepairService, RepairInfo, OutstandingShredRepairs},
        repair::serve_repair::{
             RepairProtocol, RepairRequestHeader, ServeRepair, ShredRepairType,
            
        },
        
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
    thread_hdls: Vec<JoinHandle<()>>,
    repair_service: RepairService,


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
impl ShredCollectorService {
    // updates packets received on a channel and sends them on another channel
    fn modify_packets(
        recvr: PacketBatchReceiver,
        sendr: Sender<PacketBatch>,
        bank_forks: &RwLock<BankForks>,
        shred_version: u16,
        name: &'static str,
        flags: PacketFlags,
        repair_context: Option<(&UdpSocket, &ClusterInfo)>,
        turbine_disabled: Arc<AtomicBool>,
    ) {
        const STATS_SUBMIT_CADENCE: Duration = Duration::from_secs(1);
        let mut last_updated = Instant::now();
        let mut keypair = repair_context
            .as_ref()
            .map(|(_, cluster_info)| cluster_info.keypair().clone());

        let (
            mut last_root,
            mut slots_per_epoch,
            mut feature_set,
            mut epoch_schedule,
            mut last_slot,
        ) = {
            let bank_forks_r = bank_forks.read().unwrap();
            let root_bank = bank_forks_r.root_bank();
            (
                root_bank.slot(),
                root_bank.get_slots_in_epoch(root_bank.epoch()),
                root_bank.feature_set.clone(),
                root_bank.epoch_schedule().clone(),
                bank_forks_r.highest_slot(),
            )
        };
        
        let mut stats = ShredFetchStats::default();
        let cluster_type = {
            let root_bank = bank_forks.read().unwrap().root_bank();
            root_bank.cluster_type()
        };
        debug!("last_root: {} last_slot: {} cluster_type: {:#?} ",last_root, last_slot, cluster_type);
        for mut packet_batch in recvr {
            debug!("packet_batch: {} ",packet_batch.len());
       
            if last_updated.elapsed().as_millis() as u64 > DEFAULT_MS_PER_SLOT {
                last_updated = Instant::now();
                let root_bank = {
                    let bank_forks_r = bank_forks.read().unwrap();
                    last_slot = bank_forks_r.highest_slot();
                    bank_forks_r.root_bank()
                };
                feature_set = root_bank.feature_set.clone();
                epoch_schedule = root_bank.epoch_schedule().clone();
                last_root = root_bank.slot();
                slots_per_epoch = root_bank.get_slots_in_epoch(root_bank.epoch());
                keypair = repair_context
                    .as_ref()
                    .map(|(_, cluster_info)| cluster_info.keypair().clone());
            }
            stats.shred_count += packet_batch.len();

            if let Some((udp_socket, _)) = repair_context {
                debug_assert_eq!(flags, PacketFlags::REPAIR);
                debug_assert!(keypair.is_some());
                if let Some(ref keypair) = keypair {
                    ServeRepair::handle_repair_response_pings(
                        udp_socket,
                        keypair,
                        &mut packet_batch,
                        &mut stats,
                    );
                }
            }

            // Limit shreds to 2 epochs away.
            let max_slot = last_slot + 2 * slots_per_epoch;
            let enable_chained_merkle_shreds = |shred_slot| {
                cluster_type == ClusterType::Development
                    || check_feature_activation(
                        &feature_set::enable_chained_merkle_shreds::id(),
                        shred_slot,
                        &feature_set,
                        &epoch_schedule,
                    )
            };
            let turbine_disabled = turbine_disabled.load(Ordering::Relaxed);
            for packet in packet_batch.iter_mut().filter(|p| !p.meta().discard()) {
                if turbine_disabled
                    || should_discard_shred(
                        packet,
                        last_root,
                        max_slot,
                        shred_version,
                        enable_chained_merkle_shreds,
                        &mut stats,
                    )
                {
                    packet.meta_mut().set_discard(true);
                } else {
                    packet.meta_mut().flags.insert(flags);
                }
            }
            stats.maybe_submit(name, STATS_SUBMIT_CADENCE);
            if let Err(err) = sendr.send(packet_batch) {
                error!("sendr.send(packet_batch) failed {}", err);
            }
           
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn packet_modifier(
        receiver_thread_name: &'static str,
        modifier_thread_name: &'static str,
        sockets: Vec<Arc<UdpSocket>>,
        exit: Arc<AtomicBool>,
        sender: Sender<PacketBatch>,
        recycler: PacketBatchRecycler,
        bank_forks: Arc<RwLock<BankForks>>,
        shred_version: u16,
        name: &'static str,
        flags: PacketFlags,
        repair_context: Option<(Arc<UdpSocket>, Arc<ClusterInfo>)>,
        turbine_disabled: Arc<AtomicBool>,
    ) -> (Vec<JoinHandle<()>>, JoinHandle<()>) {
        let (packet_sender, packet_receiver) = unbounded();
        let socket_addrs: Vec<_> = sockets.iter().map(|socket| socket.local_addr().unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)))).collect();
        let streamers: Vec<JoinHandle<()>> = sockets
            .into_iter()
            .enumerate()
            .map(|(i, socket)| {
                streamer::receiver(
                    format!("{receiver_thread_name}{i:02}"),
                    socket,
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
    
        debug!("streamers: {} sockets: {:?}", streamers.len(), socket_addrs);
        let modifier_hdl = Builder::new()
            .name(modifier_thread_name.to_string())
            .spawn(move || {
                let repair_context = repair_context
                    .as_ref()
                    .map(|(socket, cluster_info)| (socket.as_ref(), cluster_info.as_ref()));

                Self::modify_packets(
                    packet_receiver,
                    sender,
                    &bank_forks,
                    shred_version,
                    name,
                    flags,
                    repair_context,
                    turbine_disabled,
                )
            })
            .unwrap();
        (streamers, modifier_hdl)
    }


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

                let sender = fetch_sender;
                let (mut tvu_threads, tvu_filter) = Self::packet_modifier(
                    "solRcvrShred",
                    "solTvuPktMod",
                    sockets,
                    exit.clone(),
                    sender.clone(),
                    recycler.clone(),
                    bank_forks.clone(),
                    shred_version,
                    "shred_fetch",
                    PacketFlags::empty(),
                    None, // repair_context
                    turbine_disabled.clone(),
                );

          // Turbine shreds fetched over QUIC protocol.

        
        
                let (repair_receiver, repair_handler) = Self::packet_modifier(
                    "solRcvrShredRep",
                    "solTvuRepPktMod",
                    vec![repair_socket.clone()],
                    exit.clone(),
                    sender.clone(),
                    recycler.clone(),
                    bank_forks.clone(),
                    shred_version,
                    "shred_fetch_repair",
                    PacketFlags::REPAIR,
                    Some((repair_socket, cluster_info)),
                    turbine_disabled.clone(),
                );
        

                // let (streamers, modifier_hdl) = Self::packet_modifier(
                //     "solRcvrShred",
                //     "solTvuPktMod",
                //     sockets,
                //     exit.clone(),
                //     fetch_sender.clone(),
                //     recycler.clone(),
                //     bank_forks.clone(),
                //     50093,
                //     "shred_fetch",
                //     PacketFlags::empty(),
                //     None, // repair_context
                //     turbine_disabled.clone(),
                // );
                

                tvu_threads.extend(repair_receiver);
                tvu_threads.push(tvu_filter);
                tvu_threads.push(repair_handler);
                // Repair shreds fetched over QUIC protocol.
                {
                    let (packet_sender, packet_receiver) = unbounded();
                    let bank_forks = bank_forks.clone();
                    let recycler = recycler.clone();
                    let exit = exit.clone();
                    let sender = sender.clone();
                    let turbine_disabled = turbine_disabled.clone();
                    tvu_threads.extend([
                        Builder::new()
                            .name("solTvuRecvRpr".to_string())
                            .spawn(|| {
                                receive_repair_quic_packets(
                                    repair_quic_endpoint_response_receiver,
                                    packet_sender,
                                    recycler,
                                    exit,
                                )
                            })
                            .unwrap(),
                        Builder::new()
                            .name("solTvuFetchRpr".to_string())
                            .spawn(move || {
                                Self::modify_packets(
                                    packet_receiver,
                                    sender,
                                    &bank_forks,
                                    shred_version,
                                    "shred_fetch_repair_quic",
                                    PacketFlags::REPAIR,
                                    None, // repair_context; no ping packets!
                                    turbine_disabled,
                                )
                            })
                            .unwrap(),
                    ]);
                }


                let (_turbine_quic_endpoint_sender, turbine_quic_endpoint_receiver) = unbounded();

                let (packet_sender, packet_receiver) = unbounded();
                tvu_threads.extend([
                    Builder::new()
                        .name("solTvuRecvQuic".to_string())
                        .spawn(|| {
                            receive_quic_datagrams(
                                turbine_quic_endpoint_receiver,
                                packet_sender,
                                recycler,
                                exit,
                            )
                        })
                        .unwrap(),
                    Builder::new()
                        .name("solTvuFetchQuic".to_string())
                        .spawn(move || {
                            Self::modify_packets(
                                packet_receiver,
                                sender,
                                &bank_forks,
                                shred_version,
                                "shred_fetch_quic",
                                PacketFlags::empty(),
                                None, // repair_context
                                turbine_disabled,
                            )
                        })
                        .unwrap(),
                ]);
                       


                //  // Wait for all streamers to complete
                //  for streamer in repair_receiver {
                //     let _ = streamer.join();
                // }



                // repair_handler.join().unwrap();

                info!("Shred collection started");


                Self {
                    thread_hdls: tvu_threads,
                    repair_service,
                }
    }

    pub(crate) fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls {
            thread_hdl.join()?;
        }
        self.repair_service.join()?;
        Ok(())
    }


}   

fn receive_quic_datagrams(
    turbine_quic_endpoint_receiver: Receiver<(Pubkey, SocketAddr, Bytes)>,
    sender: Sender<PacketBatch>,
    recycler: PacketBatchRecycler,
    exit: Arc<AtomicBool>,
) {
    const RECV_TIMEOUT: Duration = Duration::from_secs(1);
    while !exit.load(Ordering::Relaxed) {
        let entry = match turbine_quic_endpoint_receiver.recv_timeout(RECV_TIMEOUT) {
            Ok(entry) => entry,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => return,
        };
        let mut packet_batch =
            PacketBatch::new_with_recycler(&recycler, PACKETS_PER_BATCH, "receive_quic_datagrams");
        unsafe {
            packet_batch.set_len(PACKETS_PER_BATCH);
        };
        let deadline = Instant::now() + PACKET_COALESCE_DURATION;
        let entries = std::iter::once(entry).chain(
            std::iter::repeat_with(|| turbine_quic_endpoint_receiver.recv_deadline(deadline).ok())
                .while_some(),
        );
        let size = entries
            .filter(|(_, _, bytes)| bytes.len() <= PACKET_DATA_SIZE)
            .zip(packet_batch.iter_mut())
            .map(|((_pubkey, addr, bytes), packet)| {
                *packet.meta_mut() = Meta {
                    size: bytes.len(),
                    addr: addr.ip(),
                    port: addr.port(),
                    flags: PacketFlags::empty(),
                };
                packet.buffer_mut()[..bytes.len()].copy_from_slice(&bytes);
            })
            .count();
        if size > 0 {
            packet_batch.truncate(size);
            if sender.send(packet_batch).is_err() {
                return;
            }
        }
    }
}



pub(crate) fn receive_repair_quic_packets(
    repair_quic_endpoint_receiver: Receiver<(SocketAddr, Vec<u8>)>,
    sender: Sender<PacketBatch>,
    recycler: PacketBatchRecycler,
    exit: Arc<AtomicBool>,
) {
    const RECV_TIMEOUT: Duration = Duration::from_secs(1);
    while !exit.load(Ordering::Relaxed) {
        let entry = match repair_quic_endpoint_receiver.recv_timeout(RECV_TIMEOUT) {
            Ok(entry) => entry,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => return,
        };
        let mut packet_batch =
            PacketBatch::new_with_recycler(&recycler, PACKETS_PER_BATCH, "receive_quic_datagrams");
        unsafe {
            packet_batch.set_len(PACKETS_PER_BATCH);
        };
        let deadline = Instant::now() + PACKET_COALESCE_DURATION;
        let entries = std::iter::once(entry).chain(
            std::iter::repeat_with(|| repair_quic_endpoint_receiver.recv_deadline(deadline).ok())
                .while_some(),
        );
        let size = entries
            .filter(|(_, bytes)| bytes.len() <= PACKET_DATA_SIZE)
            .zip(packet_batch.iter_mut())
            .map(|((addr, bytes), packet)| {
                *packet.meta_mut() = Meta {
                    size: bytes.len(),
                    addr: addr.ip(),
                    port: addr.port(),
                    flags: PacketFlags::REPAIR,
                };
                packet.buffer_mut()[..bytes.len()].copy_from_slice(&bytes);
            })
            .count();
        if size > 0 {
            packet_batch.truncate(size);
            if sender.send(packet_batch).is_err() {
                return; // The receiver end of the channel is disconnected.
            }
        }
    }
}

// Returns true if the feature is effective for the shred slot.
#[must_use]
fn check_feature_activation(
    feature: &Pubkey,
    shred_slot: Slot,
    feature_set: &FeatureSet,
    epoch_schedule: &EpochSchedule,
) -> bool {
    match feature_set.activated_slot(feature) {
        None => false,
        Some(feature_slot) => {
            let feature_epoch = epoch_schedule.get_epoch(feature_slot);
            let shred_epoch = epoch_schedule.get_epoch(shred_slot);
            feature_epoch < shred_epoch
        }
    }
}
