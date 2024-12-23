use {
    log::*,
    solana_gossip::cluster_info::{ClusterInfo, Node},
    crossbeam_channel::{bounded, unbounded, Receiver, Sender, TryRecvError},
    rayon::{prelude::*, ThreadPool},
    solana_measure::measure::Measure,

    solana_ledger::{
        blockstore::{Blockstore, BlockstoreInsertionMetrics, PossibleDuplicateShred},
        genesis_utils::create_genesis_config,
        leader_schedule_cache::LeaderScheduleCache,
        shred::{self, Nonce, ReedSolomonCache, Shred},

    },


    solana_rayon_threadlimit::get_thread_count,


    solana_accounts_db::{
        accounts_index::AccountSecondaryIndexes,
        accounts_db::AccountShrinkThreshold,
    },
    solana_core::{
        completed_data_sets_service::CompletedDataSetsSender,
        repair::repair_service::{RepairService, RepairInfo, OutstandingShredRepairs},
        
        repair::repair_response,
        shred_fetch_stage::ShredFetchStage,
        
        cluster_slots_service::cluster_slots::ClusterSlots,
        result::Result,
        
    },
    solana_sdk::{
        clock::Slot,
        native_token::LAMPORTS_PER_SOL,
      
    },
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
    // solana_perf::packet::{PacketBatch, PacketBatchRecycler, PacketFlags, PACKETS_PER_BATCH},
    // solana_perf::packet::{Packet},
    solana_perf::packet::{Packet, PacketBatch},

};
// pub enum Error {
//     #[error(transparent)]
//     Blockstore(#[from] blockstore::BlockstoreError),
//     #[error(transparent)]
//     Gossip(#[from] GossipError),
//     #[error(transparent)]
//     Io(#[from] std::io::Error),
//     #[error("ReadyTimeout")]
//     ReadyTimeout,
//     #[error(transparent)]
//     Recv(#[from] crossbeam_channel::RecvError),
//     #[error(transparent)]
//     RecvTimeout(#[from] crossbeam_channel::RecvTimeoutError),
//     #[error("Send")]
//     Send,
//     #[error("TrySend")]
//     TrySend,
// }
// pub type Result<T> = std::result::Result<T, Error>;

type ShredPayload = Vec<u8>;

const MAX_COMPLETED_DATA_SETS_IN_CHANNEL: usize = 100_000;

struct RepairMeta {
    nonce: Nonce,
}

pub struct ShredCollectorService {
    fetch_stage: ShredFetchStage,
    repair_service: RepairService,
    shred_sigverify: JoinHandle<()>,
    insert_thread: JoinHandle<()>,

}
pub struct ShredCollectorServiceConfig {
    // pub max_ledger_shreds: Option<u64>,
    pub shred_version: u16,
    // Validators from which repairs are requested
    // pub repair_validators: Option<HashSet<Pubkey>>,
    // Validators which should be given priority when serving repairs
    // pub repair_whitelist: Arc<RwLock<HashSet<Pubkey>>>,
    // pub wait_for_vote_to_start_leader: bool,
    // pub replay_forks_threads: NonZeroUsize,
    // pub replay_transactions_threads: NonZeroUsize,
}

impl Default for ShredCollectorServiceConfig {
    fn default() -> Self {
        Self {
            // max_ledger_shreds: None,
            shred_version: 0,
            // repair_validators: None,
            // repair_whitelist: Arc::new(RwLock::new(HashSet::default())),
            // wait_for_vote_to_start_leader: false,
            // replay_forks_threads: NonZeroUsize::new(1).expect("1 is non-zero"),
            // replay_transactions_threads: NonZeroUsize::new(1).expect("1 is non-zero"),
        }
    }
}


fn verify_repair(
    outstanding_requests: &mut OutstandingShredRepairs,
    shred: &Shred,
    repair_meta: &Option<RepairMeta>,
) -> bool {
    let r = repair_meta
        .as_ref()
        .map(|repair_meta| {
            outstanding_requests
                .register_response(
                    repair_meta.nonce,
                    shred,
                    solana_sdk::timing::timestamp(),
                    |_| (),
                )
                .is_some()
        })
        .unwrap_or(true);

    debug!("verify_repair {}",r);
    r
}

fn prune_shreds_by_repair_status(
    shreds: &mut Vec<Shred>,
    repair_infos: &mut Vec<Option<RepairMeta>>,
    outstanding_requests: &RwLock<OutstandingShredRepairs>,
    accept_repairs_only: bool,
) {
    debug!("prune_shreds_by_repair_status shreds.len: {} accept_repairs_only: {}", shreds.len(),accept_repairs_only);

    assert_eq!(shreds.len(), repair_infos.len());
    let mut i = 0;
    let mut removed = HashSet::new();
    {
        let mut outstanding_requests = outstanding_requests.write().unwrap();
        shreds.retain(|shred| {
            let should_keep = (
                (!accept_repairs_only || repair_infos[i].is_some())
                    && verify_repair(&mut outstanding_requests, shred, &repair_infos[i]),
                i += 1,
            )
                .0;
            if !should_keep {
                removed.insert(i - 1);
            }
            should_keep
        });
    }
    i = 0;
    repair_infos.retain(|_repair_info| (!removed.contains(&i), i += 1).0);
    assert_eq!(shreds.len(), repair_infos.len());
}
#[allow(clippy::too_many_arguments)]
fn run_insert<F>(
    thread_pool: &ThreadPool,
    verified_receiver: &Receiver<Vec<PacketBatch>>,
    blockstore: &Blockstore,
    leader_schedule_cache: &LeaderScheduleCache,
    handle_duplicate: F,
    metrics: &mut BlockstoreInsertionMetrics,
    // ws_metrics: &mut WindowServiceMetrics,
    completed_data_sets_sender: Option<&CompletedDataSetsSender>,
    retransmit_sender: &Sender<Vec<ShredPayload>>,
    outstanding_requests: &RwLock<OutstandingShredRepairs>,
    reed_solomon_cache: &ReedSolomonCache,
    accept_repairs_only: bool,
) -> Result<()>
where
    F: Fn(PossibleDuplicateShred),
{
    const RECV_TIMEOUT: Duration = Duration::from_millis(200);
    let mut shred_receiver_elapsed = Measure::start("shred_receiver_elapsed");
    // let mut packets = verified_receiver.recv_timeout(RECV_TIMEOUT)?;

    // 首先尝试立即获取数据
    let mut packets = match verified_receiver.try_recv() {
        Ok(packets) => packets,
        Err(TryRecvError::Empty) => {
            // 如果没有数据，使用较短的超时时间等待
            const SHORT_TIMEOUT: Duration = Duration::from_millis(100);
            verified_receiver.recv_timeout(RECV_TIMEOUT)?
        }
        Err(TryRecvError::Disconnected) => return Ok(()),
    };

    packets.extend(verified_receiver.try_iter().flatten());
    shred_receiver_elapsed.stop();
    // ws_metrics.shred_receiver_elapsed_us += shred_receiver_elapsed.as_us();
    // ws_metrics.run_insert_count += 1;
    let handle_packet = |packet: &Packet| {
        // debug!("packet.meta().repair(): {}", packet.meta().repair());
        debug!("packet meta size: {}", packet.meta().size);
        if let Some(data) = packet.data(..) {
            debug!("packet data length: {}", data.len());
        }
        debug!("packet.meta().repair(): {}", packet.meta().repair());
        if packet.meta().discard() {
            debug!("packet.meta().discard(): {}", packet.meta().discard());
            // return None;
        }
        // let shred = shred::layout::get_shred(packet);
        // debug!("get_shred: {}", shred);
        // let size = packet.data(..).len();
        // debug!("packet data: {:?}", packet.data(..));
        
        let shred = shred::layout::get_shred(packet)?;
        // debug!("get_shred:0");
        let shred = Shred::new_from_serialized_shred(shred.to_vec()).ok()?;
        // debug!("get_shred:1");
        debug!("handle_packet shred slot:{} index: {}", shred.slot(), shred.index());
        if packet.meta().repair() {
            let repair_info = RepairMeta {
                // If can't parse the nonce, dump the packet.
                nonce: repair_response::nonce(packet)?,
            };
            Some((shred, Some(repair_info)))
        } else {
            Some((shred, None))
        }
    };
    let now = Instant::now();
    let (mut shreds, mut repair_infos): (Vec<_>, Vec<_>) = thread_pool.install(|| {
        packets
            .par_iter()
            .flat_map_iter(|packets| packets.iter().filter_map(handle_packet))
            .unzip()
    });
    // ws_metrics.handle_packets_elapsed_us += now.elapsed().as_micros() as u64;
    // ws_metrics.num_packets += packets.iter().map(PacketBatch::len).sum::<usize>();
    // ws_metrics.num_repairs += repair_infos.iter().filter(|r| r.is_some()).count();
    // ws_metrics.num_shreds_received += shreds.len();
    // for packet in packets.iter().flat_map(PacketBatch::iter) {
    //     let addr = packet.meta().socket_addr();
    //     *ws_metrics.addrs.entry(addr).or_default() += 1;
    // }

    let mut prune_shreds_elapsed = Measure::start("prune_shreds_elapsed");
    let num_shreds = shreds.len();
    prune_shreds_by_repair_status(
        &mut shreds,
        &mut repair_infos,
        outstanding_requests,
        accept_repairs_only,
    );
    // ws_metrics.num_shreds_pruned_invalid_repair = num_shreds - shreds.len();
    let repairs: Vec<_> = repair_infos
        .iter()
        .map(|repair_info| repair_info.is_some())
        .collect();
    // prune_shreds_elapsed.stop();
    // ws_metrics.prune_shreds_elapsed_us += prune_shreds_elapsed.as_us();
//    let mut metrics = BlockstoreInsertionMetrics::default();
    let completed_data_sets = blockstore.insert_shreds_handle_duplicate(
        shreds,
        repairs,
        Some(leader_schedule_cache),
        false, // is_trusted
        Some(retransmit_sender),
        &handle_duplicate,
        reed_solomon_cache,
        metrics,
    )?;
    // debug!("completed_data_sets: {}", completed_data_sets.len());
    if let Some(sender) = completed_data_sets_sender {
        sender.try_send(completed_data_sets)?;
    }

    Ok(())
}



fn start_insert_thread(
    exit: Arc<AtomicBool>,
    blockstore: Arc<Blockstore>,
    leader_schedule_cache: Arc<LeaderScheduleCache>,
    verified_receiver: Receiver<Vec<PacketBatch>>,
    check_duplicate_sender: Sender<PossibleDuplicateShred>,
    completed_data_sets_sender: Option<CompletedDataSetsSender>,
    retransmit_sender: Sender<Vec<ShredPayload>>,
    outstanding_requests: Arc<RwLock<OutstandingShredRepairs>>,
    accept_repairs_only: bool,
) -> JoinHandle<()> {
    let handle_error = || {
        // inc_new_counter_error!("solana-window-insert-error", 1, 1);
    };
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(get_thread_count().min(8))
        .thread_name(|i| format!("solWinInsert{i:02}"))
        .build()
        .unwrap();
    let reed_solomon_cache = ReedSolomonCache::default();
    Builder::new()
        .name("solWinInsert".to_string())
        .spawn(move || {
            let handle_duplicate = |possible_duplicate_shred| {
                let _ = check_duplicate_sender.send(possible_duplicate_shred);
            };
            let mut metrics = BlockstoreInsertionMetrics::default();
            // let mut ws_metrics = WindowServiceMetrics::default();
            // let mut last_print = Instant::now();
            while !exit.load(Ordering::Relaxed) {

                debug!("run_insert");   
                if let Err(e) = run_insert(
                    &thread_pool,
                    &verified_receiver,
                    &blockstore,
                    &leader_schedule_cache,
                    handle_duplicate,
                    &mut metrics,
                    // &mut ws_metrics,
                    completed_data_sets_sender.as_ref(),
                    &retransmit_sender,
                    &outstanding_requests,
                    &reed_solomon_cache,
                    accept_repairs_only,
                ) {
                    // ws_metrics.record_error(&e);
                    // if Self::should_exit_on_error(e, &handle_error) {
                    //     break;
                    // }
                }

                // if last_print.elapsed().as_secs() > 2 {
                //     metrics.report_metrics("blockstore-insert-shreds");
                //     metrics = BlockstoreInsertionMetrics::default();
                //     ws_metrics.report_metrics("recv-window-insert-shreds");
                //     ws_metrics = WindowServiceMetrics::default();
                //     last_print = Instant::now();
                // }
            }
        })
        .unwrap()
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
    ) -> std::result::Result<Self, String> {
        

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

                let collector = solana_sdk::pubkey::new_rand();

                // let mut new_bank_time = Measure::start("new_bank");
                let new_slot = bank.slot() + 1;
                let root_bank: Arc<Bank> = Arc::new(bank);
        
                let bank = Bank::new_from_parent(root_bank, &collector, start_slot);
          

                // bank.set_root_slot(start_slot);

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
                // let repair_slots = vec![start_slot];
                let repair_slots = vec![];

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

              
                let accept_repairs_only = false;//repair_info.wen_restart_repair_slots.is_some();

                
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
                    outstanding_requests.clone(),
                    ancestor_hashes_replay_update_receiver,
                    dumped_slots_receiver,
                    popular_pruned_forks_sender,
                );
                  
                let bank = bank_forks.read().unwrap().working_bank();

                let leader_schedule_cache: Arc<LeaderScheduleCache> = Arc::new(LeaderScheduleCache::new_from_bank(&bank));

                let (verified_sender, verified_receiver) = unbounded();
                let (retransmit_sender, retransmit_receiver) = unbounded();

                
                 // let recycler: solana_perf::recycler::Recycler<solana_perf::cuda_runtime::PinnedVec<solana_sdk::packet::Packet>> = PacketBatchRecycler::warmed(100, 1024);
                 let (fetch_sender, fetch_receiver) = unbounded();
              

                let shred_sigverify = solana_turbine::sigverify_shreds::spawn_shred_sigverify(
                    cluster_info.clone(),
                    bank_forks.clone(),
                    leader_schedule_cache.clone(),
                    fetch_receiver,
                    retransmit_sender.clone(),
                    verified_sender,
                );
             

                let (duplicate_sender, duplicate_receiver) = unbounded();
                let (completed_data_sets_sender, completed_data_sets_receiver) =
                bounded(MAX_COMPLETED_DATA_SETS_IN_CHANNEL);

                // let completed_data_sets_service = CompletedDataSetsService::new(
                //     completed_data_sets_receiver,
                //     blockstore.clone(),
                //     rpc_subscriptions.clone(),
                //     exit.clone(),
                //     max_slots.clone(),
                // );

                let insert_thread = start_insert_thread(
                    exit.clone(),
                    blockstore.clone(),
                    leader_schedule_cache.clone(),
                    verified_receiver,
                    duplicate_sender,
                    Some(completed_data_sets_sender),
                    retransmit_sender,
                    outstanding_requests,
                    accept_repairs_only,
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




                info!("Shred collection started");

                Ok(ShredCollectorService {
                    fetch_stage,
                    repair_service,
                    shred_sigverify,
                    insert_thread,
                })
            
    }

    pub fn join(self) -> thread::Result<()> {
        self.fetch_stage.join()?;
        self.repair_service.join()?;

        self.shred_sigverify.join()?;
        self.insert_thread.join()?;
        Ok(())
    }



}   
