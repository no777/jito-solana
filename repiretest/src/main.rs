use {
    anyhow::{Context, Result, anyhow},
    solana_core::repair::{
        serve_repair::{ServeRepair, ShredRepairType, RepairProtocol, RepairRequestHeader},
        repair_service::RepairStats,
    },
    solana_core::cluster_slots_service::cluster_slots::ClusterSlots,
    solana_gossip::{
        cluster_info::{ClusterInfo, Node},
        contact_info::ContactInfo,
    },
    solana_ledger::{
        blockstore::Blockstore,
        blockstore_options::{AccessType, BlockstoreOptions, LedgerColumnOptions},
        shred::Shred,
        genesis_utils::create_genesis_config,
    },
    solana_runtime::{
        bank::Bank,
        bank_forks::BankForks,
        genesis_utils::GenesisConfigInfo,
    },
    solana_sdk::{
        clock::Slot,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        timing::timestamp,
        native_token::LAMPORTS_PER_SOL,
    },
    solana_streamer::socket::SocketAddrSpace,
    std::{
        collections::HashSet,
        env,
        fs,
        net::{SocketAddr, UdpSocket, IpAddr},
        path::{Path, PathBuf},
        sync::{Arc, RwLock},
        time::Duration,
        str::FromStr,
    },
};

struct RepairClient {
    cluster_info: Arc<ClusterInfo>,
    repair_socket: UdpSocket,
    blockstore: Arc<Blockstore>,
    serve_repair: ServeRepair,
    cluster_slots: Arc<ClusterSlots>,
    bank_forks: Arc<RwLock<BankForks>>,
}

impl RepairClient {
    pub fn new(ledger_path: &Path, local_addr: &str, public_addr: Option<&str>) -> Result<Self> {
        // Initialize node and cluster info
        let node_keypair = Arc::new(Keypair::new());
        
        // If public_addr is provided, use it for the node's contact info
        let node = if let Some(public_ip) = public_addr {
            let mut node = Node::new_localhost_with_pubkey(&node_keypair.pubkey());
            let mut info = node.info.clone();
            
            // Parse the IP address once
            let ip = IpAddr::from_str(public_ip).context("Failed to parse public IP")?;
            
            // Create socket addresses for each service
            let gossip_addr = SocketAddr::new(ip, 8000);
            let tpu_addr = SocketAddr::new(ip, 8001);
            let tpu_forwards_addr = SocketAddr::new(ip, 8002);
            let tvu_addr = SocketAddr::new(ip, 8003);
            let serve_repair_addr = SocketAddr::new(ip, 8004);
            
            // Set the addresses
            let _ = info.set_gossip(gossip_addr);
            let _ = info.set_tpu(tpu_addr);
            let _ = info.set_tpu_forwards(tpu_forwards_addr);
            let _ = info.set_tvu(tvu_addr);
            let _ = info.set_serve_repair(serve_repair_addr);
            
            node.info = info;
            node
        } else {
            Node::new_localhost_with_pubkey(&node_keypair.pubkey())
        };

        let cluster_info = Arc::new(ClusterInfo::new(
            node.info.clone(),
            node_keypair.clone(),
            SocketAddrSpace::Unspecified,
        ));

        // Create blockstore
        let blockstore = Arc::new(
            Blockstore::open_with_options(
                ledger_path,
                BlockstoreOptions {
                    access_type: AccessType::Primary,
                    recovery_mode: None,
                    enforce_ulimit_nofile: false,
                    column_options: LedgerColumnOptions::default(),
                },
            )
            .context("Failed to open blockstore")?,
        );

        // Create repair socket
        let repair_socket = UdpSocket::bind(format!("{}:0", local_addr))
            .context("Failed to bind repair socket")?;
        println!("Binding repair socket to {}:0", local_addr);

        // Create genesis config and bank
        let validator_keypair = Keypair::new();
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
            solana_accounts_db::accounts_index::AccountSecondaryIndexes::default(),
            solana_accounts_db::accounts_db::AccountShrinkThreshold::default(),
            false,
            None,
            None,
            None,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            None,
        );

        let bank_forks = BankForks::new_rw_arc(bank);
        
        
        // Initialize serve repair
        let repair_whitelist = Arc::new(RwLock::new(HashSet::<Pubkey>::default()));
        let serve_repair = ServeRepair::new(
            cluster_info.clone(),
            bank_forks.clone(),
            repair_whitelist,
        );

        Ok(RepairClient {
            cluster_info,
            repair_socket,
            blockstore,
            serve_repair,
            cluster_slots: Arc::new(ClusterSlots::default()),
            bank_forks,
        })
    }

    pub fn request_repair(&self, repair_peer_addr: SocketAddr, slot: Slot, shred_index: u64) -> Result<()> {
        let mut repair_peer_info = ContactInfo::new_localhost(&Pubkey::new_unique(), timestamp());
        let _ = repair_peer_info.set_gossip(repair_peer_addr);
        let _ = repair_peer_info.set_tvu(repair_peer_addr);

        let repair_request = ShredRepairType::Shred(slot, shred_index);
        println!("Requesting repair for slot {} shred {}", slot, shred_index);

        // 创建修复请求
        let nonce = timestamp() as u32;
        let keypair = Keypair::new();
        let repair_request = RepairProtocol::WindowIndex {
            header: RepairRequestHeader::new(
                keypair.pubkey(),
                *repair_peer_info.pubkey(),
                timestamp(),
                nonce,
            ),
            slot,
            shred_index,
        };

        let request_bytes = ServeRepair::repair_proto_to_bytes(&repair_request, &keypair)
            .context("Failed to create repair request")?;

        println!("Sending repair request to {}", repair_peer_addr);
        self.repair_socket
            .send_to(&request_bytes, repair_peer_addr)
            .context("Failed to send repair request")?;

        // 设置接收超时
        self.repair_socket.set_read_timeout(Some(Duration::from_secs(5)))?;

        // 接收响应
        let mut buffer = [0u8; 65536];  // 64KB buffer
        match self.repair_socket.recv_from(&mut buffer) {
            Ok((size, from)) => {
                println!("Received {} bytes from {}", size, from);
                
                // 尝试将数据作为 shred 插入到 blockstore
                if let Ok(shred) = Shred::new_from_serialized_shred(buffer[..size].to_vec()) {
                    println!("Successfully parsed shred for slot {}", shred.slot());
                    
                    // 插入到 blockstore
                    if let Err(e) = self.blockstore.insert_shreds(
                        vec![shred],
                        None, // leader_schedule
                        false, // is_trusted
                    ) {
                        println!("Failed to insert shred into blockstore: {}", e);
                    } else {
                        println!("Successfully inserted shred into blockstore for slot {}", slot);
                    }
                } else {
                    println!("Failed to parse received data as shred");
                }
            }
            Err(e) => {
                println!("No response received: {}", e);
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Get command line arguments
    let args: Vec<String> = env::args().collect();
    let mut target_ip = None;
    let mut public_ip = None;
    let mut slot = 100u64;  // default slot
    let mut i = 1;
    
    while i < args.len() {
        match args[i].as_str() {
            "--slot" => {
                if i + 1 < args.len() {
                    slot = args[i + 1].parse().context("Failed to parse slot number")?;
                    i += 2;
                } else {
                    return Err(anyhow!("--slot requires a value"));
                }
            }
            arg => {
                if target_ip.is_none() {
                    target_ip = Some(arg);
                } else if public_ip.is_none() {
                    public_ip = Some(arg);
                }
                i += 1;
            }
        }
    }

    let target_ip = target_ip.ok_or_else(|| anyhow!("Please provide a target IP address"))?;
    
    // Always bind to localhost
    let local_ip = "0.0.0.0";

    // Use default ledger path
    let ledger_path = PathBuf::from("./test-ledger");

    println!("Using local IP: {}, public IP: {}, target IP: {}, slot: {}, ledger path: {}", 
             local_ip, 
             public_ip.unwrap_or("none"), 
             target_ip, 
             slot,
             ledger_path.display());

    // Create ledger directory if it doesn't exist
    if !ledger_path.exists() {
        fs::create_dir_all(&ledger_path).context("Failed to create ledger directory")?;
    }
    
    // Initialize repair client with local IP and ledger path
    let repair_client = RepairClient::new(&ledger_path, local_ip, public_ip)?;

    // Send repair requests to target IP
    for port in 8001..8010 {
        let repair_peer_addr = SocketAddr::new(IpAddr::from_str(target_ip).context("Failed to parse target IP")?, port);
        let shred_index = 0;

        println!("Attempting repair request to {}", repair_peer_addr);
        // Send repair request and handle response
        if let Err(e) = repair_client.request_repair(repair_peer_addr, slot, shred_index) {
            println!("Failed to repair from {}: {}", repair_peer_addr, e);
        }
    }
    Ok(())
}
