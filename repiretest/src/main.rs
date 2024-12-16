use {
    anyhow::{Context, Result, anyhow},
    solana_accounts_db::{
        accounts_db::AccountShrinkThreshold,
        accounts_index::AccountSecondaryIndexes,
    },
    solana_core::repair::serve_repair::{ServeRepair, ShredRepairType},
    solana_gossip::{
        cluster_info::{ClusterInfo, Node},
        contact_info::ContactInfo,
    },
    solana_ledger::{
        blockstore::Blockstore,
        blockstore_options::{AccessType, BlockstoreOptions, LedgerColumnOptions, ShredStorageType},
    },
    solana_runtime::{
        bank::Bank,
        bank_forks::BankForks,
        genesis_utils::{create_genesis_config_with_leader, GenesisConfigInfo},
    },
    solana_sdk::{
        clock::{Clock, Slot},
        packet::Packet,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        timing::timestamp,
        native_token::LAMPORTS_PER_SOL,
        vote::state::{VoteInit, VoteState},
    },
    solana_streamer::socket::SocketAddrSpace,
    std::{
        collections::HashSet,
        env,
        fs,
        net::{SocketAddr, UdpSocket, IpAddr},
        path::{Path, PathBuf},
        sync::{atomic::AtomicBool, Arc, RwLock},
        time::Duration,
        str::FromStr,
    },
};

struct RepairClient {
    cluster_info: Arc<ClusterInfo>,
    repair_socket: UdpSocket,
    blockstore: Arc<Blockstore>,
    serve_repair: ServeRepair,
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

        // Always bind to local address for the socket
        let bind_addr = format!("{}:0", local_addr);
        println!("Binding repair socket to {}", bind_addr);
        let repair_socket = UdpSocket::bind(&bind_addr).context("Failed to bind repair socket")?;
        repair_socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .context("Failed to set socket timeout")?;

        // Initialize blockstore with modified options
        let blockstore = Arc::new(
            Blockstore::open_with_options(
                ledger_path,
                BlockstoreOptions {
                    access_type: AccessType::Primary,
                    recovery_mode: None,
                    enforce_ulimit_nofile: false,
                    column_options: LedgerColumnOptions {
                        shred_storage_type: ShredStorageType::RocksLevel,
                        ..LedgerColumnOptions::default()
                    },
                },
            )
            .context("Failed to open blockstore")?,
        );

        // Create validator vote keypairs
        let validator_keypair = Keypair::new();
        let vote_keypair = Keypair::new();
        let validator_stake = 42 * LAMPORTS_PER_SOL;
        let validator_lamports = 42 * LAMPORTS_PER_SOL;

        // Create genesis config with the validator as leader
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair: _,
            voting_keypair: _,
            validator_pubkey: _,
        } = create_genesis_config_with_leader(
            validator_lamports,
            &validator_keypair.pubkey(),
            validator_stake,
        );

        // Create bank with the genesis config
        let bank = Bank::new_with_paths(
            &genesis_config,
            Arc::new(Default::default()),
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

        // Initialize vote account
        let vote_init = VoteInit {
            node_pubkey: validator_keypair.pubkey(),
            authorized_voter: validator_keypair.pubkey(),
            authorized_withdrawer: validator_keypair.pubkey(),
            commission: 0,
        };

        let clock = Clock::default();
        let _vote_state = VoteState::new(&vote_init, &clock);

        let bank_forks = BankForks::new_rw_arc(bank);

        // Initialize serve repair with required HashSet
        let repair_whitelist = Arc::new(RwLock::new(HashSet::<Pubkey>::default()));
        let serve_repair = ServeRepair::new(
            cluster_info.clone(),
            bank_forks,
            repair_whitelist,
        );

        Ok(Self {
            cluster_info,
            repair_socket,
            blockstore,
            serve_repair,
        })
    }

    pub fn request_repair(
        &self,
        repair_peer_addr: SocketAddr,
        slot: Slot,
        shred_index: u64,
    ) -> Result<()> {
        println!("Requesting repair for slot {} shred {}", slot, shred_index);

        // Create repair request
        let _repair_request = ShredRepairType::Shred(slot, shred_index);
        
        // Create a dummy contact info for the repair peer
        let mut repair_peer_info = ContactInfo::new_localhost(&self.cluster_info.id(), timestamp());
        repair_peer_info.set_gossip(repair_peer_addr);

        // Create repair request packet
        let mut packet = Packet::default();
        packet.meta_mut().size = 1024;

        // Send repair request
        println!("Sending repair request to {}", repair_peer_addr);
        self.repair_socket
            .send_to(packet.buffer_mut(), repair_peer_addr)
            .context("Failed to send repair request")?;

        // Try to receive response with a timeout
        let mut response_packet = Packet::default();
        match self.repair_socket.recv_from(response_packet.buffer_mut()) {
            Ok((size, addr)) => {
                println!("Received response from {}", addr);
                response_packet.meta_mut().size = size;

                // Process and store the response
                if let Some(shred_data) = response_packet.data(..) {
                    println!("Received repair response of size: {}", shred_data.len());
                    println!("First few bytes of response: {:?}", &shred_data[..std::cmp::min(32, shred_data.len())]);
                }
            }
            Err(e) => {
                println!("No response received: {}", e);
                // Continue even if we don't receive a response
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Get command line arguments
    let args: Vec<String> = env::args().collect();
    
    // First argument is the target IP to send requests to
    let target_ip = args.get(1)
        .ok_or_else(|| anyhow!("Please provide a target IP address as the first argument"))?;
    
    // Second argument is the public IP (optional)
    let public_ip = args.get(2).map(|s| s.as_str());
    
    // Always bind to localhost
    let local_ip = "0.0.0.0";

    // Third argument is the ledger path (optional, defaults to "./test-ledger")
    let ledger_path = args.get(3).map(PathBuf::from).unwrap_or_else(|| PathBuf::from("./test-ledger"));

    println!("Using local IP: {}, public IP: {}, target IP: {}, ledger path: {}", 
             local_ip, public_ip.unwrap_or("none"), target_ip, ledger_path.display());

    // Create ledger directory if it doesn't exist
    if !ledger_path.exists() {
        fs::create_dir_all(&ledger_path).context("Failed to create ledger directory")?;
    }
    
    // Initialize repair client with local IP and ledger path
    let repair_client = RepairClient::new(&ledger_path, local_ip, public_ip)?;

    // Send repair requests to target IP
    for i in 8001..8010 {
        let repair_peer_addr = SocketAddr::new(IpAddr::from_str(target_ip).context("Failed to parse target IP")?, i);
        
        let slot = 307798183;
        let shred_index = 0;

        println!("Attempting repair request to {}", repair_peer_addr);
        // Send repair request and handle response
        if let Err(e) = repair_client.request_repair(repair_peer_addr, slot, shred_index) {
            println!("Failed to repair from {}: {}", repair_peer_addr, e);
        }
    }
    Ok(())
}
