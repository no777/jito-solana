use {
    anyhow::{Context, Result}, core::fmt, solana_accounts_db::{
        accounts_db::AccountShrinkThreshold, accounts_index::AccountSecondaryIndexes
    }, solana_core::repair::serve_repair::{ServeRepair, ShredRepairType}, solana_gossip::{
        cluster_info::{ClusterInfo, Node},
        contact_info::ContactInfo,
    }, solana_ledger::{
        blockstore::Blockstore,
        blockstore_options::{AccessType, LedgerColumnOptions},
    }, solana_runtime::{
        bank::Bank,
        bank_forks::BankForks,
        genesis_utils::create_genesis_config,
    }, solana_sdk::{
        clock::Slot, packet::Packet, pubkey::Pubkey, signature::{Keypair, Signer}, timing::timestamp
    }, solana_streamer::socket::SocketAddrSpace, std::{
        collections::HashSet,
        net::{SocketAddr, UdpSocket},
        path::Path,
        sync::{atomic::AtomicBool, Arc, RwLock},
        time::Duration,
    }
};

struct RepairClient {
    cluster_info: Arc<ClusterInfo>,
    repair_socket: UdpSocket,
    blockstore: Arc<Blockstore>,
    serve_repair: ServeRepair,
}

impl RepairClient {
    pub fn new(ledger_path: &Path) -> Result<Self> {
        // Initialize node and cluster info
        let node_keypair = Keypair::new();
        let node = Node::new_localhost_with_pubkey(&node_keypair.pubkey());
        let cluster_info = Arc::new(ClusterInfo::new(
            node.info.clone(),
            Arc::new(node_keypair),
            SocketAddrSpace::Unspecified,
        ));

        // Initialize repair socket
        let repair_socket = UdpSocket::bind("24.64.100.231:0").context("Failed to bind repair socket")?;
        repair_socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .context("Failed to set socket timeout")?;

        // Initialize blockstore
        let blockstore = Arc::new(
            Blockstore::open_with_options(
                ledger_path,
                solana_ledger::blockstore_options::BlockstoreOptions {
                    access_type: AccessType::Primary,
                    recovery_mode: None,
                    enforce_ulimit_nofile: true,
                    column_options: LedgerColumnOptions::default(),
                },
            )
            .context("Failed to open blockstore")?,
        );

        // Create genesis config and bank
        let genesis_config = create_genesis_config(10_000).genesis_config;
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
        let repair_request = ShredRepairType::Shred(slot, shred_index);
        
        // Create a dummy contact info for the repair peer
        let mut repair_peer_info = ContactInfo::new_localhost(&self.cluster_info.id(), timestamp());
        repair_peer_info.set_gossip(repair_peer_addr);

        // Create repair request packet
        let mut packet = Packet::default();
        packet.meta_mut().size = 1024; // Set a reasonable size

        // Send repair request
        self.repair_socket
            .send_to(packet.buffer_mut(), repair_peer_addr)
            .context("Failed to send repair request")?;

        // Receive response
        let mut response_packet = Packet::default();
        let (size, _addr) = self
            .repair_socket
            .recv_from(response_packet.buffer_mut())
            .context("Failed to receive repair response")?;
        response_packet.meta_mut().size = size;

        // Process and store the response
        if let Some(shred_data) = response_packet.data(..) {
            println!("Received repair response of size: {}", shred_data.len());
            println!("First few bytes of response: {:?}", &shred_data[..std::cmp::min(32, shred_data.len())]);
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Create a temporary directory for blockstore
    let ledger_path = tempfile::tempdir().context("Failed to create temp dir")?;
    
    // Initialize repair client
    let repair_client = RepairClient::new(ledger_path.path())?;

    // Example repair request
    // Note: In a real scenario, you would get these values from your network
    for i in 8001..8010 {
        let repair_peer_addr = format!("{}:{}", "72.46.86.235", i).parse().unwrap(); 
        let slot = 100;
        let shred_index = 0;

        println!("Attempting repair request to {}", repair_peer_addr);
        // Send repair request and handle response
        if let Err(e) = repair_client.request_repair(repair_peer_addr, slot, shred_index) {
            println!("Failed to repair from {}: {}", repair_peer_addr, e);
        }
    }
    Ok(())
}
