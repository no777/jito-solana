mod service;

use {
    clap::{crate_description, crate_name, value_t, values_t_or_exit, App, Arg},
    log::*,
    rand::{seq::SliceRandom, thread_rng},
    solana_clap_utils::{
        input_parsers::keypair_of,
        input_validators::{is_keypair, is_port},
    },
    solana_gossip::{
        cluster_info::{ClusterInfo, Node},
        contact_info::ContactInfo,
        gossip_service::GossipService,
    },
    solana_ledger::{
        blockstore::Blockstore,
        blockstore_options::{
            AccessType, BlockstoreOptions, BlockstoreRecoveryMode, LedgerColumnOptions,
        },
    },
    solana_net_utils::{
        VALIDATOR_PORT_RANGE, parse_host, parse_port_or_addr, get_public_ip_addr,
    },

    solana_sdk::{
        pubkey::Pubkey,
        signature::Signer,
        clock::Slot,
    },
    solana_streamer::socket::SocketAddrSpace,
    service::ShredCollectorService,
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr,  ToSocketAddrs},
        path::PathBuf,
        process,
        sync::{atomic::{AtomicBool, Ordering}, Arc},
        thread::sleep,
        time::Duration,
    },
};

fn parse_entrypoint(entrypoint: &str) -> SocketAddr {
    if let Ok(mut addrs) = entrypoint.to_socket_addrs() {
        if let Some(addr) = addrs.next() {
            return addr;
        }
    }
    
    // Fallback to default parsing if DNS resolution fails
    let default_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), VALIDATOR_PORT_RANGE.0);
    parse_port_or_addr(Some(entrypoint), default_addr)
}

fn main() {
    solana_logger::setup_with_default("info");

    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version("1.0.0")
        .arg(
            Arg::with_name("identity")
                .short("i")
                .long("identity")
                .value_name("PATH")
                .takes_value(true)
                .validator(is_keypair)
                .help("File containing the identity keypair"),
        )
        .arg(
            Arg::with_name("gossip_port")
                .long("gossip-port")
                .value_name("PORT")
                .takes_value(true)
                .validator(is_port)
                .help("Gossip port number for the validator"),
        )
        .arg(
            Arg::with_name("gossip_host")
                .long("gossip-host")
                .value_name("HOST")
                .takes_value(true)
                .help("Gossip DNS name or IP address for the validator to advertise in gossip"),
        )
        .arg(
            Arg::with_name("entrypoint")
                .long("entrypoint")
                .value_name("HOST:PORT")
                .takes_value(true)
                .multiple(true)
                .help("Rendezvous with the cluster at this entry point"),
        )
        .arg(
            Arg::with_name("ledger_path")
                .long("ledger")
                .value_name("DIR")
                .takes_value(true)
                .required(true)
                .help("Use DIR as ledger location"),
        )
        .arg(
            Arg::with_name("start_slot")
                .long("start-slot")
                .value_name("SLOT")
                .takes_value(true)
                .required(true)
                .help("Start collecting shreds from this slot"),
        )
        .get_matches();

    let start_slot = value_t!(matches, "start_slot", Slot).unwrap_or_else(|e| {
        eprintln!("error: {:?}", e);
        process::exit(1);
    });

    let ledger_path = PathBuf::from(value_t!(matches, "ledger_path", String).unwrap_or_else(|e| {
        eprintln!("error: {:?}", e);
        process::exit(1);
    }));

    let identity_keypair = keypair_of(&matches, "identity").unwrap_or_else(|| {
        eprintln!("The --identity <PATH> argument is required");
        process::exit(1);
    });

    let bind_address = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));

    // Parse entrypoints
    let entrypoint_addrs = values_t_or_exit!(matches, "entrypoint", String)
        .into_iter()
        .map(|entrypoint| parse_entrypoint(&entrypoint))
        .collect::<Vec<_>>();

    // Get the gossip host address
    let gossip_host: IpAddr = matches
        .value_of("gossip_host")
        .map(|gossip_host| {
            parse_host(gossip_host).unwrap_or_else(|err| {
                eprintln!("Failed to parse --gossip-host: {err}");
                process::exit(1);
            })
        })
        .unwrap_or_else(|| {
            if !entrypoint_addrs.is_empty() {
                let mut order: Vec<_> = (0..entrypoint_addrs.len()).collect();
                order.shuffle(&mut thread_rng());

                let gossip_host = order.into_iter().find_map(|i| {
                    let entrypoint_addr = &entrypoint_addrs[i];
                    info!(
                        "Contacting {} to determine the validator's public IP address",
                        entrypoint_addr
                    );
                    get_public_ip_addr(entrypoint_addr).map_or_else(
                        |err| {
                            eprintln!(
                                "Failed to contact cluster entrypoint {entrypoint_addr}: {err}"
                            );
                            None
                        },
                        Some,
                    )
                });

                gossip_host.unwrap_or_else(|| {
                    info!("Unable to determine public IP, using local IP");
                    bind_address
                })
            } else {
                bind_address
            }
        });

    let gossip_addr = SocketAddr::new(
        gossip_host,
        value_t!(matches, "gossip_port", u16).unwrap_or_else(|_| {
            solana_net_utils::find_available_port_in_range(bind_address, (0, 1)).unwrap_or_else(
                |err| {
                    eprintln!("Unable to find an available gossip port: {err}");
                    process::exit(1);
                },
            )
        }),
    );

    info!("Gossip address: {}", gossip_addr);

    // let mut node = Node::new_single_bind(
    //     &identity_keypair.pubkey(),
    //     &gossip_addr,
    //     VALIDATOR_PORT_RANGE,
    //     bind_address,
    // );

    let mut node = Node::new_single_bind(
        &identity_keypair.pubkey(),
        &gossip_addr,
        (gossip_addr.port() + 1, gossip_addr.port() + 10),
        bind_address,
    );


    // Remove unused ports since we only need gossip and repair
    node.info.remove_tpu();
    node.info.remove_tpu_forwards();
    node.info.remove_tvu();
    node.info.remove_serve_repair();
    node.sockets.ip_echo = None;

    // Get the gossip socket before wrapping node in Arc
    let gossip_socket = node.sockets.gossip.try_clone().unwrap_or_else(|e| {
        error!("Failed to clone gossip socket: {}", e);
        std::process::exit(1);
    });
    let node = Arc::new(node);

    let cluster_info = Arc::new(ClusterInfo::new(
        node.info.clone(),
        Arc::new(identity_keypair),
        SocketAddrSpace::Unspecified,
    ));

    if !entrypoint_addrs.is_empty() {
        info!("Connecting to {} entrypoints:", entrypoint_addrs.len());
        for entrypoint_addr in &entrypoint_addrs {
            info!("  {}", entrypoint_addr);
            let mut entrypoint_info = ContactInfo::new_localhost(&Pubkey::default(), 0);
            entrypoint_info.set_gossip(*entrypoint_addr).unwrap();
            cluster_info.set_entrypoint(entrypoint_info);
        }
    }

    let exit = Arc::new(AtomicBool::new(false));
    let _gossip_service = GossipService::new(
        &cluster_info,
        None,
        gossip_socket,
        None,
        true,
        None,
        exit.clone(),
    );

    info!("Gossip Service started on port {}", gossip_addr.port());


    let repair_socket =  node.sockets.repair.try_clone().unwrap();
    let repair_socket = Arc::new(repair_socket);
    // let repair_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").unwrap_or_else(|err| {
    //     eprintln!("Failed to bind repair socket: {}", err);
    //     process::exit(1);
    // }));

    let blockstore = Arc::new(
        Blockstore::open_with_options(
            &ledger_path,
            BlockstoreOptions {
                access_type: AccessType::Primary,
                recovery_mode: Some(BlockstoreRecoveryMode::TolerateCorruptedTailRecords),
                enforce_ulimit_nofile: false,
                column_options: LedgerColumnOptions::default(),
            },
        )
        .unwrap_or_else(|err| {
            eprintln!("Failed to open ledger at {:?}: {:?}", ledger_path, err);
            process::exit(1);
        }),
    );

   
    let shred_collector = ShredCollectorService::new(
        node.clone(),
        blockstore,
        repair_socket,
        cluster_info,
        start_slot,
        exit.clone(),
    );


    info!("Started shred collector service");

    loop {
        if exit.load(Ordering::Relaxed) {
            break;
        }
        sleep(Duration::from_millis(100));
    }

    exit.store(true, Ordering::Relaxed);
    shred_collector.join().unwrap();

}
