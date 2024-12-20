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
        gossip_service::GossipService,
        contact_info::ContactInfo,
    },
    solana_net_utils::{VALIDATOR_PORT_RANGE, get_public_ip_addr, parse_host},
    solana_sdk::{
        pubkey::Pubkey,
        signature::Signer,
    },
    solana_streamer::socket::SocketAddrSpace,
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs},
        process::exit,
        sync::{atomic::AtomicBool, Arc},
        thread::sleep,
        time::Duration,
    },
};

fn parse_entrypoint(entrypoint: &str) -> SocketAddr {
    if let Ok(addr) = entrypoint.to_socket_addrs() {
        if let Some(addr) = addr.filter(|addr| addr.port() != 0).next() {
            return addr;
        }
    }
    eprintln!("Failed to parse entrypoint address: {}", entrypoint);
    exit(1);
}

fn main() {
    solana_logger::setup_with_default("info");

    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(solana_version::version!())
        .arg(
            Arg::with_name("identity")
                .short("i")
                .long("identity")
                .value_name("PATH")
                .takes_value(true)
                .validator(is_keypair)
                .help("Path to validator identity keypair"),
        )
        .arg(
            Arg::with_name("gossip_port")
                .long("gossip-port")
                .value_name("PORT")
                .takes_value(true)
                .validator(is_port)
                .help("Gossip port number"),
        )
        .arg(
            Arg::with_name("gossip_host")
                .long("gossip-host")
                .value_name("HOST")
                .takes_value(true)
                .help("Gossip DNS name or IP address for the node to advertise in gossip"),
        )
        .arg(
            Arg::with_name("entrypoint")
                .short("n")
                .long("entrypoint")
                .value_name("HOST:PORT")
                .takes_value(true)
                .multiple(true)
                .help("Rendezvous with the cluster at this gossip entrypoint"),
        )
        .get_matches();

    let identity_keypair = keypair_of(&matches, "identity").unwrap_or_else(|| {
        clap::Error::with_description(
            "The --identity <KEYPAIR> argument is required",
            clap::ErrorKind::ArgumentNotFound,
        )
        .exit();
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
                exit(1);
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
                    eprintln!("Unable to determine the validator's public IP address");
                    exit(1);
                })
            } else {
                IpAddr::V4(Ipv4Addr::LOCALHOST)
            }
        });

    let gossip_addr = SocketAddr::new(
        gossip_host,
        value_t!(matches, "gossip_port", u16).unwrap_or_else(|_| {
            solana_net_utils::find_available_port_in_range(bind_address, (0, 1)).unwrap_or_else(
                |err| {
                    eprintln!("Unable to find an available gossip port: {err}");
                    exit(1);
                },
            )
        }),
    );

    info!("Gossip address: {}", gossip_addr);

    let mut node = Node::new_single_bind(
        &identity_keypair.pubkey(),
        &gossip_addr,
        VALIDATOR_PORT_RANGE,
        bind_address,
    );

    // Remove unused ports since we only need gossip and repair
    node.info.remove_tpu();
    node.info.remove_tpu_forwards();
    node.info.remove_tvu();
    node.info.remove_serve_repair();
    node.sockets.ip_echo = None;

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
        node.sockets.gossip,
        None,
        true,
        None,
        exit.clone(),
    );

    info!("Gossip Service started on port {}", gossip_addr.port());
    info!("Node identity: {}", node.info.pubkey());

    loop {
        let nodes = cluster_info.all_peers();
        info!("Connected to {} peers", nodes.len());
        for (node, _) in nodes {
            info!("Peer: {}", node.pubkey());
        }
        sleep(Duration::from_secs(2));
    }
}
