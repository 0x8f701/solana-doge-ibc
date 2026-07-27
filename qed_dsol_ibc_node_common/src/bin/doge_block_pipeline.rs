use std::{path::PathBuf, time::Duration};

use anyhow::Context;
use clap::Parser;
use doge_bridge_client::constants::{
    DOGE_BRIDGE_PROGRAM_ID, PENDING_MINT_BUFFER_BUILDER_PROGRAM_ID,
    TXO_BUFFER_BUILDER_PROGRAM_ID,
};
use qed_dsol_ibc_node_common::block_pipeline::{
    recover_checkpoint_from_proof_archive, DogeNetworkProfile, BlockPipeline,
    BlockPipelineConfig,
};
use qed_dsol_ibc_node_common::proof_queue::{
    ProverBackendKind, DEFAULT_QUEUE_PREFIX,
};
use solana_sdk::pubkey::Pubkey;

#[derive(Debug, Clone, clap::Subcommand)]
enum Command {
    Run,
    Recover {
        #[arg(long)]
        proof_archive_dir: PathBuf,
    },
}

#[derive(Debug, Parser)]
#[command(about = "Poll finalized Dogecoin blocks, prove SP1 transitions, and submit Solana block updates")]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,

    #[arg(long, env = "DOGE_NETWORK", value_enum)]
    network: DogeNetworkProfile,

    #[arg(long, env = "DOGE_ELECTRS_URL", default_value = "http://127.0.0.1:3002")]
    electrs_url: String,

    #[arg(long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379")]
    redis_url: String,

    #[arg(long, env = "DOGE_BLOCK_SENDER_URL", default_value = "http://127.0.0.1:3000")]
    sender_url: String,

    #[arg(long, env = "DOGE_BLOCK_SENDER_TOKEN")]
    sender_token: String,

    #[arg(long, env = "SP1_GEN_PROOF_PATH", default_value = "../psy-bridge-sp1/target/release/gen-proof")]
    gen_proof_path: PathBuf,

    #[arg(long, env = "SP1_BLOCK_ELF_PATH")]
    block_elf_path: Option<PathBuf>,

    #[arg(long, env = "SP1_BLOCK_VK_HASH")]
    expected_vk_hash: Option<String>,

    #[arg(
        long,
        env = "DOGE_BLOCK_PROOF_ARCHIVE_DIR",
        default_value = "/tmp/psy-doge-block-proof-archive"
    )]
    proof_archive_dir: PathBuf,

    #[arg(long, env = "DOGE_POLL_INTERVAL_MS", default_value_t = 1_000)]
    poll_interval_ms: u64,

    #[arg(long, env = "DOGE_REDIS_SEED", default_value_t = 1_337)]
    redis_seed: u64,
    #[arg(long, env = "DOGE_PROVER_BACKEND", value_enum, default_value_t = ProverBackendKind::Local)]
    prover_backend: ProverBackendKind,

    #[arg(long, env = "DOGE_PROOF_PREPARE_WINDOW", default_value_t = 1)]
    proof_prepare_window: usize,

    #[arg(long, env = "DOGE_PROOF_QUEUE_PREFIX", default_value = DEFAULT_QUEUE_PREFIX)]
    proof_queue_prefix: String,

    #[arg(long, env = "DOGE_PROOF_WAIT_INTERVAL_MS", default_value_t = 1_000)]
    proof_wait_interval_ms: u64,

    #[arg(long, env = "DOGE_START_HEIGHT")]
    start_height: Option<u32>,

    #[arg(long, env = "DOGE_OLD_STATE_DIR")]
    old_state_dir: Option<PathBuf>,

    #[arg(long, env = "DOGE_WITNESS_DIR")]
    witness_dir: Option<PathBuf>,

    #[arg(long, env = "DOGE_CUSTODY_SCRIPT_CONFIG")]
    custody_script_config: String,

    #[arg(long, env = "DOGE_RECIPIENT_ATAS", value_delimiter = ',')]
    recipient_atas: Vec<String>,

    #[arg(long, env = "DOGE_REQUIRED_CONFIRMATIONS", default_value_t = 1)]
    required_confirmations: u32,

    #[arg(long, env = "DOGE_BRIDGE_CONFIG")]
    config_params: String,

    #[arg(long, env = "DOGE_DEPOSIT_EVIDENCE")]
    deposit_evidence_path: Option<PathBuf>,

    #[arg(long, env = "DOGE_INITIAL_HEADER")]
    initial_header: String,

    #[arg(long, env = "SOLANA_RPC_URL", default_value = "http://127.0.0.1:8899")]
    solana_rpc_url: String,

    #[arg(long, env = "DOGE_OPERATOR_KEYPAIR")]
    operator_keypair: PathBuf,

    #[arg(long, env = "DOGE_PAYER_KEYPAIR")]
    payer_keypair: PathBuf,

    #[arg(long, env = "DOGE_MINT")]
    doge_mint: Pubkey,

    #[arg(long, env = "DOGE_BRIDGE_PROGRAM", default_value_t = DOGE_BRIDGE_PROGRAM_ID)]
    bridge_program: Pubkey,

    #[arg(long, env = "PENDING_MINT_BUFFER_PROGRAM", default_value_t = PENDING_MINT_BUFFER_BUILDER_PROGRAM_ID)]
    pending_mint_program: Pubkey,

    #[arg(long, env = "TXO_BUFFER_PROGRAM", default_value_t = TXO_BUFFER_BUILDER_PROGRAM_ID)]
    txo_buffer_program: Pubkey,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let block_elf_path = args
        .block_elf_path
        .unwrap_or_else(|| args.network.default_block_elf_path());
    let expected_vk_hash = args
        .expected_vk_hash
        .as_deref()
        .map(|value| read_fixed::<32>(value, "SP1 block VK hash"))
        .transpose()?
        .unwrap_or_else(|| args.network.default_vk_hash());
    let config = BlockPipelineConfig {
        network: args.network,
        electrs_url: args.electrs_url,
        redis_url: args.redis_url,
        sender_url: args.sender_url,
        sender_bearer_token: args.sender_token,
        gen_proof_path: args.gen_proof_path,
        block_elf_path,
        expected_vk_hash,
        evidence_dir: args.proof_archive_dir,
        poll_interval: Duration::from_millis(args.poll_interval_ms),
        redis_seed: args.redis_seed,
        prover_backend: args.prover_backend,
        proof_prepare_window: args.proof_prepare_window,
        proof_queue_prefix: args.proof_queue_prefix,
        proof_wait_interval: Duration::from_millis(args.proof_wait_interval_ms),
        start_height: args.start_height,
        custody_script_config: read_fixed::<32>(
            &args.custody_script_config,
            "custody script config",
        )?,
        recipient_atas: args
            .recipient_atas
            .iter()
            .map(|value| read_fixed::<32>(value, "recipient ATA"))
            .collect::<anyhow::Result<Vec<_>>>()?,
        required_confirmations: args.required_confirmations,
        old_state_dir: args.old_state_dir,
        witness_dir: args.witness_dir,
        config_params: read_fixed::<48>(&args.config_params, "bridge config")?,
        initial_header: read_fixed::<320>(&args.initial_header, "initial bridge header")?,
        deposit_evidence_path: args.deposit_evidence_path,
        solana_rpc_url: args.solana_rpc_url,
        operator_keypair: args.operator_keypair,
        payer_keypair: args.payer_keypair,
        doge_mint: args.doge_mint,
        bridge_program: args.bridge_program,
        pending_mint_program: args.pending_mint_program,
        txo_buffer_program: args.txo_buffer_program,
    };

    match args.command.unwrap_or(Command::Run) {
        Command::Run => BlockPipeline::initialize(config).await?.run().await,
        Command::Recover { proof_archive_dir } => {
            let height = recover_checkpoint_from_proof_archive(&config, &proof_archive_dir).await?;
            println!("recovered checkpoint height {height}");
            Ok(())
        }
    }
}

fn read_fixed<const N: usize>(value: &str, name: &str) -> anyhow::Result<[u8; N]> {
    let text = if let Some(path) = value.strip_prefix('@') {
        std::fs::read_to_string(path)
            .with_context(|| format!("failed to read {name} from {path}"))?
    } else if std::path::Path::new(value).is_file() {
        std::fs::read_to_string(value)
            .with_context(|| format!("failed to read {name} from {value}"))?
    } else {
        value.to_owned()
    };
    let normalized: String = text
        .trim()
        .strip_prefix("0x")
        .unwrap_or(text.trim())
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect();
    let bytes = hex::decode(&normalized).with_context(|| format!("invalid {name} hex"))?;
    if bytes.len() != N {
        anyhow::bail!("{name} must decode to {N} bytes, got {}", bytes.len());
    }
    Ok(bytes.try_into().expect("length was checked"))
}
