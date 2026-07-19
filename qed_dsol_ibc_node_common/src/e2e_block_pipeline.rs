use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use borsh::BorshDeserialize;
use doge_bridge_client::{
    BridgeApi, BridgeClient, BridgeClientConfigBuilder, PendingMint, PsyBridgeHeader,
};
use doge_light_client::{
    chain_state::QEDDogeChainStateCore,
    common_types::QHash256,
    constants::{DogeNetworkConfig, DogeRegTestConfig, DogeTestNetConfig},
    core_data::{QDogeBlock, QDogeBlockHeader},
    hash::sha256_impl::{
        hash_impl_btc_hash256_two_to_one_bytes, hash_impl_sha256_bytes,
        hash_impl_sha256_two_to_one_bytes,
    },
    init_params::InitBlockDataIBC,
};
use fred::{
    prelude::{ClientLike, Config, KeysInterface, ReconnectPolicy},
    types::Builder,
};
use psy_doge_bridge_helper::{
    block_transition::prover_guest::prover_guest_verify_block_transition_detailed,
    claim::{
        auto_claim_deposits_tree::{
            constants::AUTO_CLAIM_DEPOSITS_TREE_HEIGHT,
            pending_mints_buffer_builder::PendingMintsGroupsBuilder,
        },
        block_tx_output_tree::{
            TXO_BLOCK_FULL_MERKLE_TREE_HEIGHT, TXO_TREE_INDEX_BITS_BLOCK_NUM_LENGTH,
        },
        transition::{
            transition_builder::{calcuate_fee, hash_deposit_leaf, BlockTransitionBuilder},
            validator::{
                block_witness::{
                    PsyBridgeClaimBlockWitness, PsyBridgeClaimBlockWitnessHeader,
                    PsyBridgeClaimBlockWitnessVerifyResult,
                },
                tx_witness::{PsyBridgeClaimBlockTransactionWitness, PsyBridgeClaimDepositItem},
            },
        },
    },
    constants::{PSY_DOGE_BRIDGE_BLOCK_HASH_CACHE_SIZE, PSY_DOGE_BRIDGE_BLOCK_TREE_HEIGHT},
    data::core::{PsyDogeBridgeIncomingBlockWitness, PsyDogeBridgeState},
    tx_template::{
        get_manager_custody_output_script, get_manager_custody_redeem_script, CustodyScriptConfig,
        LocalRegtestManagerCustody, ManagerCustodyProfile, OfficialTestnetManagerCustody,
        MANAGER_CUSTODY_REDEEM_SCRIPT_SIZE,
    },
    utils::sha256_zero_hashes::SHA256_ZERO_HASHES,
};
use serde::{Deserialize, Serialize};
use solana_client::nonblocking::rpc_client::RpcClient as SolanaRpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{read_keypair_file, Keypair, Signature, Signer},
};
use speedy::{Readable, Writable};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::task::JoinHandle;

use crate::{
    doge_link_rpc_async::DogeLinkElectrsAsyncClient,
    sol_submitter::{BlockUpdateRequestBody, SolSubmitterClient},
};

const PROOF_PATH: &str = "/tmp/bridge-block-transition-proof.bin";
const PUBLIC_VALUES_PATH: &str = "/tmp/bridge-block-transition-pubvals.bin";
const HEADER_SIZE: usize = 320;
const CONFIG_SIZE: usize = 48;
const CUSTODY_SCRIPT_CONFIG_SIZE: usize = 32;
const PROOF_SIZE: usize = 356;
const PUBLIC_VALUES_SIZE: usize = 32;
const CHECKPOINT_PREFIX: &str = "PDOGE-E2E-BLOCK-CHECKPOINT-V3";
const EVIDENCE_SCHEMA_VERSION: u32 = 2;
pub const REGTEST_BLOCK_VK_HASH: [u8; 32] =
    hex_literal::hex!("001fa018c35d88136afe0e92bc9afe33ba94ca5dcd9156147adf004c7810e199");
pub const TESTNET_BLOCK_VK_HASH: [u8; 32] =
    hex_literal::hex!("00b25e2fe5866751a38e5ca4d975b30b4187f3e0528a06dc86edc6e9a8b9cc02");

const SOL_TIP_BLOCK_HASH: std::ops::Range<usize> = 0..32;
const SOL_TIP_BLOCK_MERKLE_ROOT: std::ops::Range<usize> = 32..64;
const SOL_TIP_BLOCK_TIME: std::ops::Range<usize> = 64..68;
const SOL_TIP_BLOCK_HEIGHT: std::ops::Range<usize> = 68..72;
const SOL_FINALIZED_BLOCK_HASH: std::ops::Range<usize> = 72..104;
const SOL_FINALIZED_BLOCK_MERKLE_ROOT: std::ops::Range<usize> = 104..136;
const SOL_FINALIZED_PENDING_MINTS_HASH: std::ops::Range<usize> = 136..168;
const SOL_FINALIZED_TXO_LIST_HASH: std::ops::Range<usize> = 168..200;
const SOL_FINALIZED_AUTO_CLAIMED_TXO_ROOT: std::ops::Range<usize> = 200..232;
const SOL_FINALIZED_AUTO_CLAIMED_DEPOSITS_ROOT: std::ops::Range<usize> = 232..264;
const SOL_FINALIZED_AUTO_CLAIMED_NEXT_INDEX: std::ops::Range<usize> = 264..268;
const SOL_FINALIZED_BLOCK_HEIGHT: std::ops::Range<usize> = 268..272;
const SOL_BRIDGE_STATE_HASH: std::ops::Range<usize> = 272..304;
const SOL_LAST_ROLLBACK_AT_SECS: std::ops::Range<usize> = 304..308;
const SOL_PAUSED_UNTIL_SECS: std::ops::Range<usize> = 308..312;
const SOL_TOTAL_FINALIZED_FEES: std::ops::Range<usize> = 312..320;

type BridgeState =
    QEDDogeChainStateCore<PSY_DOGE_BRIDGE_BLOCK_HASH_CACHE_SIZE, PSY_DOGE_BRIDGE_BLOCK_TREE_HEIGHT>;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum DogeNetworkProfile {
    #[default]
    Regtest,
    Testnet,
}

impl DogeNetworkProfile {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Regtest => "regtest",
            Self::Testnet => "testnet",
        }
    }

    pub const fn default_vk_hash(self) -> [u8; 32] {
        match self {
            Self::Regtest => REGTEST_BLOCK_VK_HASH,
            Self::Testnet => TESTNET_BLOCK_VK_HASH,
        }
    }

    pub fn default_block_elf_path(self) -> PathBuf {
        let name = match self {
            Self::Regtest => "block-transition",
            Self::Testnet => "block-transition-testnet",
        };
        PathBuf::from(
            "../psy-bridge-sp1/target/elf-compilation/riscv64im-succinct-zkvm-elf/release",
        )
        .join(name)
    }

    /// Expected on-chain custodian wallet config hash for the selected
    /// network's manager custody profile and the configured emitter PDA.
    pub fn custodian_hash(self, emitter_bridge_pda: [u8; 32]) -> QHash256 {
        let config = CustodyScriptConfig::new(emitter_bridge_pda);
        match self {
            Self::Regtest => config.hash::<LocalRegtestManagerCustody>(),
            Self::Testnet => config.hash::<OfficialTestnetManagerCustody>(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct E2EBlockPipelineConfig {
    pub network: DogeNetworkProfile,
    pub electrs_url: String,
    pub redis_url: String,
    pub sender_url: String,
    pub sender_bearer_token: String,
    pub gen_proof_path: PathBuf,
    pub block_elf_path: PathBuf,
    pub expected_vk_hash: [u8; 32],
    pub evidence_dir: PathBuf,
    pub poll_interval: Duration,
    pub redis_seed: u64,
    pub start_height: Option<u32>,
    pub old_state_dir: Option<PathBuf>,
    pub witness_dir: Option<PathBuf>,
    pub custody_script_config: [u8; CUSTODY_SCRIPT_CONFIG_SIZE],
    pub recipient_atas: Vec<[u8; 32]>,
    pub required_confirmations: u32,
    pub config_params: [u8; CONFIG_SIZE],
    pub initial_header: [u8; HEADER_SIZE],
    pub deposit_evidence_path: Option<PathBuf>,
    pub solana_rpc_url: String,
    pub operator_keypair: PathBuf,
    pub payer_keypair: PathBuf,
    pub doge_mint: Pubkey,
    pub bridge_program: Pubkey,
    pub pending_mint_program: Pubkey,
    pub txo_buffer_program: Pubkey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MerkleFrontier {
    index: u32,
    value_hex: String,
    siblings_hex: Vec<String>,
}

impl MerkleFrontier {
    fn new(index: u32, value: QHash256, siblings: &[QHash256]) -> Self {
        Self {
            index,
            value_hex: hex::encode(value),
            siblings_hex: siblings.iter().map(hex::encode).collect(),
        }
    }

    fn decode(
        &self,
        expected_height: usize,
        name: &str,
    ) -> anyhow::Result<(QHash256, Vec<QHash256>)> {
        let value = decode_hash(&self.value_hex, &format!("{name} value"))?;
        if self.siblings_hex.len() != expected_height {
            anyhow::bail!(
                "{name} must contain {expected_height} siblings, got {}",
                self.siblings_hex.len()
            );
        }
        let siblings = self
            .siblings_hex
            .iter()
            .enumerate()
            .map(|(index, value)| decode_hash(value, &format!("{name} sibling {index}")))
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok((value, siblings))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingMintPayload {
    recipient: [u8; 32],
    amount: u64,
}

impl PendingMintPayload {
    fn to_bridge_mint(&self) -> PendingMint {
        PendingMint {
            recipient: self.recipient,
            amount: self.amount,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BlockBufferCommitment {
    pending_mints_hash_hex: String,
    txo_output_list_hash_hex: String,
    pending_mints: Vec<PendingMintPayload>,
    txo_indices: Vec<u32>,
    deposit_count: u32,
    minted_amount_sats: u64,
    auto_claim_start_index: u32,
    auto_claim_end_index: u32,
    fees_collected: u64,
}

impl BlockBufferCommitment {
    fn from_evaluation(evaluation: &ClaimEvaluation) -> Self {
        Self {
            pending_mints_hash_hex: hex::encode(evaluation.pending_mints_hash),
            txo_output_list_hash_hex: hex::encode(evaluation.txo_output_list_hash),
            pending_mints: evaluation.pending_mints.clone(),
            txo_indices: evaluation.txo_indices.clone(),
            deposit_count: evaluation.deposit_count,
            minted_amount_sats: evaluation.minted_amount_sats,
            auto_claim_start_index: evaluation.transition.start_auto_claimed_deposits_index,
            auto_claim_end_index: evaluation.transition.end_auto_claimed_deposits_index,
            fees_collected: evaluation.transition.fees_collected,
        }
    }

    fn empty() -> anyhow::Result<Self> {
        Ok(Self {
            pending_mints_hash_hex: hex::encode(
                PendingMintsGroupsBuilder::new_with_hint(0).finalize()?,
            ),
            txo_output_list_hash_hex: hex::encode(hash_impl_sha256_bytes(&[])),
            pending_mints: Vec::new(),
            txo_indices: Vec::new(),
            deposit_count: 0,
            minted_amount_sats: 0,
            auto_claim_start_index: 0,
            auto_claim_end_index: 0,
            fees_collected: 0,
        })
    }

    fn pending_mints_hash(&self) -> anyhow::Result<QHash256> {
        decode_hash(&self.pending_mints_hash_hex, "pending mints hash")
    }

    fn txo_output_list_hash(&self) -> anyhow::Result<QHash256> {
        decode_hash(&self.txo_output_list_hash_hex, "TXO output list hash")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PipelineCheckpoint {
    height: u32,
    state_hex: String,
    header_hex: String,
    claim_frontier: MerkleFrontier,
    txo_block_frontier: MerkleFrontier,
    #[serde(default)]
    claim_history: Vec<QHash256>,
    #[serde(default)]
    txo_block_history: Vec<QHash256>,
    pending_finalization: BTreeMap<u32, BlockBufferCommitment>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceArtifact {
    pub path: String,
    pub sha256: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockProofEvidence {
    pub schema_version: u32,
    pub status: String,
    pub height: u32,
    pub block_hash: String,
    pub finalized_source_height: u32,
    pub source_witness_height: u32,
    pub witness_deposit_count: u32,
    pub witness_minted_amount_sats: u64,
    pub deposit_count: u32,
    pub minted_amount_sats: u64,
    pub auto_claim_start_index: u32,
    pub auto_claim_end_index: u32,
    pub fees_collected: u64,
    pub content_sha256: String,
    /// SHA-256 of this manifest serialized with this field set to an empty string.
    pub manifest_sha256: String,
    pub evidence_dir: String,
    pub manifest_path: String,
    pub latest_path: String,
    pub idempotency_key: String,
    pub mint_buffer: Option<String>,
    pub mint_buffer_bump: Option<u8>,
    pub txo_buffer: Option<String>,
    pub txo_buffer_bump: Option<u8>,
    pub buffer_upload_completed: bool,
    pub submission_signature: Option<String>,
    pub mint_group_signatures: Vec<String>,
    pub mint_groups_processed: usize,
    pub total_mints_processed: usize,
    pub vk_hash: String,
    pub elf_sha256: String,
    pub artifacts: BTreeMap<String, EvidenceArtifact>,
}

struct ClaimEvaluation {
    transition: PsyBridgeClaimBlockWitnessVerifyResult,
    pending_mints_hash: QHash256,
    txo_output_list_hash: QHash256,
    pending_mints: Vec<PendingMintPayload>,
    txo_indices: Vec<u32>,
    block_txo_root: QHash256,
    deposit_leaf_hashes: Vec<QHash256>,
    deposit_count: u32,
    minted_amount_sats: u64,
}

struct ProverOutput {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    vk_hash: [u8; 32],
}

#[derive(Debug)]
struct ProverRequestError(String);

impl std::fmt::Display for ProverRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ProverRequestError {}

#[derive(Debug, Deserialize)]
struct ProverIdentityResponse {
    kind: String,
    network: String,
    block_elf_path: String,
    block_elf_sha256: String,
    vkey_hash: String,
}

#[derive(Debug, Deserialize)]
struct ProverProofResponse {
    kind: String,
    request_id: String,
    ok: bool,
    network: Option<String>,
    block_elf_path: Option<String>,
    block_elf_sha256: Option<String>,
    vkey_hash: Option<String>,
    proof_path: Option<String>,
    proof_size: Option<usize>,
    proof_bytes: Option<String>,
    public_values_path: Option<String>,
    public_values_size: Option<usize>,
    public_values: Option<String>,
    error: Option<String>,
}

#[derive(Serialize)]
struct ProverProofRequest<'a> {
    request_id: &'a str,
    old_state: String,
    witness: String,
    custody_script_config: String,
    required_confirmations: u32,
    flat_fee: u64,
    fee_num: u64,
    fee_den: u64,
    old_header: String,
    new_header: String,
    config_params: String,
}

const MAX_PROVER_DIAGNOSTICS_BYTES: usize = 256 * 1024;

struct ProverDaemon {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    stderr_task: Option<JoinHandle<std::io::Result<()>>>,
    stderr: Arc<tokio::sync::Mutex<Vec<u8>>>,
    vk_hash: [u8; 32],
}

impl ProverDaemon {
    async fn start(config: &E2EBlockPipelineConfig) -> anyhow::Result<Self> {
        let mut command = Command::new(&config.gen_proof_path);
        command
            .arg("--network")
            .arg(config.network.as_str())
            .arg("--daemon")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("gen-proof daemon stdin was not piped"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("gen-proof daemon stdout was not piped"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow::anyhow!("gen-proof daemon stderr was not piped"))?;
        let stderr_bytes = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let task_stderr_bytes = Arc::clone(&stderr_bytes);
        let stderr_task = tokio::spawn(async move {
            let mut stderr = stderr;
            let mut chunk = [0u8; 8192];
            loop {
                let bytes_read = tokio::io::AsyncReadExt::read(&mut stderr, &mut chunk).await?;
                if bytes_read == 0 {
                    break;
                }
                let mut diagnostics = task_stderr_bytes.lock().await;
                diagnostics.extend_from_slice(&chunk[..bytes_read]);
                if diagnostics.len() > MAX_PROVER_DIAGNOSTICS_BYTES {
                    let keep_from = diagnostics.len() - MAX_PROVER_DIAGNOSTICS_BYTES;
                    diagnostics.drain(..keep_from);
                }
            }
            Ok(())
        });
        let mut daemon = Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            stderr: stderr_bytes,
            stderr_task: Some(stderr_task),
            vk_hash: [0; 32],
        };
        let identity_line = match daemon.read_response_line().await {
            Ok(line) => line,
            Err(error) => {
                let _ = daemon.child.kill().await;
                let _ = daemon.child.wait().await;
                let stderr = daemon.finish_stderr().await;
                return Err(anyhow::anyhow!(
                    "gen-proof daemon startup failed: {error:#}; stderr:\n{}",
                    String::from_utf8_lossy(&stderr)
                ));
            }
        };
        let identity: ProverIdentityResponse = match serde_json::from_slice(&identity_line) {
            Ok(identity) => identity,
            Err(error) => {
                let _ = daemon.child.kill().await;
                let _ = daemon.child.wait().await;
                let stderr = daemon.finish_stderr().await;
                anyhow::bail!(
                    "malformed gen-proof identity response: {error}; stderr:\n{}",
                    String::from_utf8_lossy(&stderr)
                );
            }
        };
        match validate_prover_identity(config, &identity).await {
            Ok(vk_hash) => daemon.vk_hash = vk_hash,
            Err(error) => {
                let _ = daemon.child.kill().await;
                let _ = daemon.child.wait().await;
                let stderr = daemon.finish_stderr().await;
                return Err(anyhow::anyhow!(
                    "gen-proof daemon identity validation failed: {error:#}; stderr:\n{}",
                    String::from_utf8_lossy(&stderr)
                ));
            }
        }
        Ok(daemon)
    }

    async fn prove(
        &mut self,
        config: &E2EBlockPipelineConfig,
        request_id: &str,
        old_state: &[u8],
        witness: &[u8],
        old_header: &[u8; HEADER_SIZE],
        new_header: &[u8; HEADER_SIZE],
    ) -> anyhow::Result<ProverOutput> {
        let request = ProverProofRequest {
            request_id,
            old_state: hex::encode(old_state),
            witness: hex::encode(witness),
            custody_script_config: hex::encode(config.custody_script_config),
            required_confirmations: config.required_confirmations,
            flat_fee: deposit_flat_fee(&config.config_params),
            fee_num: deposit_fee_numerator(&config.config_params),
            fee_den: deposit_fee_denominator(&config.config_params),
            old_header: hex::encode(old_header),
            new_header: hex::encode(new_header),
            config_params: hex::encode(config.config_params),
        };
        let mut request_line = serde_json::to_vec(&request)?;
        request_line.push(b'\n');
        self.stdin.write_all(&request_line).await?;
        self.stdin.flush().await?;

        let response_line = self.read_response_line().await?;
        let response: ProverProofResponse = serde_json::from_slice(&response_line)
            .map_err(|error| anyhow::anyhow!("malformed gen-proof response: {error}"))?;
        if response.kind != "proof" {
            anyhow::bail!(
                "gen-proof response kind was '{}', expected 'proof'",
                response.kind
            );
        }
        if response.request_id != request_id {
            anyhow::bail!(
                "gen-proof response request id '{}' did not match '{request_id}'",
                response.request_id
            );
        }
        if !response.ok {
            return Err(anyhow::Error::new(ProverRequestError(format!(
                "gen-proof request {request_id} failed: {}",
                response.error.as_deref().unwrap_or("missing error message")
            ))));
        }
        validate_proof_response(config, self.vk_hash, &response)?;
        let proof = decode_required_response_bytes(response.proof_bytes.as_deref(), "proof_bytes")?;
        let public_values =
            decode_required_response_bytes(response.public_values.as_deref(), "public_values")?;
        if response.proof_size != Some(proof.len()) {
            anyhow::bail!(
                "gen-proof proof_size {:?} did not match {} returned bytes",
                response.proof_size,
                proof.len()
            );
        }
        if response.public_values_size != Some(public_values.len()) {
            anyhow::bail!(
                "gen-proof public_values_size {:?} did not match {} returned bytes",
                response.public_values_size,
                public_values.len()
            );
        }
        tokio::fs::write(PROOF_PATH, proof).await?;
        tokio::fs::write(PUBLIC_VALUES_PATH, public_values).await?;
        let stderr = self.take_stderr().await;
        Ok(ProverOutput {
            stdout: response_line,
            stderr,
            vk_hash: self.vk_hash,
        })
    }

    async fn read_response_line(&mut self) -> anyhow::Result<Vec<u8>> {
        loop {
            let mut line = Vec::new();
            let bytes_read = tokio::select! {
                biased;
                read_result = self.stdout.read_until(b'\n', &mut line) => read_result?,
                status_result = self.child.wait() => {
                    let status = status_result?;
                    let stderr = self.finish_stderr().await;
                    anyhow::bail!(
                        "gen-proof daemon exited with {status} before returning a response; stderr:\n{}",
                        String::from_utf8_lossy(&stderr)
                    );
                }
            };
            if bytes_read == 0 {
                let status = self.child.wait().await?;
                let stderr = self.finish_stderr().await;
                anyhow::bail!(
                    "gen-proof daemon closed stdout with {status}; stderr:\n{}",
                    String::from_utf8_lossy(&stderr)
                );
            }
            while matches!(line.last(), Some(b'\n' | b'\r')) {
                line.pop();
            }
            if line.is_empty() {
                continue;
            }
            if line.starts_with(b"{") {
                return Ok(line);
            }
            let mut diagnostics = self.stderr.lock().await;
            diagnostics.extend_from_slice(&line);
            diagnostics.push(b'\n');
            if diagnostics.len() > MAX_PROVER_DIAGNOSTICS_BYTES {
                let keep_from = diagnostics.len() - MAX_PROVER_DIAGNOSTICS_BYTES;
                diagnostics.drain(..keep_from);
            }
        }
    }

    async fn take_stderr(&self) -> Vec<u8> {
        let mut stderr = self.stderr.lock().await;
        std::mem::take(&mut *stderr)
    }

    async fn finish_stderr(&mut self) -> Vec<u8> {
        if let Some(task) = self.stderr_task.take() {
            let _ = task.await;
        }
        self.take_stderr().await
    }

    async fn terminate(mut self) {
        drop(self.stdin);
        match tokio::time::timeout(Duration::from_secs(5), self.child.wait()).await {
            Ok(Ok(_)) => {}
            _ => {
                let _ = self.child.kill().await;
            }
        }
        if let Some(task) = self.stderr_task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

enum ProverProcess {
    Daemon(ProverDaemon),
    Stopped,
}

#[derive(Serialize)]
struct ProverInputsEvidence {
    schema_version: u32,
    height: u32,
    block_hash: String,
    custody_script_config: String,
    recipient_atas: Vec<String>,
    required_confirmations: u32,
    flat_fee: u64,
    fee_num: u64,
    fee_den: u64,
    config_params: String,
    custodian_hash: String,
    expected_vk_hash: String,
    gen_proof_path: String,
    block_elf_source_path: String,
    old_state_sha256: String,
    witness_sha256: String,
    old_header_sha256: String,
    new_header_sha256: String,
}

struct UploadedBuffers {
    mint_buffer: Pubkey,
    mint_buffer_bump: u8,
    txo_buffer: Pubkey,
    txo_buffer_bump: u8,
}

struct MintProcessingEvidence {
    signatures: Vec<String>,
    groups_processed: usize,
    total_mints_processed: usize,
}

pub struct E2EBlockPipeline {
    config: E2EBlockPipelineConfig,
    block_rpc: DogeLinkElectrsAsyncClient,
    sender: SolSubmitterClient,
    bridge_client: BridgeClient,
    redis: fred::prelude::Pool,
    checkpoint_key: String,
    checkpoint: PipelineCheckpoint,
    prover: ProverProcess,
}

impl E2EBlockPipeline {
    pub async fn initialize(config: E2EBlockPipelineConfig) -> anyhow::Result<Self> {
        validate_config(&config)?;
        let redis_config = Config::from_url(&config.redis_url)?;
        let redis = Builder::from_config(redis_config)
            .with_connection_config(|connection| {
                connection.connection_timeout = Duration::from_secs(10);
            })
            .set_policy(ReconnectPolicy::new_exponential(0, 100, 30_000, 2))
            .build_pool(2)?;
        redis.init().await?;
        let operator = read_pipeline_keypair(&config.operator_keypair, "operator")?;
        let operator_pubkey = operator.pubkey();
        let payer = read_pipeline_keypair(&config.payer_keypair, "payer")?;
        let (bridge_state_pda, _) =
            Pubkey::find_program_address(&[b"bridge_state"], &config.bridge_program);
        let bridge_client = BridgeClient::with_config(
            BridgeClientConfigBuilder::new()
                .rpc_url(config.solana_rpc_url.clone())
                .bridge_state_pda(bridge_state_pda)
                .operator(operator)
                .payer(payer)
                .doge_mint(config.doge_mint)
                .program_id(config.bridge_program)
                .pending_mint_program_id(config.pending_mint_program)
                .txo_buffer_program_id(config.txo_buffer_program)
                .wormhole_core_program_id(config.bridge_program)
                .wormhole_shim_program_id(config.bridge_program)
                .build()?,
        )?;
        let chain_state = bridge_client.get_current_bridge_state().await?;
        if chain_state.access_control.operator_pubkey != operator_pubkey.to_bytes() {
            anyhow::bail!(
                "configured operator {operator_pubkey} does not match on-chain operator {}",
                Pubkey::new_from_array(chain_state.access_control.operator_pubkey)
            );
        }
        let expected_custodian_hash = config.network.custodian_hash(config.custody_script_config);
        if chain_state.custodian_wallet_config_hash != expected_custodian_hash {
            anyhow::bail!(
                "on-chain custodian wallet config hash {} does not match manager custody script config hash {}",
                hex::encode(chain_state.custodian_wallet_config_hash),
                hex::encode(expected_custodian_hash),
            );
        }
        let chain_doge_mint = bridge_client.get_doge_mint().await?;
        if chain_doge_mint != config.doge_mint {
            anyhow::bail!(
                "configured DOGE mint {} does not match on-chain mint {chain_doge_mint}",
                config.doge_mint
            );
        }

        let checkpoint_key = format!(
            "{CHECKPOINT_PREFIX}-{}-{}",
            config.network.as_str(),
            config.redis_seed
        );
        let block_rpc = DogeLinkElectrsAsyncClient::new(config.electrs_url.clone());
        let checkpoint = match redis.get::<Option<String>, _>(&checkpoint_key).await? {
            Some(value) => serde_json::from_str(&value)?,
            None => initialize_checkpoint(&config, &block_rpc).await?,
        };
        validate_checkpoint(&checkpoint)?;
        assert_checkpoint_matches_chain(&checkpoint, &chain_state.bridge_header)?;

        let prover = ProverProcess::Daemon(ProverDaemon::start(&config).await?);
        eprintln!(
            "block pipeline started: network={} checkpoint={} sender={} electrs={}",
            config.network.as_str(),
            checkpoint.height,
            config.sender_url,
            config.electrs_url,
        );
        Ok(Self {
            sender: SolSubmitterClient::new_with_bearer_token(
                config.sender_url.clone(),
                config.sender_bearer_token.clone(),
            )?,
            bridge_client,
            block_rpc,
            redis,
            checkpoint_key,
            checkpoint,
            config,
            prover,
        })
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        #[cfg(unix)]
        let result = {
            let mut terminate = tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::terminate(),
            )?;
            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => break Ok(()),
                    _ = terminate.recv() => break Ok(()),
                    result = self.poll_once() => match result {
                        Err(error) => break Err(error),
                        Ok(_) => tokio::select! {
                            _ = tokio::signal::ctrl_c() => break Ok(()),
                            _ = terminate.recv() => break Ok(()),
                            _ = tokio::time::sleep(self.config.poll_interval) => {}
                        },
                    }
                }
            }
        };
        #[cfg(not(unix))]
        let result = loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => break Ok(()),
                result = self.poll_once() => match result {
                    Err(error) => break Err(error),
                    Ok(_) => tokio::select! {
                        _ = tokio::signal::ctrl_c() => break Ok(()),
                        _ = tokio::time::sleep(self.config.poll_interval) => {}
                    },
                }
            }
        };
        self.shutdown().await;
        result
    }

    async fn shutdown(&mut self) {
        let prover = std::mem::replace(&mut self.prover, ProverProcess::Stopped);
        if let ProverProcess::Daemon(prover) = prover {
            prover.terminate().await;
        }
    }

    pub async fn poll_once(&mut self) -> anyhow::Result<bool> {
        let electrs_tip = self.block_rpc.get_block_height().await?;
        let finalized_tip = electrs_tip.saturating_sub(self.config.required_confirmations);
        let next_height = self
            .checkpoint
            .height
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("checkpoint height overflow"))?;
        if next_height > finalized_tip {
            return Ok(false);
        }
        match self.config.network {
            DogeNetworkProfile::Regtest => {
                self.process_height::<DogeRegTestConfig, LocalRegtestManagerCustody>(next_height)
                    .await?
            }
            DogeNetworkProfile::Testnet => {
                self.process_height::<DogeTestNetConfig, OfficialTestnetManagerCustody>(
                    next_height,
                )
                .await?
            }
        }
        Ok(true)
    }

    async fn run_gen_proof(
        &mut self,
        height: u32,
        old_state: &[u8],
        witness: &[u8],
        old_header: &[u8; HEADER_SIZE],
        new_header: &[u8; HEADER_SIZE],
    ) -> anyhow::Result<ProverOutput> {
        if matches!(&self.prover, ProverProcess::Stopped) {
            anyhow::bail!("gen-proof daemon is stopped");
        }

        prepare_prover_request(&self.config, old_state, witness, old_header, new_header).await?;
        let request_id = format!("block-{height}");
        let first_result = match &mut self.prover {
            ProverProcess::Daemon(prover) => {
                prover
                    .prove(
                        &self.config,
                        &request_id,
                        old_state,
                        witness,
                        old_header,
                        new_header,
                    )
                    .await
            }
            ProverProcess::Stopped => unreachable!("checked above"),
        };
        let first_error = match first_result {
            Ok(output) => return Ok(output),
            Err(error) if error.downcast_ref::<ProverRequestError>().is_some() => {
                return Err(error);
            }
            Err(error) => error,
        };

        let old_process = std::mem::replace(&mut self.prover, ProverProcess::Stopped);
        if let ProverProcess::Daemon(prover) = old_process {
            prover.terminate().await;
        }
        let mut prover = ProverDaemon::start(&self.config).await.map_err(|restart_error| {
            anyhow::anyhow!(
                "gen-proof daemon request failed: {first_error:#}; restart/revalidation failed: {restart_error:#}"
            )
        })?;
        let retry_result = prover
            .prove(
                &self.config,
                &request_id,
                old_state,
                witness,
                old_header,
                new_header,
            )
            .await;
        match retry_result {
            Ok(output) => {
                self.prover = ProverProcess::Daemon(prover);
                Ok(output)
            }
            Err(retry_error) => {
                prover.terminate().await;
                Err(anyhow::anyhow!(
                    "gen-proof daemon request failed: {first_error:#}; retry after restart failed: {retry_error:#}"
                ))
            }
        }
    }

    async fn process_height<NC: DogeNetworkConfig, P: ManagerCustodyProfile>(
        &mut self,
        height: u32,
    ) -> anyhow::Result<()> {
        let checkpoint = self.checkpoint.clone();
        let generated_state_bytes = hex::decode(&checkpoint.state_hex)?;
        let old_state_bytes = read_optional_height_artifact(
            self.config.old_state_dir.as_ref(),
            height,
            "old-state",
            &generated_state_bytes,
        )
        .await?;
        let old_state = BridgeState::try_from_slice(&old_state_bytes)?;
        if old_state.get_tip_block_number() != checkpoint.height {
            anyhow::bail!(
                "helper state tip {} does not match checkpoint height {}",
                old_state.get_tip_block_number(),
                checkpoint.height
            );
        }

        validate_frontiers_against_state(&checkpoint, &old_state)?;

        let block = self.block_rpc.get_qd_block(height).await?;
        let block_header = block.to_qdoge_block_header();
        if block_header.header.previous_block_hash != old_state.get_tip_block_hash() {
            anyhow::bail!(
                "block {height} does not extend checkpoint tip {}; reorg handling is out of scope",
                checkpoint.height
            );
        }
        let custody_script_config = CustodyScriptConfig::new(self.config.custody_script_config);
        validate_live_deposit_script::<P>(
            self.config.deposit_evidence_path.as_deref(),
            height,
            &block,
            &custody_script_config,
            &self.config.recipient_atas,
        )?;

        let generated_witness = build_deposit_claim_witness::<P>(
            &block,
            &checkpoint,
            &custody_script_config,
            &self.config.recipient_atas,
        )?;
        let generated_witness_bytes = generated_witness.write_to_vec()?;
        let witness_bytes = read_optional_height_artifact(
            self.config.witness_dir.as_ref(),
            height,
            "witness",
            &generated_witness_bytes,
        )
        .await?;
        let witness = PsyDogeBridgeIncomingBlockWitness::read_from_buffer(&witness_bytes)?;
        if witness.block_header != block_header {
            anyhow::bail!("witness block header does not match Electrs block {height}");
        }

        let evaluation = evaluate_claim_witness::<P>(
            height,
            &witness.claim_witness,
            witness.block_header.header.merkle_root,
            &custody_script_config,
            deposit_flat_fee(&self.config.config_params),
            deposit_fee_numerator(&self.config.config_params),
            deposit_fee_denominator(&self.config.config_params),
        )?;

        let finalized_height = height
            .checked_sub(self.config.required_confirmations)
            .ok_or_else(|| anyhow::anyhow!("finalized height underflow"))?;
        let finalized_buffers = checkpoint
            .pending_finalization
            .get(&finalized_height)
            .cloned()
            .unwrap_or(BlockBufferCommitment::empty()?);

        let mut new_state = old_state;
        new_state.append_block::<NC>(
            height,
            &witness.block_header,
            evaluation.transition.new_claimed_txo_tree_root,
            evaluation.transition.new_auto_claimed_deposits_tree_root,
            evaluation.transition.end_auto_claimed_deposits_index,
            evaluation.transition.fees_collected,
            None,
        )?;
        let mut verified_state = old_state;
        prover_guest_verify_block_transition_detailed::<NC, P>(
            custody_script_config,
            self.config.required_confirmations,
            witness.clone(),
            &mut verified_state,
            deposit_flat_fee(&self.config.config_params),
            deposit_fee_numerator(&self.config.config_params),
            deposit_fee_denominator(&self.config.config_params),
        )?;
        if verified_state != new_state {
            anyhow::bail!("pipeline and guest helper produced different new chain states");
        }

        let old_header = decode_fixed::<HEADER_SIZE>(&checkpoint.header_hex, "checkpoint header")?;
        let new_header = build_new_solana_header(
            &old_header,
            &new_state,
            self.config.required_confirmations,
            finalized_buffers.pending_mints_hash()?,
            finalized_buffers.txo_output_list_hash()?,
        )?;
        let new_state_bytes = borsh::to_vec(&new_state)?;

        let mut claim_history = checkpoint.claim_history.clone();
        claim_history.extend_from_slice(&evaluation.deposit_leaf_hashes);
        let claim_frontier = if let Some(last_leaf) = claim_history.last() {
            let last_index = claim_history.len() - 1;
            MerkleFrontier::new(
                last_index as u32,
                *last_leaf,
                &sparse_sha256_merkle_siblings(
                    &claim_history,
                    last_index,
                    AUTO_CLAIM_DEPOSITS_TREE_HEIGHT,
                    0,
                )?,
            )
        } else {
            checkpoint.claim_frontier.clone()
        };

        let (txo_block_frontier, txo_block_history) = if checkpoint.txo_block_history.is_empty() {
            let txo_siblings = sequential_next_leaf_siblings(
                &checkpoint.txo_block_frontier,
                height,
                TXO_TREE_INDEX_BITS_BLOCK_NUM_LENGTH,
                TXO_BLOCK_FULL_MERKLE_TREE_HEIGHT,
                "TXO block frontier",
            )?;
            (
                MerkleFrontier::new(height, evaluation.block_txo_root, &txo_siblings),
                Vec::new(),
            )
        } else {
            let mut history = checkpoint.txo_block_history.clone();
            if history.len() != height as usize {
                anyhow::bail!(
                    "TXO block history length {} does not match next height {height}",
                    history.len()
                );
            }
            history.push(evaluation.block_txo_root);
            let txo_siblings = sparse_sha256_merkle_siblings(
                &history,
                height as usize,
                TXO_TREE_INDEX_BITS_BLOCK_NUM_LENGTH,
                TXO_BLOCK_FULL_MERKLE_TREE_HEIGHT,
            )?;
            (
                MerkleFrontier::new(height, evaluation.block_txo_root, &txo_siblings),
                history,
            )
        };

        let mut pending_finalization = checkpoint.pending_finalization.clone();
        pending_finalization.insert(height, BlockBufferCommitment::from_evaluation(&evaluation));
        pending_finalization.remove(&finalized_height);
        let next_checkpoint = PipelineCheckpoint {
            height,
            state_hex: hex::encode(new_state_bytes),
            header_hex: hex::encode(new_header),
            claim_frontier,
            txo_block_frontier,
            claim_history,
            txo_block_history,
            pending_finalization,
        };
        validate_checkpoint(&next_checkpoint)?;
        let prover_output = self
            .run_gen_proof(
                height,
                &old_state_bytes,
                &witness_bytes,
                &old_header,
                &new_header,
            )
            .await?;

        let proof = tokio::fs::read(PROOF_PATH).await?;
        let public_values = tokio::fs::read(PUBLIC_VALUES_PATH).await?;
        ensure_length("SP1 proof", &proof, PROOF_SIZE)?;
        ensure_length("SP1 public values", &public_values, PUBLIC_VALUES_SIZE)?;
        if proof.iter().all(|byte| *byte == 0) {
            anyhow::bail!("SP1 proof is an all-zero placeholder");
        }

        let idempotency_key = format!(
            "doge-block-{height}-{}",
            hex::encode(witness.block_header.header.get_hash())
        );
        let uploaded_buffers = self
            .upload_finalized_buffers(finalized_height, &finalized_buffers)
            .await?;
        let mut evidence = persist_evidence(
            &self.config,
            height,
            finalized_height,
            &idempotency_key,
            &old_state_bytes,
            &witness_bytes,
            &old_header,
            &new_header,
            &proof,
            &public_values,
            &prover_output,
            &evaluation,
            &finalized_buffers,
            Some(&uploaded_buffers),
            None,
            None,
        )
        .await?;

        let response = self
            .sender
            .block_update(&BlockUpdateRequestBody {
                idempotency_key: idempotency_key.clone(),
                proof_hex: hex::encode(&proof),
                header_hex: hex::encode(new_header),
                mint_buffer: uploaded_buffers.mint_buffer.to_string(),
                txo_buffer: uploaded_buffers.txo_buffer.to_string(),
                mint_buffer_bump: uploaded_buffers.mint_buffer_bump,
                txo_buffer_bump: uploaded_buffers.txo_buffer_bump,
            })
            .await?;
        if response.idempotency_key != idempotency_key {
            anyhow::bail!(
                "sender returned idempotency key '{}' for request '{}'",
                response.idempotency_key,
                idempotency_key
            );
        }
        let mint_processing = self
            .process_finalized_mints(&finalized_buffers, &uploaded_buffers)
            .await?;

        self.checkpoint = next_checkpoint;
        self.redis
            .set::<(), _, _>(
                &self.checkpoint_key,
                serde_json::to_string(&self.checkpoint)?,
                None,
                None,
                false,
            )
            .await?;

        evidence.status = "minted".to_owned();
        evidence.submission_signature = Some(response.signature);
        evidence.mint_group_signatures = mint_processing.signatures;
        evidence.mint_groups_processed = mint_processing.groups_processed;
        evidence.total_mints_processed = mint_processing.total_mints_processed;
        write_evidence_manifest(&mut evidence).await?;
        println!("{}", serde_json::to_string(&evidence)?);
        Ok(())
    }
    async fn upload_finalized_buffers(
        &self,
        finalized_height: u32,
        finalized: &BlockBufferCommitment,
    ) -> anyhow::Result<UploadedBuffers> {
        let pending_mints: Vec<PendingMint> = finalized
            .pending_mints
            .iter()
            .map(PendingMintPayload::to_bridge_mint)
            .collect();
        validate_buffer_payload(finalized, &pending_mints)?;
        self.ensure_recipient_token_accounts(&pending_mints).await?;
        let (mint_buffer, mint_buffer_bump) = self
            .bridge_client
            .setup_pending_mints_buffer(finalized_height, &pending_mints)
            .await?;
        let (txo_buffer, txo_buffer_bump) = self
            .bridge_client
            .setup_txo_buffer(finalized_height, &finalized.txo_indices)
            .await?;
        Ok(UploadedBuffers {
            mint_buffer,
            mint_buffer_bump,
            txo_buffer,
            txo_buffer_bump,
        })
    }

    async fn ensure_recipient_token_accounts(
        &self,
        pending_mints: &[PendingMint],
    ) -> anyhow::Result<()> {
        if pending_mints.is_empty() {
            return Ok(());
        }
        let rpc = SolanaRpcClient::new_with_commitment(
            self.config.solana_rpc_url.clone(),
            CommitmentConfig::confirmed(),
        );
        for mint in pending_mints {
            let recipient = Pubkey::new_from_array(mint.recipient);
            if !self.config.recipient_atas.contains(&mint.recipient) {
                anyhow::bail!(
                    "pending mint recipient {recipient} is not a configured recipient ATA"
                );
            }
            if rpc.get_account(&recipient).await.is_err() {
                anyhow::bail!(
                    "configured recipient ATA {recipient} does not exist; create it before starting the block pipeline"
                );
            }
        }
        Ok(())
    }

    async fn process_finalized_mints(
        &self,
        finalized: &BlockBufferCommitment,
        buffers: &UploadedBuffers,
    ) -> anyhow::Result<MintProcessingEvidence> {
        let pending_mints: Vec<PendingMint> = finalized
            .pending_mints
            .iter()
            .map(PendingMintPayload::to_bridge_mint)
            .collect();
        let result = self
            .bridge_client
            .process_remaining_pending_mints_groups(
                &pending_mints,
                buffers.mint_buffer,
                buffers.mint_buffer_bump,
            )
            .await?;
        if !result.fully_completed || result.total_mints_processed != pending_mints.len() {
            anyhow::bail!(
                "mint processing completed={} and processed {} of {} pending mints",
                result.fully_completed,
                result.total_mints_processed,
                pending_mints.len()
            );
        }
        Ok(MintProcessingEvidence {
            signatures: result
                .signatures
                .into_iter()
                .map(|signature| signature.to_string())
                .collect(),
            groups_processed: result.groups_processed,
            total_mints_processed: result.total_mints_processed,
        })
    }
}

async fn initialize_checkpoint(
    config: &E2EBlockPipelineConfig,
    block_rpc: &DogeLinkElectrsAsyncClient,
) -> anyhow::Result<PipelineCheckpoint> {
    let electrs_tip = block_rpc.get_block_height().await?;
    let finalized_tip = electrs_tip.saturating_sub(config.required_confirmations);
    let start_height = config.start_height.unwrap_or(finalized_tip);
    if start_height > finalized_tip {
        anyhow::bail!(
            "start height {start_height} is above finalized Electrs height {finalized_tip}"
        );
    }
    if start_height + 1 < PSY_DOGE_BRIDGE_BLOCK_HASH_CACHE_SIZE as u32 {
        anyhow::bail!(
            "start height {start_height} is too low for the {}-header helper cache",
            PSY_DOGE_BRIDGE_BLOCK_HASH_CACHE_SIZE
        );
    }

    let first = start_height + 1 - PSY_DOGE_BRIDGE_BLOCK_HASH_CACHE_SIZE as u32;
    let headers = block_rpc
        .get_qd_block_headers_range_parallel(first, start_height)
        .await?;
    let headers: [QDogeBlockHeader; PSY_DOGE_BRIDGE_BLOCK_HASH_CACHE_SIZE] =
        headers.try_into().map_err(|headers: Vec<_>| {
            anyhow::anyhow!("expected 32 initialization headers, got {}", headers.len())
        })?;
    let state = PsyDogeBridgeState::from_init_data(
        &InitBlockDataIBC::new_from_block_headers_empty_tree(&headers, start_height),
    );
    let pending_finalization = BTreeMap::new();

    let claim_siblings: [QHash256; AUTO_CLAIM_DEPOSITS_TREE_HEIGHT] =
        core::array::from_fn(|index| SHA256_ZERO_HASHES[index]);
    let txo_siblings: [QHash256; TXO_TREE_INDEX_BITS_BLOCK_NUM_LENGTH] =
        core::array::from_fn(|index| SHA256_ZERO_HASHES[TXO_BLOCK_FULL_MERKLE_TREE_HEIGHT + index]);
    let txo_block_history = if start_height <= 1_000_000 {
        vec![SHA256_ZERO_HASHES[TXO_BLOCK_FULL_MERKLE_TREE_HEIGHT]; start_height as usize + 1]
    } else {
        Vec::new()
    };

    Ok(PipelineCheckpoint {
        height: start_height,
        state_hex: hex::encode(borsh::to_vec(&state)?),
        header_hex: hex::encode(config.initial_header),
        claim_frontier: MerkleFrontier::new(0, SHA256_ZERO_HASHES[0], &claim_siblings),
        txo_block_frontier: MerkleFrontier::new(
            start_height,
            SHA256_ZERO_HASHES[TXO_BLOCK_FULL_MERKLE_TREE_HEIGHT],
            &txo_siblings,
        ),
        claim_history: Vec::new(),
        txo_block_history,
        pending_finalization,
    })
}

async fn read_optional_height_artifact(
    directory: Option<&PathBuf>,
    height: u32,
    name: &str,
    generated: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let Some(directory) = directory else {
        return Ok(generated.to_vec());
    };
    let path = directory.join(format!("{height}.hex"));
    let text = tokio::fs::read_to_string(&path).await.map_err(|error| {
        anyhow::anyhow!("failed to read {name} artifact {}: {error}", path.display())
    })?;
    let normalized: String = text
        .trim()
        .strip_prefix("0x")
        .unwrap_or(text.trim())
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect();
    hex::decode(&normalized)
        .map_err(|error| anyhow::anyhow!("invalid {name} artifact {}: {error}", path.display()))
}

#[derive(Deserialize)]
struct DepositEvidenceDocument {
    deposit: DepositEvidenceDeposit,
    custody: DepositEvidenceCustody,
}

#[derive(Deserialize)]
struct DepositEvidenceDeposit {
    txid: String,
    #[serde(default)]
    confirmation_height: Option<u32>,
    #[serde(default)]
    vout: u32,
}

#[derive(Deserialize)]
struct DepositEvidenceCustody {
    original_recipient_address_hex: String,
    #[serde(default)]
    script_pubkey_hex: Option<String>,
    #[serde(default)]
    redeem_script_hex: Option<String>,
}

fn validate_live_deposit_script<P: ManagerCustodyProfile>(
    evidence_path: Option<&Path>,
    height: u32,
    block: &QDogeBlock,
    custody_script_config: &CustodyScriptConfig,
    recipient_atas: &[[u8; 32]],
) -> anyhow::Result<()> {
    let Some(path) = evidence_path else {
        return Ok(());
    };
    if !path.is_file() {
        return Ok(());
    }
    let evidence: DepositEvidenceDocument =
        serde_json::from_slice(&std::fs::read(path).map_err(|error| {
            anyhow::anyhow!(
                "failed to read deposit evidence {}: {error}",
                path.display()
            )
        })?)?;
    if evidence.deposit.confirmation_height != Some(height) {
        return Ok(());
    }
    let recipient_ata = decode_fixed::<32>(
        &evidence.custody.original_recipient_address_hex,
        "deposit evidence recipient ATA",
    )?;
    if !recipient_atas.contains(&recipient_ata) {
        anyhow::bail!(
            "deposit evidence recipient ATA {} is not configured for block auto-claim",
            hex::encode(recipient_ata)
        );
    }
    let expected_redeem_script =
        get_manager_custody_redeem_script::<P>(custody_script_config, &recipient_ata);
    let actual_redeem_script = hex::decode(
        evidence
            .custody
            .redeem_script_hex
            .as_deref()
            .ok_or_else(|| {
                anyhow::anyhow!("confirmed deposit evidence is missing redeem_script_hex")
            })?,
    )?;
    if actual_redeem_script != expected_redeem_script {
        anyhow::bail!(
            "deposit {} at height {height} redeem script does not match the exact {}-byte deterministic 5-of-7 manager custody script for recipient ATA {}",
            evidence.deposit.txid,
            MANAGER_CUSTODY_REDEEM_SCRIPT_SIZE,
            hex::encode(recipient_ata),
        );
    }
    let expected_script =
        get_manager_custody_output_script::<P>(custody_script_config, &recipient_ata);
    let actual_script = hex::decode(evidence.custody.script_pubkey_hex.as_deref().ok_or_else(
        || anyhow::anyhow!("confirmed deposit evidence is missing script_pubkey_hex"),
    )?)?;
    if actual_script != expected_script {
        anyhow::bail!(
            "deposit {} at height {height} P2SH output {} does not match manager custody output {}",
            evidence.deposit.txid,
            evidence
                .custody
                .script_pubkey_hex
                .as_deref()
                .unwrap_or("<missing>"),
            hex::encode(expected_script),
        );
    }
    let tx_hash = decode_dogecoin_txid(&evidence.deposit.txid)?;
    let transaction = block
        .transactions
        .iter()
        .find(|transaction| transaction.get_hash() == tx_hash)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "deposit evidence transaction {} is absent from Electrs block {height}",
                evidence.deposit.txid
            )
        })?;
    let output = transaction
        .outputs
        .get(evidence.deposit.vout as usize)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "deposit evidence vout {} is absent from transaction {}",
                evidence.deposit.vout,
                evidence.deposit.txid
            )
        })?;
    if output.script != expected_script {
        anyhow::bail!(
            "deposit evidence transaction {} vout {} does not contain the exact manager custody output",
            evidence.deposit.txid,
            evidence.deposit.vout,
        );
    }
    Ok(())
}

fn decode_dogecoin_txid(value: &str) -> anyhow::Result<QHash256> {
    let mut bytes = decode_fixed::<32>(value, "Dogecoin transaction id")?;
    bytes.reverse();
    Ok(bytes)
}

fn build_deposit_claim_witness<P: ManagerCustodyProfile>(
    block: &QDogeBlock,
    checkpoint: &PipelineCheckpoint,
    custody_script_config: &CustodyScriptConfig,
    recipient_atas: &[[u8; 32]],
) -> anyhow::Result<PsyDogeBridgeIncomingBlockWitness> {
    if block.transactions.is_empty() {
        anyhow::bail!("Electrs block contains no transactions");
    }

    let transaction_hashes: Vec<QHash256> = block
        .transactions
        .iter()
        .map(|transaction| transaction.get_hash())
        .collect();
    let computed_merkle_root = bitcoin_merkle_root(&transaction_hashes)
        .ok_or_else(|| anyhow::anyhow!("cannot compute an empty block transaction Merkle root"))?;
    if computed_merkle_root != block.header.merkle_root {
        anyhow::bail!(
            "Electrs transaction data Merkle root {} does not match block header {}",
            hex::encode(computed_merkle_root),
            hex::encode(block.header.merkle_root)
        );
    }

    let expected_scripts: Vec<[u8; 23]> = recipient_atas
        .iter()
        .map(|recipient_ata| {
            get_manager_custody_output_script::<P>(custody_script_config, recipient_ata)
        })
        .collect();
    let mut total_outputs = 0u32;
    let mut deposit_transactions = Vec::new();

    for (transaction_index, transaction) in block.transactions.iter().enumerate() {
        let mut deposit_outputs = Vec::new();
        for (output_index, output) in transaction.outputs.iter().enumerate() {
            let matching_key = expected_scripts
                .iter()
                .position(|script| output.script.as_slice() == script);
            if let Some(public_key_index) = matching_key {
                deposit_outputs.push(PsyBridgeClaimDepositItem {
                    output_index: u32::try_from(output_index)
                        .map_err(|_| anyhow::anyhow!("transaction output index exceeds u32"))?,
                    public_key_index: u32::try_from(public_key_index)
                        .map_err(|_| anyhow::anyhow!("deposit public key index exceeds u32"))?,
                });
                total_outputs = total_outputs
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("deposit output count overflow"))?;
            }
        }
        if !deposit_outputs.is_empty() {
            deposit_transactions.push(PsyBridgeClaimBlockTransactionWitness::new(
                u32::try_from(transaction_index)
                    .map_err(|_| anyhow::anyhow!("transaction index exceeds u32"))?,
                bitcoin_merkle_siblings(&transaction_hashes, transaction_index)?,
                deposit_outputs,
                transaction.clone(),
            ));
        }
    }

    let claim_last_value =
        decode_hash(&checkpoint.claim_frontier.value_hex, "claim frontier value")?;
    let claim_siblings = if checkpoint.claim_history.is_empty() {
        checkpoint
            .claim_frontier
            .decode(AUTO_CLAIM_DEPOSITS_TREE_HEIGHT, "claim frontier")?
            .1
    } else {
        sparse_sha256_merkle_siblings(
            &checkpoint.claim_history,
            checkpoint.claim_history.len() - 1,
            AUTO_CLAIM_DEPOSITS_TREE_HEIGHT,
            0,
        )?
    };
    let next_height = checkpoint
        .height
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("checkpoint height overflow"))?;
    let txo_tree_block_siblings = if checkpoint.txo_block_history.is_empty() {
        sequential_next_leaf_siblings(
            &checkpoint.txo_block_frontier,
            next_height,
            TXO_TREE_INDEX_BITS_BLOCK_NUM_LENGTH,
            TXO_BLOCK_FULL_MERKLE_TREE_HEIGHT,
            "TXO block frontier",
        )?
    } else {
        let mut prospective = checkpoint.txo_block_history.clone();
        prospective.push(SHA256_ZERO_HASHES[TXO_BLOCK_FULL_MERKLE_TREE_HEIGHT]);
        sparse_sha256_merkle_siblings(
            &prospective,
            next_height as usize,
            TXO_TREE_INDEX_BITS_BLOCK_NUM_LENGTH,
            TXO_BLOCK_FULL_MERKLE_TREE_HEIGHT,
        )?
    };

    let old_header = decode_fixed::<HEADER_SIZE>(&checkpoint.header_hex, "checkpoint header")?;
    Ok(PsyDogeBridgeIncomingBlockWitness {
        block_header: block.to_qdoge_block_header(),
        claim_witness: PsyBridgeClaimBlockWitness::new(
            PsyBridgeClaimBlockWitnessHeader {
                txo_tree_block_siblings: txo_tree_block_siblings
                    .try_into()
                    .expect("TXO sibling height was checked"),
                last_auto_claimed_deposits_siblings: claim_siblings
                    .try_into()
                    .expect("claim sibling height was checked"),
                total_outputs_hint: total_outputs,
                claim_deposits_last_index: checkpoint.claim_frontier.index,
                claim_deposits_last_value: claim_last_value,
            },
            recipient_atas.to_vec(),
            deposit_transactions,
        ),
        previous_header_last_rollback_at_secs: u32::from_le_bytes(
            old_header[SOL_LAST_ROLLBACK_AT_SECS]
                .try_into()
                .expect("fixed Solana header range"),
        ),
        previous_header_paused_until_secs: u32::from_le_bytes(
            old_header[SOL_PAUSED_UNTIL_SECS]
                .try_into()
                .expect("fixed Solana header range"),
        ),
    })
}

fn evaluate_claim_witness<P: ManagerCustodyProfile>(
    block_height: u32,
    witness: &PsyBridgeClaimBlockWitness,
    block_transaction_tree_merkle_root: QHash256,
    custody_script_config: &CustodyScriptConfig,
    flat_fee_per_deposit_sats: u64,
    deposit_fee_rate_numerator: u64,
    deposit_fee_rate_denominator: u64,
) -> anyhow::Result<ClaimEvaluation> {
    let mut builder = BlockTransitionBuilder::new_from_siblings::<P>(
        witness.header.total_outputs_hint as usize,
        flat_fee_per_deposit_sats,
        deposit_fee_rate_numerator,
        deposit_fee_rate_denominator,
        &witness.header.last_auto_claimed_deposits_siblings,
        witness.header.claim_deposits_last_index,
        &witness.header.claim_deposits_last_value,
        custody_script_config,
        witness.deposit_solana_public_keys.clone(),
    )?;

    let mut deposit_leaf_hashes = Vec::with_capacity(witness.header.total_outputs_hint as usize);
    let mut pending_mints = Vec::with_capacity(witness.header.total_outputs_hint as usize);
    let mut txo_indices = Vec::with_capacity(witness.header.total_outputs_hint as usize);
    for transaction_witness in &witness.deposit_transactions {
        transaction_witness.verify_and_add_to_transition_builder(
            &mut builder,
            &block_transaction_tree_merkle_root,
        )?;
        let transaction_hash = transaction_witness.transaction.get_hash();
        for deposit in &transaction_witness.deposit_outputs {
            let public_key = witness
                .deposit_solana_public_keys
                .get(deposit.public_key_index as usize)
                .ok_or_else(|| anyhow::anyhow!("deposit public key index out of bounds"))?;
            let output = transaction_witness
                .transaction
                .outputs
                .get(deposit.output_index as usize)
                .ok_or_else(|| anyhow::anyhow!("deposit output index out of bounds"))?;
            let (_, net_amount) = calcuate_fee(
                output.value,
                flat_fee_per_deposit_sats,
                deposit_fee_rate_numerator,
                deposit_fee_rate_denominator,
            )?;
            pending_mints.push(PendingMintPayload {
                recipient: *public_key,
                amount: net_amount,
            });
            txo_indices.push(combined_txo_index(
                transaction_witness.transaction_index,
                deposit.output_index,
            )?);
            deposit_leaf_hashes.push(hash_deposit_leaf(
                &transaction_hash,
                deposit.output_index,
                public_key,
                &net_amount,
            ));
        }
    }

    if deposit_leaf_hashes.len() != witness.header.total_outputs_hint as usize {
        anyhow::bail!(
            "witness total_outputs_hint {} does not match {} deposit outputs",
            witness.header.total_outputs_hint,
            deposit_leaf_hashes.len()
        );
    }

    let transition = builder.finalize(
        block_height,
        witness.header.claim_deposits_last_index,
        &witness.header.claim_deposits_last_value,
        flat_fee_per_deposit_sats,
        deposit_fee_rate_numerator,
        deposit_fee_rate_denominator,
        &witness.header.txo_tree_block_siblings,
    )?;
    let block_txo_root = builder.txo_claimed_txs_in_block_tree.current_root;
    let minted_amount_sats = builder.total_deposit_amount;
    let pending_mints_hash = builder.pending_mints.finalize()?;
    let txo_output_list_hash = hash_impl_sha256_bytes(&serialize_txo_indices(&txo_indices));

    Ok(ClaimEvaluation {
        transition,
        pending_mints_hash,
        txo_output_list_hash,
        pending_mints,
        txo_indices,
        block_txo_root,
        deposit_count: u32::try_from(deposit_leaf_hashes.len())
            .map_err(|_| anyhow::anyhow!("deposit count exceeds u32"))?,
        deposit_leaf_hashes,
        minted_amount_sats,
    })
}

fn bitcoin_merkle_root(transaction_hashes: &[QHash256]) -> Option<QHash256> {
    if transaction_hashes.is_empty() {
        return None;
    }
    let mut level = transaction_hashes.to_vec();
    while level.len() > 1 {
        if level.len() % 2 == 1 {
            level.push(*level.last().expect("non-empty Merkle level"));
        }
        let mut next = Vec::with_capacity(level.len() / 2);
        for pair in level.chunks_exact(2) {
            next.push(hash_impl_btc_hash256_two_to_one_bytes(&pair[0], &pair[1]));
        }
        level = next;
    }
    level.first().copied()
}

fn bitcoin_merkle_siblings(
    transaction_hashes: &[QHash256],
    transaction_index: usize,
) -> anyhow::Result<Vec<QHash256>> {
    if transaction_hashes.is_empty() || transaction_index >= transaction_hashes.len() {
        anyhow::bail!("transaction Merkle proof index is out of bounds");
    }
    let mut level = transaction_hashes.to_vec();
    let mut index = transaction_index;
    let mut siblings = Vec::new();
    while level.len() > 1 {
        if level.len() % 2 == 1 {
            level.push(*level.last().expect("non-empty Merkle level"));
        }
        siblings.push(level[index ^ 1]);
        let mut next = Vec::with_capacity(level.len() / 2);
        for pair in level.chunks_exact(2) {
            next.push(hash_impl_btc_hash256_two_to_one_bytes(&pair[0], &pair[1]));
        }
        level = next;
        index >>= 1;
    }
    Ok(siblings)
}

fn sparse_sha256_merkle_siblings(
    leaves: &[QHash256],
    target_index: usize,
    height: usize,
    leaf_height: usize,
) -> anyhow::Result<Vec<QHash256>> {
    if target_index >= leaves.len() {
        anyhow::bail!("Merkle target index is outside known leaves");
    }
    let mut level = leaves.to_vec();
    let mut index = target_index;
    let mut siblings = Vec::with_capacity(height);
    for depth in 0..height {
        let sibling_index = index ^ 1;
        siblings.push(
            level
                .get(sibling_index)
                .copied()
                .unwrap_or(SHA256_ZERO_HASHES[leaf_height + depth]),
        );
        if level.len() % 2 == 1 {
            level.push(SHA256_ZERO_HASHES[leaf_height + depth]);
        }
        let mut next = Vec::with_capacity(level.len() / 2);
        for pair in level.chunks_exact(2) {
            next.push(hash_impl_sha256_two_to_one_bytes(&pair[0], &pair[1]));
        }
        level = next;
        index >>= 1;
    }
    Ok(siblings)
}

fn sequential_next_leaf_siblings(
    frontier: &MerkleFrontier,
    new_index: u32,
    height: usize,
    leaf_height: usize,
    name: &str,
) -> anyhow::Result<Vec<QHash256>> {
    let expected_index = frontier
        .index
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("{name} index overflow"))?;
    if new_index != expected_index {
        anyhow::bail!("{name} expected next index {expected_index}, got {new_index}");
    }
    let (last_value, last_siblings) = frontier.decode(height, name)?;
    let mut current = last_value;
    let mut previous_index = frontier.index;
    let mut next_frontier = vec![[0u8; 32]; height];
    for (level, sibling) in last_siblings.iter().enumerate() {
        if previous_index & 1 == 0 {
            next_frontier[level] = current;
            current = hash_impl_sha256_two_to_one_bytes(&current, sibling);
        } else {
            next_frontier[level] = *sibling;
            current = hash_impl_sha256_two_to_one_bytes(sibling, &current);
        }
        previous_index >>= 1;
    }

    Ok((0..height)
        .map(|level| {
            if (new_index >> level) & 1 == 0 {
                SHA256_ZERO_HASHES[leaf_height + level]
            } else {
                next_frontier[level]
            }
        })
        .collect())
}

fn frontier_root(frontier: &MerkleFrontier, height: usize, name: &str) -> anyhow::Result<QHash256> {
    let (mut current, siblings) = frontier.decode(height, name)?;
    let mut index = frontier.index;
    for sibling in siblings {
        current = if index & 1 == 0 {
            hash_impl_sha256_two_to_one_bytes(&current, &sibling)
        } else {
            hash_impl_sha256_two_to_one_bytes(&sibling, &current)
        };
        index >>= 1;
    }
    if index != 0 {
        anyhow::bail!("{name} index exceeds its configured tree height");
    }
    Ok(current)
}

fn validate_frontiers_against_state(
    checkpoint: &PipelineCheckpoint,
    state: &BridgeState,
) -> anyhow::Result<()> {
    let tip = state
        .block_data_tracker
        .get_record(state.get_tip_block_number())?;
    let claim_root = frontier_root(
        &checkpoint.claim_frontier,
        AUTO_CLAIM_DEPOSITS_TREE_HEIGHT,
        "claim frontier",
    )?;
    if claim_root != tip.auto_claimed_deposits_tree_root {
        anyhow::bail!("claim frontier root does not match checkpoint state");
    }
    let claim_value = decode_hash(&checkpoint.claim_frontier.value_hex, "claim frontier value")?;
    let expected_claim_index =
        if checkpoint.claim_frontier.index == 0 && claim_value == SHA256_ZERO_HASHES[0] {
            0
        } else {
            checkpoint.claim_frontier.index + 1
        };
    let state_claim_index: u32 = tip.auto_claimed_deposits_next_index.into();
    if expected_claim_index != state_claim_index {
        anyhow::bail!(
            "claim frontier next index {expected_claim_index} does not match state {state_claim_index}"
        );
    }

    let txo_root = frontier_root(
        &checkpoint.txo_block_frontier,
        TXO_TREE_INDEX_BITS_BLOCK_NUM_LENGTH,
        "TXO block frontier",
    )?;
    if txo_root != tip.auto_claimed_txo_tree_root {
        anyhow::bail!("TXO block frontier root does not match checkpoint state");
    }
    if checkpoint.txo_block_frontier.index != checkpoint.height {
        anyhow::bail!("TXO block frontier height does not match checkpoint height");
    }
    Ok(())
}

fn build_new_solana_header(
    old_header: &[u8; HEADER_SIZE],
    state: &BridgeState,
    required_confirmations: u32,
    pending_mints_hash: QHash256,
    txo_output_list_hash: QHash256,
) -> anyhow::Result<[u8; HEADER_SIZE]> {
    let tip = state.block_data_tracker.get_tip_state_commitment();
    let finalized = state
        .block_data_tracker
        .get_finalized_state_commitment(required_confirmations)?;
    let tip_record = state
        .block_data_tracker
        .get_record(state.get_tip_block_number())?;
    let finalized_record = state
        .block_data_tracker
        .get_record(state.get_finalized_block_number(required_confirmations))?;

    let mut new_header = *old_header;
    new_header[SOL_TIP_BLOCK_HASH].copy_from_slice(&tip.block_hash);
    new_header[SOL_TIP_BLOCK_MERKLE_ROOT].copy_from_slice(&tip.block_merkle_tree_root);
    new_header[SOL_TIP_BLOCK_TIME].copy_from_slice(&u32::from(tip_record.timestamp).to_le_bytes());
    new_header[SOL_TIP_BLOCK_HEIGHT].copy_from_slice(&tip.block_height.to_le_bytes());

    new_header[SOL_FINALIZED_BLOCK_HASH].copy_from_slice(&finalized.block_hash);
    new_header[SOL_FINALIZED_BLOCK_MERKLE_ROOT].copy_from_slice(&finalized.block_merkle_tree_root);
    new_header[SOL_FINALIZED_PENDING_MINTS_HASH].copy_from_slice(&pending_mints_hash);
    new_header[SOL_FINALIZED_TXO_LIST_HASH].copy_from_slice(&txo_output_list_hash);
    new_header[SOL_FINALIZED_AUTO_CLAIMED_TXO_ROOT]
        .copy_from_slice(&finalized.auto_claimed_txo_tree_root);
    new_header[SOL_FINALIZED_AUTO_CLAIMED_DEPOSITS_ROOT]
        .copy_from_slice(&finalized.auto_claimed_deposits_tree_root);
    new_header[SOL_FINALIZED_AUTO_CLAIMED_NEXT_INDEX]
        .copy_from_slice(&finalized.auto_claimed_deposits_next_index.to_le_bytes());
    new_header[SOL_FINALIZED_BLOCK_HEIGHT].copy_from_slice(&finalized.block_height.to_le_bytes());
    new_header[SOL_BRIDGE_STATE_HASH]
        .copy_from_slice(&hash_impl_sha256_bytes(&borsh::to_vec(state)?));
    new_header[SOL_TOTAL_FINALIZED_FEES].copy_from_slice(
        &u64::from(finalized_record.total_fees_collected_chain_history).to_le_bytes(),
    );
    Ok(new_header)
}



fn parse_prover_field<'a>(stdout: &'a [u8], field: &str) -> anyhow::Result<&'a str> {
    let stdout = std::str::from_utf8(stdout)?;
    stdout
        .lines()
        .find_map(|line| line.trim().strip_prefix(&format!("{field}:")))
        .map(str::trim)
        .ok_or_else(|| anyhow::anyhow!("gen-proof stdout did not contain {field}"))
}

async fn prepare_prover_request(
    config: &E2EBlockPipelineConfig,
    old_state: &[u8],
    witness: &[u8],
    old_header: &[u8; HEADER_SIZE],
    new_header: &[u8; HEADER_SIZE],
) -> anyhow::Result<()> {
    for path in [PROOF_PATH, PUBLIC_VALUES_PATH] {
        match tokio::fs::remove_file(path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    if std::env::var_os("DOGE_SAVE_PROVER_ARGS").is_some() {
        let args = serde_json::json!({
            "network": config.network.as_str(),
            "old_state": hex::encode(old_state),
            "witness": hex::encode(witness),
            "custody_script_config": hex::encode(config.custody_script_config),
            "required_confirmations": config.required_confirmations,
            "flat_fee": deposit_flat_fee(&config.config_params),
            "fee_num": deposit_fee_numerator(&config.config_params),
            "fee_den": deposit_fee_denominator(&config.config_params),
            "old_header": hex::encode(old_header),
            "new_header": hex::encode(new_header),
            "config_params": hex::encode(config.config_params),
        });
        tokio::fs::write(
            "/tmp/psy-block-prover-args.json",
            serde_json::to_vec_pretty(&args)?,
        )
        .await?;
    }
    Ok(())
}

async fn validate_prover_identity(
    config: &E2EBlockPipelineConfig,
    identity: &ProverIdentityResponse,
) -> anyhow::Result<[u8; 32]> {
    if identity.kind != "identity" {
        anyhow::bail!(
            "gen-proof initial response kind was '{}', expected 'identity'",
            identity.kind
        );
    }
    if identity.network != config.network.as_str() {
        anyhow::bail!(
            "SP1 prover network mismatch: expected {}, got {}",
            config.network.as_str(),
            identity.network
        );
    }
    validate_prover_elf(config, &identity.block_elf_path, &identity.block_elf_sha256).await?;
    let vk_hash = decode_prover_hash(&identity.vkey_hash, "SP1 program VK hash")?;
    if vk_hash != config.expected_vk_hash {
        anyhow::bail!(
            "SP1 program VK mismatch: expected {}, got {}",
            hex::encode(config.expected_vk_hash),
            hex::encode(vk_hash)
        );
    }
    Ok(vk_hash)
}

async fn validate_prover_elf(
    config: &E2EBlockPipelineConfig,
    prover_elf_path: &str,
    prover_elf_sha256: &str,
) -> anyhow::Result<()> {
    let prover_elf_path = PathBuf::from(prover_elf_path);
    let configured_elf_path = std::fs::canonicalize(&config.block_elf_path)?;
    let embedded_elf_path = std::fs::canonicalize(&prover_elf_path).map_err(|error| {
        anyhow::anyhow!(
            "failed to resolve gen-proof embedded ELF path {}: {error}",
            prover_elf_path.display()
        )
    })?;
    if embedded_elf_path != configured_elf_path {
        anyhow::bail!(
            "SP1 ELF path mismatch: gen-proof embeds {}, pipeline configured {}",
            embedded_elf_path.display(),
            configured_elf_path.display()
        );
    }
    let configured_elf = tokio::fs::read(&configured_elf_path).await?;
    let configured_elf_sha256 = sha256_hex(&configured_elf);
    if prover_elf_sha256 != configured_elf_sha256 {
        anyhow::bail!(
            "SP1 ELF hash mismatch: gen-proof embeds {prover_elf_sha256}, configured ELF is {configured_elf_sha256}"
        );
    }
    Ok(())
}

fn validate_proof_response(
    config: &E2EBlockPipelineConfig,
    identity_vk_hash: [u8; 32],
    response: &ProverProofResponse,
) -> anyhow::Result<()> {
    if response.network.as_deref() != Some(config.network.as_str()) {
        anyhow::bail!(
            "gen-proof response network {:?} did not match {}",
            response.network,
            config.network.as_str()
        );
    }
    let block_elf_path = response
        .block_elf_path
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("gen-proof response omitted block_elf_path"))?;
    let response_elf_path = std::fs::canonicalize(block_elf_path)?;
    let configured_elf_path = std::fs::canonicalize(&config.block_elf_path)?;
    if response_elf_path != configured_elf_path {
        anyhow::bail!("gen-proof response block_elf_path changed after identity validation");
    }
    let configured_elf = std::fs::read(&configured_elf_path)?;
    let configured_elf_sha256 = sha256_hex(&configured_elf);
    if response.block_elf_sha256.as_deref() != Some(configured_elf_sha256.as_str()) {
        anyhow::bail!("gen-proof response block_elf_sha256 changed after identity validation");
    }
    let response_vk_hash = decode_prover_hash(
        response
            .vkey_hash
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("gen-proof response omitted vkey_hash"))?,
        "SP1 response VK hash",
    )?;
    if response_vk_hash != identity_vk_hash || response_vk_hash != config.expected_vk_hash {
        anyhow::bail!("gen-proof response VK hash changed after identity validation");
    }
    if response.proof_path.as_deref() != Some(PROOF_PATH) {
        anyhow::bail!("gen-proof response proof_path was not {PROOF_PATH}");
    }
    if response.public_values_path.as_deref() != Some(PUBLIC_VALUES_PATH) {
        anyhow::bail!("gen-proof response public_values_path was not {PUBLIC_VALUES_PATH}");
    }
    Ok(())
}

fn decode_required_response_bytes(value: Option<&str>, name: &str) -> anyhow::Result<Vec<u8>> {
    let value = value.ok_or_else(|| anyhow::anyhow!("gen-proof response omitted {name}"))?;
    hex::decode(value).map_err(|error| anyhow::anyhow!("invalid gen-proof {name} hex: {error}"))
}

fn decode_prover_hash(value: &str, name: &str) -> anyhow::Result<[u8; 32]> {
    decode_fixed::<32>(value.strip_prefix("0x").unwrap_or(value), name)
}

async fn persist_evidence(
    config: &E2EBlockPipelineConfig,
    height: u32,
    finalized_source_height: u32,
    idempotency_key: &str,
    old_state: &[u8],
    witness: &[u8],
    old_header: &[u8; HEADER_SIZE],
    new_header: &[u8; HEADER_SIZE],
    proof: &[u8],
    public_values: &[u8],
    prover_output: &ProverOutput,
    evaluation: &ClaimEvaluation,
    finalized_buffers: &BlockBufferCommitment,
    uploaded_buffers: Option<&UploadedBuffers>,
    submission_signature: Option<String>,
    mint_processing: Option<&MintProcessingEvidence>,
) -> anyhow::Result<BlockProofEvidence> {
    let elf = tokio::fs::read(&config.block_elf_path).await?;
    if elf.is_empty() {
        anyhow::bail!("block-transition ELF is empty");
    }
    let block_hash = new_header[SOL_TIP_BLOCK_HASH].to_vec();
    let inputs = ProverInputsEvidence {
        schema_version: EVIDENCE_SCHEMA_VERSION,
        height,
        block_hash: hex::encode(&block_hash),
        custody_script_config: hex::encode(config.custody_script_config),
        recipient_atas: config.recipient_atas.iter().map(hex::encode).collect(),
        required_confirmations: config.required_confirmations,
        flat_fee: deposit_flat_fee(&config.config_params),
        fee_num: deposit_fee_numerator(&config.config_params),
        fee_den: deposit_fee_denominator(&config.config_params),
        config_params: hex::encode(config.config_params),
        custodian_hash: hex::encode(config.network.custodian_hash(config.custody_script_config)),
        expected_vk_hash: hex::encode(config.expected_vk_hash),
        gen_proof_path: absolute_path(&config.gen_proof_path)?.display().to_string(),
        block_elf_source_path: absolute_path(&config.block_elf_path)?.display().to_string(),
        old_state_sha256: sha256_hex(old_state),
        witness_sha256: sha256_hex(witness),
        old_header_sha256: sha256_hex(old_header),
        new_header_sha256: sha256_hex(new_header),
    };
    let inputs_json = serde_json::to_vec_pretty(&inputs)?;
    let pending_mints_json = serde_json::to_vec_pretty(&finalized_buffers.pending_mints)?;
    let txo_indices_json = serde_json::to_vec_pretty(&finalized_buffers.txo_indices)?;
    let pending_mints_bin = serialize_pending_mint_payloads(&finalized_buffers.pending_mints);
    let txo_indices_bin = serialize_txo_indices(&finalized_buffers.txo_indices);

    let artifact_bytes: Vec<(&str, &[u8])> = vec![
        ("old_state.bin", old_state),
        ("witness.bin", witness),
        ("old_header.bin", old_header),
        ("new_header.bin", new_header),
        ("proof.bin", proof),
        ("public_values.bin", public_values),
        ("prover_stdout.txt", &prover_output.stdout),
        ("prover_stderr.txt", &prover_output.stderr),
        ("vk.bin", &prover_output.vk_hash),
        ("elf.bin", &elf),
        ("inputs.json", &inputs_json),
        ("pending_mints.bin", &pending_mints_bin),
        ("pending_mints.json", &pending_mints_json),
        ("txo_indices.bin", &txo_indices_bin),
        ("txo_indices.json", &txo_indices_json),
    ];
    let content_sha256 = artifact_bundle_sha256(&artifact_bytes);
    let root = absolute_path(&config.evidence_dir)?;
    let evidence_dir = root.join(format!("height-{height}")).join(&content_sha256);
    tokio::fs::create_dir_all(&evidence_dir).await?;

    let mut artifacts = BTreeMap::new();
    for (filename, bytes) in artifact_bytes {
        let path = evidence_dir.join(filename);
        tokio::fs::write(&path, bytes).await?;
        let key = match filename {
            "pending_mints.bin" => "pending_mints",
            "pending_mints.json" => "pending_mints_json",
            "txo_indices.bin" => "txo_indices",
            "txo_indices.json" => "txo_indices_json",
            _ => filename
                .trim_end_matches(".bin")
                .trim_end_matches(".txt")
                .trim_end_matches(".json"),
        };
        artifacts.insert(
            key.to_owned(),
            EvidenceArtifact {
                path: path.display().to_string(),
                sha256: sha256_hex(bytes),
                size: bytes.len() as u64,
            },
        );
    }

    let manifest_path = evidence_dir.join("manifest.json");
    let latest_path = root.join("latest.json");
    let mut manifest = BlockProofEvidence {
        schema_version: EVIDENCE_SCHEMA_VERSION,
        status: if mint_processing.is_some() {
            "minted".to_owned()
        } else if submission_signature.is_some() {
            "submitted".to_owned()
        } else if uploaded_buffers.is_some() {
            "buffers_uploaded".to_owned()
        } else {
            "proof_generated".to_owned()
        },
        height,
        block_hash: hex::encode(block_hash),
        finalized_source_height,
        source_witness_height: height,
        witness_deposit_count: evaluation.deposit_count,
        witness_minted_amount_sats: evaluation.minted_amount_sats,
        deposit_count: finalized_buffers.deposit_count,
        minted_amount_sats: finalized_buffers.minted_amount_sats,
        auto_claim_start_index: finalized_buffers.auto_claim_start_index,
        auto_claim_end_index: finalized_buffers.auto_claim_end_index,
        fees_collected: finalized_buffers.fees_collected,
        content_sha256,
        manifest_sha256: String::new(),
        evidence_dir: evidence_dir.display().to_string(),
        manifest_path: manifest_path.display().to_string(),
        latest_path: latest_path.display().to_string(),
        idempotency_key: idempotency_key.to_owned(),
        mint_buffer: uploaded_buffers.map(|buffers| buffers.mint_buffer.to_string()),
        mint_buffer_bump: uploaded_buffers.map(|buffers| buffers.mint_buffer_bump),
        txo_buffer: uploaded_buffers.map(|buffers| buffers.txo_buffer.to_string()),
        txo_buffer_bump: uploaded_buffers.map(|buffers| buffers.txo_buffer_bump),
        buffer_upload_completed: uploaded_buffers.is_some(),
        submission_signature,
        mint_group_signatures: mint_processing
            .map(|processing| processing.signatures.clone())
            .unwrap_or_default(),
        mint_groups_processed: mint_processing
            .map(|processing| processing.groups_processed)
            .unwrap_or_default(),
        total_mints_processed: mint_processing
            .map(|processing| processing.total_mints_processed)
            .unwrap_or_default(),
        vk_hash: hex::encode(prover_output.vk_hash),
        elf_sha256: sha256_hex(&elf),
        artifacts,
    };
    write_evidence_manifest(&mut manifest).await?;
    Ok(manifest)
}

async fn write_evidence_manifest(manifest: &mut BlockProofEvidence) -> anyhow::Result<()> {
    manifest.manifest_sha256.clear();
    let payload = serde_json::to_vec(manifest)?;
    manifest.manifest_sha256 = sha256_hex(&payload);
    let bytes = serde_json::to_vec_pretty(manifest)?;
    let manifest_path = PathBuf::from(&manifest.manifest_path);
    let latest_path = PathBuf::from(&manifest.latest_path);
    tokio::fs::write(&manifest_path, &bytes).await?;
    write_atomic(&latest_path, &bytes).await?;
    Ok(())
}

async fn write_atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    tokio::fs::write(&temporary, bytes).await?;
    tokio::fs::rename(&temporary, path).await?;
    Ok(())
}

fn artifact_bundle_sha256(artifacts: &[(&str, &[u8])]) -> String {
    let mut commitment = Vec::with_capacity(artifacts.len() * 80);
    for (name, bytes) in artifacts {
        commitment.extend_from_slice(&(name.len() as u32).to_le_bytes());
        commitment.extend_from_slice(name.as_bytes());
        commitment.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        commitment.extend_from_slice(&hash_impl_sha256_bytes(bytes));
    }
    sha256_hex(&commitment)
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(hash_impl_sha256_bytes(bytes))
}

fn serialize_pending_mint_payloads(payloads: &[PendingMintPayload]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(payloads.len() * 40);
    for payload in payloads {
        bytes.extend_from_slice(&payload.recipient);
        bytes.extend_from_slice(&payload.amount.to_le_bytes());
    }
    bytes
}

fn serialize_txo_indices(indices: &[u32]) -> Vec<u8> {
    bytemuck::cast_slice(indices).to_vec()
}

fn pending_mints_hash(payloads: &[PendingMintPayload]) -> anyhow::Result<QHash256> {
    let mut builder = PendingMintsGroupsBuilder::new_with_hint(payloads.len());
    for payload in payloads {
        builder.append_pending_mint(&payload.recipient, payload.amount);
    }
    builder.finalize()
}

fn validate_buffer_payload(
    finalized: &BlockBufferCommitment,
    pending_mints: &[PendingMint],
) -> anyhow::Result<()> {
    if finalized.pending_mints.len() != finalized.txo_indices.len()
        || finalized.pending_mints.len() != finalized.deposit_count as usize
    {
        anyhow::bail!(
            "finalized buffer payload lengths mints={}, txos={}, deposits={} do not match",
            finalized.pending_mints.len(),
            finalized.txo_indices.len(),
            finalized.deposit_count
        );
    }
    if pending_mints.len() != finalized.pending_mints.len() {
        anyhow::bail!("bridge pending-mint conversion changed payload length");
    }
    let amount = finalized.pending_mints.iter().try_fold(0u64, |sum, mint| {
        sum.checked_add(mint.amount)
            .ok_or_else(|| anyhow::anyhow!("pending-mint amount overflow"))
    })?;
    if amount != finalized.minted_amount_sats {
        anyhow::bail!(
            "pending-mint payload amount {amount} does not match commitment {}",
            finalized.minted_amount_sats
        );
    }
    if pending_mints_hash(&finalized.pending_mints)? != finalized.pending_mints_hash()? {
        anyhow::bail!("pending-mint payload hash does not match finalized header commitment");
    }
    let actual_txo_hash = hash_impl_sha256_bytes(&serialize_txo_indices(&finalized.txo_indices));
    let expected_txo_hash = finalized.txo_output_list_hash()?;
    if actual_txo_hash != expected_txo_hash {
        anyhow::bail!(
            "TXO payload hash {} does not match finalized header commitment {} for indices {:?}",
            hex::encode(actual_txo_hash),
            hex::encode(expected_txo_hash),
            finalized.txo_indices
        );
    }
    Ok(())
}

fn combined_txo_index(transaction_index: u32, output_index: u32) -> anyhow::Result<u32> {
    const MAX_OUTPUTS_PER_TX: u32 =
        psy_doge_bridge_helper::claim::block_tx_output_tree::TXO_TREE_MAX_OUTPUTS_PER_TX as u32;
    transaction_index
        .checked_mul(MAX_OUTPUTS_PER_TX)
        .and_then(|index| index.checked_add(output_index))
        .ok_or_else(|| anyhow::anyhow!("combined TXO index overflow"))
}

fn read_pipeline_keypair(path: &Path, name: &str) -> anyhow::Result<Keypair> {
    read_keypair_file(path).map_err(|error| {
        anyhow::anyhow!("failed to read {name} keypair {}: {error}", path.display())
    })
}

fn absolute_path(path: &Path) -> anyhow::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_owned())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn validate_config(config: &E2EBlockPipelineConfig) -> anyhow::Result<()> {
    if cfg!(debug_assertions) {
        anyhow::bail!("e2e block pipeline must be built in release mode");
    }
    if !config.gen_proof_path.is_file() {
        anyhow::bail!(
            "SP1 gen-proof executable does not exist at {}",
            config.gen_proof_path.display()
        );
    }
    ensure_release_path(&config.gen_proof_path, "SP1 gen-proof")?;
    if !config.block_elf_path.is_file() {
        anyhow::bail!(
            "block-transition ELF does not exist at {}",
            config.block_elf_path.display()
        );
    }
    ensure_release_path(&config.block_elf_path, "block-transition ELF")?;
    if config.expected_vk_hash.iter().all(|byte| *byte == 0) {
        anyhow::bail!("expected SP1 program VK hash must be non-zero");
    }
    let profile_vk_hash = config.network.default_vk_hash();
    if config.expected_vk_hash != profile_vk_hash {
        anyhow::bail!(
            "{} profile requires SP1 block VK {}, got {}",
            config.network.as_str(),
            hex::encode(profile_vk_hash),
            hex::encode(config.expected_vk_hash)
        );
    }
    if config.sender_bearer_token.trim().is_empty() {
        anyhow::bail!("sender bearer token is required");
    }
    if deposit_fee_denominator(&config.config_params) == 0 {
        anyhow::bail!("deposit fee denominator must be non-zero");
    }
    let expected_bridge_state =
        Pubkey::find_program_address(&[b"bridge_state"], &config.bridge_program).0;
    if config.custody_script_config != expected_bridge_state.to_bytes() {
        anyhow::bail!(
            "custody script config {} does not match bridge-state PDA {expected_bridge_state}",
            Pubkey::new_from_array(config.custody_script_config),
        );
    }
    if config.recipient_atas.is_empty() {
        anyhow::bail!(
            "at least one recipient ATA is required for manager custody auto-claim scanning"
        );
    }
    for (index, key) in config.recipient_atas.iter().enumerate() {
        if config.recipient_atas[..index].contains(key) {
            anyhow::bail!("duplicate recipient ATA at index {index}");
        }
    }
    if config.solana_rpc_url.trim().is_empty() {
        anyhow::bail!("Solana RPC URL is required");
    }
    for (name, path) in [
        ("operator", &config.operator_keypair),
        ("payer", &config.payer_keypair),
    ] {
        if !path.is_file() {
            anyhow::bail!("{name} keypair does not exist at {}", path.display());
        }
    }
    let operator = read_pipeline_keypair(&config.operator_keypair, "operator")?;
    let (expected_mint_buffer, _) = Pubkey::find_program_address(
        &[b"mint_buffer", operator.pubkey().as_ref()],
        &config.pending_mint_program,
    );
    let (expected_txo_buffer, _) = Pubkey::find_program_address(
        &[b"txo_buffer", operator.pubkey().as_ref()],
        &config.txo_buffer_program,
    );
    if expected_mint_buffer == Pubkey::default() || expected_txo_buffer == Pubkey::default() {
        anyhow::bail!("derived buffer PDA must be non-zero");
    }
    Ok(())
}

fn ensure_release_path(path: &Path, name: &str) -> anyhow::Result<()> {
    let components: Vec<_> = path
        .components()
        .map(|component| component.as_os_str())
        .collect();
    let has_release = components.iter().any(|component| *component == "release");
    let has_debug = components.iter().any(|component| *component == "debug");
    if !has_release || has_debug {
        anyhow::bail!(
            "{name} must be loaded from a release path: {}",
            path.display()
        );
    }
    Ok(())
}


fn assert_checkpoint_matches_chain(
    checkpoint: &PipelineCheckpoint,
    chain_header: &PsyBridgeHeader,
) -> anyhow::Result<()> {
    let checkpoint_header = decode_fixed::<HEADER_SIZE>(&checkpoint.header_hex, "checkpoint header")?;
    let chain_header_bytes: &[u8; HEADER_SIZE] = bytemuck::bytes_of(chain_header)
        .try_into()
        .map_err(|_| anyhow::anyhow!("on-chain bridge header has an unexpected size"))?;
    if checkpoint_header != *chain_header_bytes {
        anyhow::bail!(
            "Redis checkpoint height {} does not match the current on-chain bridge header; refusing to prove or submit from stale off-chain state",
            checkpoint.height
        );
    }
    Ok(())
}


fn validate_checkpoint(checkpoint: &PipelineCheckpoint) -> anyhow::Result<()> {
    ensure_length(
        "checkpoint header",
        &hex::decode(&checkpoint.header_hex)?,
        HEADER_SIZE,
    )?;
    if checkpoint.claim_history.len() > 1_000_000 || checkpoint.txo_block_history.len() > 1_000_000
    {
        anyhow::bail!("checkpoint Merkle history exceeds the E2E safety limit");
    }
    if !checkpoint.claim_history.is_empty() {
        let last_index = checkpoint.claim_history.len() - 1;
        let expected = MerkleFrontier::new(
            last_index as u32,
            checkpoint.claim_history[last_index],
            &sparse_sha256_merkle_siblings(
                &checkpoint.claim_history,
                last_index,
                AUTO_CLAIM_DEPOSITS_TREE_HEIGHT,
                0,
            )?,
        );
        if expected.index != checkpoint.claim_frontier.index
            || expected.value_hex != checkpoint.claim_frontier.value_hex
            || expected.siblings_hex != checkpoint.claim_frontier.siblings_hex
        {
            anyhow::bail!("claim history does not reproduce the checkpoint frontier");
        }
    }
    if !checkpoint.txo_block_history.is_empty() {
        if checkpoint.txo_block_history.len() != checkpoint.height as usize + 1 {
            anyhow::bail!("TXO block history length does not match checkpoint height");
        }
        let expected = MerkleFrontier::new(
            checkpoint.height,
            checkpoint.txo_block_history[checkpoint.height as usize],
            &sparse_sha256_merkle_siblings(
                &checkpoint.txo_block_history,
                checkpoint.height as usize,
                TXO_TREE_INDEX_BITS_BLOCK_NUM_LENGTH,
                TXO_BLOCK_FULL_MERKLE_TREE_HEIGHT,
            )?,
        );
        if expected.index != checkpoint.txo_block_frontier.index
            || expected.value_hex != checkpoint.txo_block_frontier.value_hex
            || expected.siblings_hex != checkpoint.txo_block_frontier.siblings_hex
        {
            anyhow::bail!("TXO block history does not reproduce the checkpoint frontier");
        }
    }
    let state_bytes = hex::decode(&checkpoint.state_hex)?;
    let state = BridgeState::try_from_slice(&state_bytes)?;
    if state.get_tip_block_number() != checkpoint.height {
        anyhow::bail!(
            "checkpoint height {} does not match serialized state tip {}",
            checkpoint.height,
            state.get_tip_block_number()
        );
    }
    validate_frontiers_against_state(checkpoint, &state)
}

fn deposit_fee_numerator(config: &[u8; CONFIG_SIZE]) -> u64 {
    u64::from_le_bytes(config[0..8].try_into().expect("fixed config range"))
}

fn deposit_fee_denominator(config: &[u8; CONFIG_SIZE]) -> u64 {
    u64::from_le_bytes(config[8..16].try_into().expect("fixed config range"))
}

fn deposit_flat_fee(config: &[u8; CONFIG_SIZE]) -> u64 {
    u64::from_le_bytes(config[32..40].try_into().expect("fixed config range"))
}

fn decode_hash(value: &str, name: &str) -> anyhow::Result<QHash256> {
    decode_fixed::<32>(value, name)
}

fn decode_fixed<const N: usize>(value: &str, name: &str) -> anyhow::Result<[u8; N]> {
    let bytes = hex::decode(value)?;
    ensure_length(name, &bytes, N)?;
    Ok(bytes.try_into().expect("length was checked"))
}

fn ensure_length(name: &str, bytes: &[u8], expected: usize) -> anyhow::Result<()> {
    if bytes.len() != expected {
        anyhow::bail!("{name} must be {expected} bytes, got {}", bytes.len());
    }
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use doge_light_client::{
        core_data::QStandardBlockHeader,
        doge::transaction::{BTCTransaction, BTCTransactionOutput},
    };
    #[cfg(unix)]
    use tokio::process::Command;

    #[cfg(unix)]
    #[tokio::test]
    async fn daemon_response_wait_reports_early_child_exit() {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("printf diagnostic >&2; exit 23")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn diagnostic child");
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        let mut stderr = child.stderr.take().expect("piped stderr");
        let stderr_bytes = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let task_stderr_bytes = Arc::clone(&stderr_bytes);
        let stderr_task = tokio::spawn(async move {
            tokio::io::AsyncReadExt::read_to_end(&mut stderr, &mut *task_stderr_bytes.lock().await)
                .await
                .map(|_| ())
        });
        let mut daemon = ProverDaemon {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            stderr_task: Some(stderr_task),
            stderr: stderr_bytes,
            vk_hash: [0; 32],
        };

        let error = tokio::time::timeout(Duration::from_secs(2), daemon.read_response_line())
            .await
            .expect("early exit must not hang")
            .expect_err("early exit must fail");
        let message = format!("{error:#}");
        assert!(message.contains("exit status: 23"), "{message}");
        assert!(message.contains("diagnostic"), "{message}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn daemon_response_wait_skips_non_json_startup_banner() {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(r#"printf '%s\n' 'startup banner' '{"kind":"identity"}'; sleep 1"#)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn diagnostic child");
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        let mut stderr = child.stderr.take().expect("piped stderr");
        let stderr_bytes = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let task_stderr_bytes = Arc::clone(&stderr_bytes);
        let stderr_task = tokio::spawn(async move {
            tokio::io::AsyncReadExt::read_to_end(&mut stderr, &mut *task_stderr_bytes.lock().await)
                .await
                .map(|_| ())
        });
        let mut daemon = ProverDaemon {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            stderr_task: Some(stderr_task),
            stderr: stderr_bytes,
            vk_hash: [0; 32],
        };

        let response = daemon
            .read_response_line()
            .await
            .expect("read JSON response");
        assert_eq!(response, br#"{"kind":"identity"}"#);
        assert!(String::from_utf8(daemon.finish_stderr().await)
            .expect("captured diagnostics are UTF-8")
            .contains("startup banner"));
        daemon.terminate().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn daemon_diagnostics_are_drained_per_request() {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 10")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn diagnostic child");
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr_task = tokio::spawn(async { Ok(()) });
        let stderr = Arc::new(tokio::sync::Mutex::new(b"request-one".to_vec()));
        let mut daemon = ProverDaemon {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            stderr_task: Some(stderr_task),
            stderr,
            vk_hash: [0; 32],
        };

        assert_eq!(daemon.take_stderr().await, b"request-one");
        assert!(daemon.take_stderr().await.is_empty());
        daemon.terminate().await;
    }


    const CUSTODY_SCRIPT_CONFIG: CustodyScriptConfig = CustodyScriptConfig::new([7u8; 32]);
    const RECIPIENT_ATA: [u8; 32] = [9u8; 32];

    #[test]
    fn stale_checkpoint_is_rejected_against_on_chain_header() {
        let checkpoint = empty_checkpoint(42);
        let chain_header = PsyBridgeHeader::default();
        assert!(assert_checkpoint_matches_chain(&checkpoint, &chain_header).is_ok());

        let mut stale = checkpoint;
        stale.header_hex = hex::encode([1u8; HEADER_SIZE]);
        let error = assert_checkpoint_matches_chain(&stale, &chain_header).unwrap_err();
        assert!(error.to_string().contains("stale off-chain state"));
    }

    fn empty_checkpoint(height: u32) -> PipelineCheckpoint {
        let claim_siblings: [QHash256; AUTO_CLAIM_DEPOSITS_TREE_HEIGHT] =
            core::array::from_fn(|level| SHA256_ZERO_HASHES[level]);
        let txo_siblings: [QHash256; TXO_TREE_INDEX_BITS_BLOCK_NUM_LENGTH] =
            core::array::from_fn(|level| {
                SHA256_ZERO_HASHES[TXO_BLOCK_FULL_MERKLE_TREE_HEIGHT + level]
            });
        PipelineCheckpoint {
            height,
            state_hex: String::new(),
            header_hex: hex::encode([0u8; HEADER_SIZE]),
            claim_frontier: MerkleFrontier::new(0, SHA256_ZERO_HASHES[0], &claim_siblings),
            pending_finalization: BTreeMap::new(),
            txo_block_frontier: MerkleFrontier::new(
                height,
                SHA256_ZERO_HASHES[TXO_BLOCK_FULL_MERKLE_TREE_HEIGHT],
                &txo_siblings,
            ),
            claim_history: Vec::new(),
            txo_block_history: vec![
                SHA256_ZERO_HASHES[TXO_BLOCK_FULL_MERKLE_TREE_HEIGHT];
                height as usize + 1
            ],
        }
    }

    fn transaction(outputs: Vec<BTCTransactionOutput>) -> BTCTransaction {
        BTCTransaction::from_io(Vec::new(), outputs)
    }

    fn block(height: u32, transactions: Vec<BTCTransaction>) -> QDogeBlock {
        let hashes: Vec<_> = transactions.iter().map(BTCTransaction::get_hash).collect();
        QDogeBlock {
            header: QStandardBlockHeader {
                version: 0,
                previous_block_hash: [height as u8; 32],
                merkle_root: bitcoin_merkle_root(&hashes).unwrap(),
                timestamp: 1_700_000_000 + height,
                bits: 0,
                nonce: height,
            },
            aux_pow: None,
            transactions,
        }
    }

    #[test]
    fn network_profiles_select_distinct_guests_and_vks() {
        assert_eq!(
            DogeNetworkProfile::Regtest.default_vk_hash(),
            REGTEST_BLOCK_VK_HASH
        );
        assert_eq!(
            DogeNetworkProfile::Testnet.default_vk_hash(),
            TESTNET_BLOCK_VK_HASH
        );
        assert_ne!(REGTEST_BLOCK_VK_HASH, TESTNET_BLOCK_VK_HASH);
        assert!(DogeNetworkProfile::Regtest
            .default_block_elf_path()
            .ends_with("block-transition"));
        assert!(DogeNetworkProfile::Testnet
            .default_block_elf_path()
            .ends_with("block-transition-testnet"));
    }

    #[test]
    fn network_profiles_select_expected_manager_custody_hashes() {
        // Shared Bridge State emitter PDA used by the official custody vectors.
        const BRIDGE_STATE_PDA: [u8; 32] = hex_literal::hex!(
            "f02732708965bb9473177495e608496b0af3bdbe5bd62ec062d8cddb1824a813"
        );
        const LOCAL_FIXTURE_HASH: [u8; 32] = hex_literal::hex!(
            "6b6c33fa023611fdd672361f9c198353580959ad34af813af69178d61ca955eb"
        );
        const OFFICIAL_TESTNET_HASH: [u8; 32] = hex_literal::hex!(
            "2621f9ac4de46226f85b48bcf2e20c87e6bb62ff946a9b12becb8c35a4e90ab0"
        );

        assert_eq!(
            DogeNetworkProfile::Regtest.custodian_hash(BRIDGE_STATE_PDA),
            LOCAL_FIXTURE_HASH
        );
        assert_eq!(
            DogeNetworkProfile::Testnet.custodian_hash(BRIDGE_STATE_PDA),
            OFFICIAL_TESTNET_HASH
        );
        assert_eq!(
            CustodyScriptConfig::new(BRIDGE_STATE_PDA).hash::<LocalRegtestManagerCustody>(),
            LOCAL_FIXTURE_HASH
        );
        assert_eq!(
            CustodyScriptConfig::new(BRIDGE_STATE_PDA).hash::<OfficialTestnetManagerCustody>(),
            OFFICIAL_TESTNET_HASH
        );
        assert_ne!(
            get_manager_custody_output_script::<LocalRegtestManagerCustody>(
                &CustodyScriptConfig::new(BRIDGE_STATE_PDA),
                &RECIPIENT_ATA,
            ),
            get_manager_custody_output_script::<OfficialTestnetManagerCustody>(
                &CustodyScriptConfig::new(BRIDGE_STATE_PDA),
                &RECIPIENT_ATA,
            )
        );
    }

    #[test]
    fn high_checkpoint_frontier_advances_without_full_history() {
        let height = 67_765_166;
        let checkpoint = empty_checkpoint(height);
        let next = height + 1;
        let siblings = sequential_next_leaf_siblings(
            &checkpoint.txo_block_frontier,
            next,
            TXO_TREE_INDEX_BITS_BLOCK_NUM_LENGTH,
            TXO_BLOCK_FULL_MERKLE_TREE_HEIGHT,
            "TXO block frontier",
        )
        .unwrap();
        assert_eq!(siblings.len(), TXO_TREE_INDEX_BITS_BLOCK_NUM_LENGTH);
        assert_eq!(checkpoint.txo_block_frontier.index + 1, next);
    }

    #[tokio::test]
    #[ignore = "live QED Dogecoin testnet check"]
    async fn qed_testnet_block_constructs_real_guest_input() {
        const ELECTRS_URL: &str = "https://doge-electrs-testnet-demo.qed.me";
        const CHECKPOINT_HEIGHT: u32 = 67_765_166;
        const BLOCK_HEIGHT: u32 = CHECKPOINT_HEIGHT + 1;

        let rpc = DogeLinkElectrsAsyncClient::new(ELECTRS_URL.to_owned());
        let first = CHECKPOINT_HEIGHT + 1 - PSY_DOGE_BRIDGE_BLOCK_HASH_CACHE_SIZE as u32;
        let headers: [QDogeBlockHeader; PSY_DOGE_BRIDGE_BLOCK_HASH_CACHE_SIZE] = rpc
            .get_qd_block_headers_range_parallel(first, CHECKPOINT_HEIGHT)
            .await
            .unwrap()
            .try_into()
            .unwrap();
        let state = PsyDogeBridgeState::from_init_data(
            &InitBlockDataIBC::new_from_block_headers_empty_tree(&headers, CHECKPOINT_HEIGHT),
        );
        let claim_siblings: [QHash256; AUTO_CLAIM_DEPOSITS_TREE_HEIGHT] =
            core::array::from_fn(|level| SHA256_ZERO_HASHES[level]);
        let txo_siblings: [QHash256; TXO_TREE_INDEX_BITS_BLOCK_NUM_LENGTH] =
            core::array::from_fn(|level| {
                SHA256_ZERO_HASHES[TXO_BLOCK_FULL_MERKLE_TREE_HEIGHT + level]
            });
        let checkpoint = PipelineCheckpoint {
            height: CHECKPOINT_HEIGHT,
            state_hex: hex::encode(borsh::to_vec(&state).unwrap()),
            header_hex: hex::encode([0u8; HEADER_SIZE]),
            claim_frontier: MerkleFrontier::new(0, SHA256_ZERO_HASHES[0], &claim_siblings),
            txo_block_frontier: MerkleFrontier::new(
                CHECKPOINT_HEIGHT,
                SHA256_ZERO_HASHES[TXO_BLOCK_FULL_MERKLE_TREE_HEIGHT],
                &txo_siblings,
            ),
            claim_history: Vec::new(),
            txo_block_history: Vec::new(),
            pending_finalization: BTreeMap::new(),
        };
        let block = rpc.get_qd_block(BLOCK_HEIGHT).await.unwrap();
        assert_eq!(block.header.previous_block_hash, state.get_tip_block_hash());
        let witness = build_deposit_claim_witness::<OfficialTestnetManagerCustody>(
            &block,
            &checkpoint,
            &CUSTODY_SCRIPT_CONFIG,
            &[RECIPIENT_ATA],
        )
        .unwrap();
        let witness_bytes = witness.write_to_vec().unwrap();
        let old_state_bytes = borsh::to_vec(&state).unwrap();
        let mut verified_state = state;
        prover_guest_verify_block_transition_detailed::<
            DogeTestNetConfig,
            OfficialTestnetManagerCustody,
        >(
            CUSTODY_SCRIPT_CONFIG,
            1,
            witness,
            &mut verified_state,
            0,
            0,
            1,
        )
        .unwrap();
        assert_eq!(verified_state.get_tip_block_number(), BLOCK_HEIGHT);

        let root = PathBuf::from("/tmp/psy-doge-qed-block-input-67765167");
        tokio::fs::create_dir_all(&root).await.unwrap();
        tokio::fs::write(root.join("old_state.bin"), &old_state_bytes)
            .await
            .unwrap();
        tokio::fs::write(root.join("witness.bin"), &witness_bytes)
            .await
            .unwrap();
        let manifest = serde_json::json!({
            "network": "testnet",
            "electrs_url": ELECTRS_URL,
            "checkpoint_height": CHECKPOINT_HEIGHT,
            "block_height": BLOCK_HEIGHT,
            "block_hash": hex::encode(block.header.get_hash()),
            "old_state_size": old_state_bytes.len(),
            "old_state_sha256": sha256_hex(&old_state_bytes),
            "witness_size": witness_bytes.len(),
            "witness_sha256": sha256_hex(&witness_bytes),
            "guest_input_verified": true,
        });
        tokio::fs::write(
            root.join("manifest.json"),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .await
        .unwrap();
        println!("{}", serde_json::to_string(&manifest).unwrap());
    }
    #[test]
    fn manager_custody_deposit_advances_auto_claim_and_mint_commitments() {
        let height = 101;
        let checkpoint = empty_checkpoint(height - 1);
        let coinbase = transaction(vec![BTCTransactionOutput {
            value: 5_000_000_000,
            script: vec![0x51],
        }]);
        let deposit_amount = 100_000_000;
        let redeem_script =
            get_manager_custody_redeem_script::<LocalRegtestManagerCustody>(&CUSTODY_SCRIPT_CONFIG, &RECIPIENT_ATA);
        assert_eq!(redeem_script.len(), MANAGER_CUSTODY_REDEEM_SCRIPT_SIZE);
        let deposit = transaction(vec![BTCTransactionOutput {
            value: deposit_amount,
            script: get_manager_custody_output_script::<LocalRegtestManagerCustody>(&CUSTODY_SCRIPT_CONFIG, &RECIPIENT_ATA)
                .to_vec(),
        }]);
        let block = block(height, vec![coinbase, deposit]);

        let witness = build_deposit_claim_witness::<LocalRegtestManagerCustody>(
            &block,
            &checkpoint,
            &CUSTODY_SCRIPT_CONFIG,
            &[RECIPIENT_ATA],
        )
        .unwrap();
        assert_eq!(witness.claim_witness.header.total_outputs_hint, 1);
        assert_eq!(witness.claim_witness.deposit_transactions.len(), 1);
        assert_eq!(
            witness.claim_witness.deposit_transactions[0].transaction_index,
            1
        );

        let evaluation = evaluate_claim_witness::<LocalRegtestManagerCustody>(
            height,
            &witness.claim_witness,
            block.header.merkle_root,
            &CUSTODY_SCRIPT_CONFIG,
            0,
            0,
            100,
        )
        .unwrap();
        assert_eq!(evaluation.deposit_count, 1);
        assert_eq!(evaluation.minted_amount_sats, deposit_amount);
        assert_eq!(evaluation.pending_mints.len(), 1);
        assert_eq!(evaluation.pending_mints[0].recipient, RECIPIENT_ATA);
        assert_eq!(evaluation.pending_mints[0].amount, deposit_amount);
        assert_eq!(evaluation.txo_indices, vec![4095]);
        let finalized = BlockBufferCommitment::from_evaluation(&evaluation);
        let bridge_mints = finalized
            .pending_mints
            .iter()
            .map(PendingMintPayload::to_bridge_mint)
            .collect::<Vec<_>>();
        validate_buffer_payload(&finalized, &bridge_mints).unwrap();
        assert_eq!(evaluation.transition.start_auto_claimed_deposits_index, 0);
        assert_eq!(evaluation.transition.end_auto_claimed_deposits_index, 1);
        assert_ne!(
            evaluation.transition.new_auto_claimed_deposits_tree_root,
            evaluation.transition.old_auto_claimed_deposits_tree_root
        );
        assert_ne!(
            evaluation.transition.new_claimed_txo_tree_root,
            evaluation.transition.old_claimed_txo_tree_root
        );
        let empty_mints = PendingMintsGroupsBuilder::new_with_hint(0)
            .finalize()
            .unwrap();
        assert_ne!(evaluation.pending_mints_hash, empty_mints);
        assert_ne!(evaluation.txo_output_list_hash, hash_impl_sha256_bytes(&[]));
    }

    #[test]
    fn legacy_59_byte_custody_output_is_not_scanned_as_manager_deposit() {
        let height = 102;
        let checkpoint = empty_checkpoint(height - 1);
        let legacy_output = psy_doge_bridge_helper::tx_template::get_bridge_deposit_output_script(
            &RECIPIENT_ATA,
            &[8u8; 20],
        );
        let block = block(
            height,
            vec![transaction(vec![BTCTransactionOutput {
                value: 100_000_000,
                script: legacy_output.to_vec(),
            }])],
        );

        let witness = build_deposit_claim_witness::<LocalRegtestManagerCustody>(
            &block,
            &checkpoint,
            &CUSTODY_SCRIPT_CONFIG,
            &[RECIPIENT_ATA],
        )
        .unwrap();

        assert_eq!(witness.claim_witness.header.total_outputs_hint, 0);
        assert!(witness.claim_witness.deposit_transactions.is_empty());
    }

    #[test]
    fn live_deposit_evidence_requires_exact_312_byte_manager_script() {
        let height = 103;
        let deposit = transaction(vec![BTCTransactionOutput {
            value: 100_000_000,
            script: get_manager_custody_output_script::<LocalRegtestManagerCustody>(&CUSTODY_SCRIPT_CONFIG, &RECIPIENT_ATA)
                .to_vec(),
        }]);
        let txid = {
            let mut txid = deposit.get_hash();
            txid.reverse();
            hex::encode(txid)
        };
        let block = block(height, vec![deposit]);
        let root = std::env::temp_dir().join(format!(
            "psy-manager-deposit-evidence-test-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("unnamed")
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("deposit.json");
        let exact_redeem =
            get_manager_custody_redeem_script::<LocalRegtestManagerCustody>(&CUSTODY_SCRIPT_CONFIG, &RECIPIENT_ATA);
        let output = get_manager_custody_output_script::<LocalRegtestManagerCustody>(&CUSTODY_SCRIPT_CONFIG, &RECIPIENT_ATA);
        let evidence = serde_json::json!({
            "deposit": { "txid": txid, "vout": 0, "confirmation_height": height },
            "custody": {
                "original_recipient_address_hex": hex::encode(RECIPIENT_ATA),
                "script_pubkey_hex": hex::encode(output),
                "redeem_script_hex": hex::encode(exact_redeem),
            }
        });
        std::fs::write(&path, serde_json::to_vec(&evidence).unwrap()).unwrap();
        validate_live_deposit_script::<LocalRegtestManagerCustody>(
            Some(&path),
            height,
            &block,
            &CUSTODY_SCRIPT_CONFIG,
            &[RECIPIENT_ATA],
        )
        .unwrap();

        let pending = serde_json::json!({
            "deposit": { "txid": evidence["deposit"]["txid"], "vout": 0, "status": "BROADCAST_PENDING_CONFIRMATION" },
            "custody": {
                "original_recipient_address_hex": hex::encode(RECIPIENT_ATA),
                "script_pubkey_hex": hex::encode(output),
                "redeem_script_hex": hex::encode(exact_redeem),
            }
        });
        std::fs::write(&path, serde_json::to_vec(&pending).unwrap()).unwrap();
        validate_live_deposit_script::<LocalRegtestManagerCustody>(
            Some(&path),
            height,
            &block,
            &CUSTODY_SCRIPT_CONFIG,
            &[RECIPIENT_ATA],
        )
        .unwrap();

        let mut mismatched = exact_redeem;
        mismatched[0] ^= 1;
        let evidence = serde_json::json!({
            "deposit": { "txid": evidence["deposit"]["txid"], "vout": 0, "confirmation_height": height },
            "custody": {
                "original_recipient_address_hex": hex::encode(RECIPIENT_ATA),
                "script_pubkey_hex": hex::encode(output),
                "redeem_script_hex": hex::encode(mismatched),
            }
        });
        std::fs::write(&path, serde_json::to_vec(&evidence).unwrap()).unwrap();
        let error = validate_live_deposit_script::<LocalRegtestManagerCustody>(
            Some(&path),
            height,
            &block,
            &CUSTODY_SCRIPT_CONFIG,
            &[RECIPIENT_ATA],
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("exact 312-byte deterministic 5-of-7"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn no_deposit_block_does_not_forge_mint_or_advance_claim_index() {
        let height = 201;
        let checkpoint = empty_checkpoint(height - 1);
        let ordinary = transaction(vec![BTCTransactionOutput {
            value: 42,
            script: vec![0x51],
        }]);
        let block = block(height, vec![ordinary]);

        let witness = build_deposit_claim_witness::<LocalRegtestManagerCustody>(
            &block,
            &checkpoint,
            &CUSTODY_SCRIPT_CONFIG,
            &[RECIPIENT_ATA],
        )
        .unwrap();
        let evaluation = evaluate_claim_witness::<LocalRegtestManagerCustody>(
            height,
            &witness.claim_witness,
            block.header.merkle_root,
            &CUSTODY_SCRIPT_CONFIG,
            0,
            0,
            100,
        )
        .unwrap();

        assert_eq!(evaluation.deposit_count, 0);
        assert_eq!(evaluation.minted_amount_sats, 0);
        assert_eq!(evaluation.transition.start_auto_claimed_deposits_index, 0);
        assert_eq!(evaluation.transition.end_auto_claimed_deposits_index, 0);
        assert_eq!(
            evaluation.transition.new_auto_claimed_deposits_tree_root,
            evaluation.transition.old_auto_claimed_deposits_tree_root
        );
        assert_eq!(
            evaluation.transition.new_claimed_txo_tree_root,
            evaluation.transition.old_claimed_txo_tree_root
        );
        let empty_mints = PendingMintsGroupsBuilder::new_with_hint(0)
            .finalize()
            .unwrap();
        assert_eq!(evaluation.pending_mints_hash, empty_mints);
        assert_eq!(evaluation.txo_output_list_hash, hash_impl_sha256_bytes(&[]));
        assert!(evaluation.pending_mints.is_empty());
        assert!(evaluation.txo_indices.is_empty());
    }

    #[test]
    fn finalized_buffers_are_delayed_until_the_source_height_finalizes() {
        let evaluation = ClaimEvaluation {
            transition: PsyBridgeClaimBlockWitnessVerifyResult {
                old_claimed_txo_tree_root: [1u8; 32],
                new_claimed_txo_tree_root: [2u8; 32],
                old_auto_claimed_deposits_tree_root: [3u8; 32],
                new_auto_claimed_deposits_tree_root: [4u8; 32],
                start_auto_claimed_deposits_index: 8,
                end_auto_claimed_deposits_index: 9,
                fees_collected: 10,
            },
            pending_mints_hash: [5u8; 32],
            txo_output_list_hash: [6u8; 32],
            pending_mints: vec![PendingMintPayload {
                recipient: [9u8; 32],
                amount: 99,
            }],
            txo_indices: vec![17],
            block_txo_root: [7u8; 32],
            deposit_leaf_hashes: vec![[8u8; 32]],
            deposit_count: 1,
            minted_amount_sats: 99,
        };
        let source = BlockBufferCommitment::from_evaluation(&evaluation);
        let empty = BlockBufferCommitment::empty().unwrap();
        let mut pending = BTreeMap::new();
        pending.insert(300, source.clone());

        assert_eq!(
            pending
                .get(&299)
                .cloned()
                .unwrap_or(empty.clone())
                .deposit_count,
            0
        );
        let finalized = pending.get(&300).cloned().unwrap_or(empty);
        assert_eq!(finalized.deposit_count, 1);
        assert_eq!(finalized.minted_amount_sats, 99);
        assert_eq!(finalized.auto_claim_end_index, 9);
        assert_eq!(finalized.pending_mints_hash().unwrap(), [5u8; 32]);
        assert_eq!(finalized.txo_output_list_hash().unwrap(), [6u8; 32]);
    }

    #[test]
    fn bitcoin_merkle_paths_verify_for_even_and_odd_widths() {
        for count in 1..=7 {
            let transactions: Vec<_> = (0..count)
                .map(|index| {
                    transaction(vec![BTCTransactionOutput {
                        value: index as u64 + 1,
                        script: vec![index as u8],
                    }])
                })
                .collect();
            let hashes: Vec<_> = transactions.iter().map(BTCTransaction::get_hash).collect();
            let root = bitcoin_merkle_root(&hashes).unwrap();
            for (index, hash) in hashes.iter().enumerate() {
                let siblings = bitcoin_merkle_siblings(&hashes, index).unwrap();
                let verified = doge_light_client::hash::merkle::in_memory::compute_dogecoin_block_transaction_merkle_proof_tree_root_hash256(
                    *hash,
                    &siblings,
                    index as u32,
                );
                assert_eq!(verified, Some(root), "count={count}, index={index}");
            }
        }
    }

    #[test]
    fn history_frontier_supports_two_deposits_across_blocks() {
        let first_leaf = [11u8; 32];
        let second_leaf = [12u8; 32];
        let first_history = vec![first_leaf];
        let first_frontier = MerkleFrontier::new(
            0,
            first_leaf,
            &sparse_sha256_merkle_siblings(&first_history, 0, AUTO_CLAIM_DEPOSITS_TREE_HEIGHT, 0)
                .unwrap(),
        );
        let second_history = vec![first_leaf, second_leaf];
        let second_frontier = MerkleFrontier::new(
            1,
            second_leaf,
            &sparse_sha256_merkle_siblings(&second_history, 1, AUTO_CLAIM_DEPOSITS_TREE_HEIGHT, 0)
                .unwrap(),
        );

        let first_root =
            frontier_root(&first_frontier, AUTO_CLAIM_DEPOSITS_TREE_HEIGHT, "first").unwrap();
        let second_root =
            frontier_root(&second_frontier, AUTO_CLAIM_DEPOSITS_TREE_HEIGHT, "second").unwrap();
        assert_ne!(first_root, second_root);
        let rebuilt =
            psy_doge_bridge_helper::utils::append_only_merkle_tree::AppendOnlyMerkleTreeFixed::<
                AUTO_CLAIM_DEPOSITS_TREE_HEIGHT,
                0,
            >::new_from_siblings(
                &first_frontier
                    .decode(AUTO_CLAIM_DEPOSITS_TREE_HEIGHT, "first")
                    .unwrap()
                    .1,
                0,
                &first_leaf,
            )
            .unwrap();
        assert_eq!(rebuilt.start_root, first_root);
        assert_eq!(rebuilt.next_index, 1);
    }

    #[tokio::test]
    async fn evidence_manifest_writes_stable_latest_json() {
        let root = std::env::temp_dir().join(format!(
            "psy-block-evidence-test-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("unnamed")
        ));
        let evidence_dir = root.join("height-7").join("abc");
        tokio::fs::create_dir_all(&evidence_dir).await.unwrap();
        let mut manifest = BlockProofEvidence {
            schema_version: EVIDENCE_SCHEMA_VERSION,
            status: "proof_generated".to_owned(),
            height: 7,
            block_hash: "11".repeat(32),
            finalized_source_height: 6,
            source_witness_height: 7,
            witness_deposit_count: 1,
            witness_minted_amount_sats: 100,
            deposit_count: 1,
            minted_amount_sats: 100,
            auto_claim_start_index: 0,
            auto_claim_end_index: 1,
            fees_collected: 0,
            content_sha256: "22".repeat(32),
            manifest_sha256: String::new(),
            evidence_dir: evidence_dir.display().to_string(),
            manifest_path: evidence_dir.join("manifest.json").display().to_string(),
            latest_path: root.join("latest.json").display().to_string(),
            idempotency_key: "test".to_owned(),
            mint_buffer: None,
            mint_buffer_bump: None,
            txo_buffer: None,
            txo_buffer_bump: None,
            buffer_upload_completed: false,
            submission_signature: None,
            mint_group_signatures: Vec::new(),
            mint_groups_processed: 0,
            total_mints_processed: 0,
            vk_hash: "33".repeat(32),
            elf_sha256: "44".repeat(32),
            artifacts: BTreeMap::new(),
        };
        write_evidence_manifest(&mut manifest).await.unwrap();
        let latest = tokio::fs::read(&manifest.latest_path).await.unwrap();
        let decoded: BlockProofEvidence = serde_json::from_slice(&latest).unwrap();
        assert_eq!(decoded.height, 7);
        assert_eq!(decoded.deposit_count, 1);
        assert_eq!(decoded.manifest_sha256.len(), 64);
        assert_eq!(decoded.evidence_dir, evidence_dir.display().to_string());
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[test]
    fn evidence_bundle_hash_changes_with_proof_bytes() {
        let first = artifact_bundle_sha256(&[("proof.bin", &[1u8; PROOF_SIZE])]);
        let second = artifact_bundle_sha256(&[("proof.bin", &[2u8; PROOF_SIZE])]);
        assert_ne!(first, second);
    }
}
