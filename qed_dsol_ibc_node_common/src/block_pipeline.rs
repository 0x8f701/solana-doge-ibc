use std::{
    collections::BTreeMap,
    future::Future,
    path::{Path, PathBuf},
    process::Stdio,
    str::FromStr,
    sync::{
        atomic::{AtomicU32, AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};
use doge_bridge_client::{
    BridgeApi, BridgeClient, BridgeClientConfigBuilder, BridgeError, PendingMint,
    ProcessMintsResult, PsyBridgeHeader,
};
use borsh::BorshDeserialize;
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
    doge::transaction::BTCTransaction,
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
                    combined_txo_index, pending_mint_and_txo_hashes_from_claim_witness,
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
use solana_client::{client_error::ClientErrorKind, nonblocking::rpc_client::RpcClient as SolanaRpcClient};
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
    network_retry::{has_retryable_io_source, is_retryable_reqwest, NetworkRetryPolicy, RetryAction},
    proof_queue::{
        file_sha256_hex, proof_namespace, ProofBackend, ProofJob, ProofRequest, ProofTicket,
        ProverBackendKind,
    },
    sol_submitter::{BlockUpdateRequestBody, SolSubmitterClient},
};

const HEADER_SIZE: usize = 320;
const CONFIG_SIZE: usize = 48;
const CUSTODY_SCRIPT_CONFIG_SIZE: usize = 32;
const PROOF_SIZE: usize = 356;
const PUBLIC_VALUES_SIZE: usize = 32;
const CHECKPOINT_PREFIX: &str = "PDOGE-E2E-BLOCK-CHECKPOINT-V3";
const CHECKPOINT_JOURNAL_SUFFIX: &str = "pending";
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

    /// Stable path-independent SP1 guest identifier reported by the gen-proof
    /// daemon. The pipeline validates the daemon identity by this id plus the
    /// embedded ELF SHA-256 and verifying key, not by the daemon's local path.
    pub const fn guest_id(self) -> &'static str {
        match self {
            Self::Regtest => "block-transition",
            Self::Testnet => "block-transition-testnet",
        }
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
pub struct BlockPipelineConfig {
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
    pub prover_backend: crate::proof_queue::ProverBackendKind,
    pub proof_prepare_window: usize,
    pub proof_queue_prefix: String,
    pub proof_wait_interval: Duration,
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
struct CheckpointJournal {
    checkpoint: PipelineCheckpoint,
    #[serde(default)]
    uploaded_buffers: Option<UploadedBuffers>,
    /// The finalized source-height buffer commitment that was removed from
    /// `checkpoint.pending_finalization`. It is required to re-run mint
    /// recovery after a crash that follows `block_update`.
    #[serde(default)]
    finalized_commitment: Option<BlockBufferCommitment>,
    /// Sender signature for the committed `block_update`, persisted once the
    /// sender accepts the submission.
    #[serde(default)]
    submission_signature: Option<String>,
    /// Mint processing progress, persisted once all mint groups are
    /// processed. Its presence means the height is fully recovered.
    #[serde(default)]
    mint_progress: Option<MintProgressRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct MintProgressRecord {
    signatures: Vec<String>,
    groups_processed: usize,
    total_mints_processed: usize,
}

impl From<&MintProcessingEvidence> for MintProgressRecord {
    fn from(evidence: &MintProcessingEvidence) -> Self {
        Self {
            signatures: evidence.signatures.clone(),
            groups_processed: evidence.groups_processed,
            total_mints_processed: evidence.total_mints_processed,
        }
    }
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

pub(crate) struct ProverOutput {
    pub(crate) stdout: Vec<u8>,
    pub(crate) stderr: Vec<u8>,
    pub(crate) vk_hash: [u8; 32],
    pub(crate) proof: Vec<u8>,
    pub(crate) public_values: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct ProverRequestError(pub(crate) String);

impl std::fmt::Display for ProverRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ProverRequestError {}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ProofDisposition {
    Preparing,
    Waiting,
    Failed,
    Validated,
}

const fn commit_may_start(disposition: ProofDisposition) -> bool {
    matches!(disposition, ProofDisposition::Validated)
}

#[derive(Debug, Deserialize)]
struct ProverIdentityResponse {
    kind: String,
    network: String,
    guest_id: String,
    block_elf_sha256: String,
    vkey_hash: String,
}

#[derive(Debug, Deserialize)]
struct ProverProofResponse {
    kind: String,
    // The daemon always tags a successful proof response with `request_id`;
    // an error response may omit it, so deserialize both via `Option`.
    request_id: Option<String>,
    ok: bool,
    network: Option<String>,
    guest_id: Option<String>,
    block_elf_sha256: Option<String>,
    vkey_hash: Option<String>,
    proof_size: Option<usize>,
    proof_bytes: Option<String>,
    public_values_size: Option<usize>,
    public_values: Option<String>,
    error: Option<String>,
}


const MAX_PROVER_DIAGNOSTICS_BYTES: usize = 256 * 1024;
/// SP1 daemon phase budgets passed explicitly to `gen-proof` so the IBC and
/// the prover share one timeout contract. The daemon self-exits 75 on its own
/// phase timeout; the IBC deadlines below are strictly larger than these so a
/// normal long proof is never killed before the daemon's own deadline.
const SP1_SETUP_TIMEOUT_SECS: u64 = 600;
const SP1_EXECUTE_TIMEOUT_SECS: u64 = 900;
const SP1_PROVE_TIMEOUT_SECS: u64 = 7_200;
/// Headroom applied on top of the SP1 phase budgets when deriving IBC
/// deadlines, so the IBC never fires before the daemon's own timeout.
const PROVER_DEADLINE_HEADROOM_SECS: u64 = 300;
/// IBC identity budget: SP1 setup + headroom.
const PROVER_IDENTITY_DEADLINE: Duration =
    Duration::from_secs(SP1_SETUP_TIMEOUT_SECS + PROVER_DEADLINE_HEADROOM_SECS);
/// Per-attempt budget for writing a proof request line to the daemon stdin.
const PROVER_STDIN_WRITE_DEADLINE: Duration = Duration::from_secs(60);
/// Per-attempt budget for reading a single proof response line. Covers the
/// daemon's execute + prove phases plus headroom.
const PROVER_RESPONSE_DEADLINE: Duration = Duration::from_secs(
    SP1_EXECUTE_TIMEOUT_SECS + SP1_PROVE_TIMEOUT_SECS + PROVER_DEADLINE_HEADROOM_SECS,
);
/// Overall budget for a single proof attempt (stdin write + response read).
const PROVER_PROOF_DEADLINE: Duration = Duration::from_secs(
    60 + SP1_EXECUTE_TIMEOUT_SECS + SP1_PROVE_TIMEOUT_SECS + PROVER_DEADLINE_HEADROOM_SECS,
);
/// Overall wall-clock watchdog for a single `poll_once` iteration. Covers the
/// identity/setup phase plus at most two proof attempts (run_gen_proof
/// restarts the daemon once), then buffer upload, sender submission, and
/// mint processing, with headroom. This must exceed a single normal proof so
/// the watchdog never kills a healthy prove.
const POLL_ONCE_WATCHDOG: Duration = Duration::from_secs(
    (SP1_SETUP_TIMEOUT_SECS + PROVER_DEADLINE_HEADROOM_SECS)
        + 2 * (60 + SP1_EXECUTE_TIMEOUT_SECS + SP1_PROVE_TIMEOUT_SECS + PROVER_DEADLINE_HEADROOM_SECS)
        + 1_800,
);
/// Redis TCP/TLS connection and internal-command budget.
const REDIS_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
/// Budget applied to every Redis command by fred's `default_command_timeout`.
const REDIS_COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
/// Max time a frame may wait without a response before fred closes the
/// connection as unresponsive and reconnects.
const REDIS_UNRESPONSIVE_TIMEOUT: Duration = Duration::from_secs(20);
/// Top-level deadline for Redis pool initialization.
const REDIS_INIT_DEADLINE: Duration = Duration::from_secs(30);
/// Top-level deadline for critical Redis checkpoint read/write operations.
const REDIS_CHECKPOINT_DEADLINE: Duration = Duration::from_secs(30);

pub(crate) struct ProverDaemon {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    stderr_task: Option<JoinHandle<std::io::Result<()>>>,
    stderr: Arc<tokio::sync::Mutex<Vec<u8>>>,
    vk_hash: [u8; 32],
}

impl ProverDaemon {
    pub(crate) async fn start(config: &BlockPipelineConfig) -> anyhow::Result<Self> {
        let mut command = Command::new(&config.gen_proof_path);
        command
            .arg("--network")
            .arg(config.network.as_str())
            .arg("--daemon")
            .arg("--setup-timeout-secs")
            .arg(SP1_SETUP_TIMEOUT_SECS.to_string())
            .arg("--execute-timeout-secs")
            .arg(SP1_EXECUTE_TIMEOUT_SECS.to_string())
            .arg("--prove-timeout-secs")
            .arg(SP1_PROVE_TIMEOUT_SECS.to_string())
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
        let identity_line = match daemon
            .read_response_line_within(PROVER_IDENTITY_DEADLINE)
            .await
        {
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

    pub(crate) async fn prove(
        &mut self,
        config: &BlockPipelineConfig,
        request: &crate::proof_queue::ProofRequest,
    ) -> anyhow::Result<ProverOutput> {
        let request_id = &request.request_id;
        let mut request_line = serde_json::to_vec(request)?;
        request_line.push(b'\n');

        let response_line = tokio::time::timeout(PROVER_PROOF_DEADLINE, async {
            tokio::time::timeout(
                PROVER_STDIN_WRITE_DEADLINE,
                self.stdin.write_all(&request_line),
            )
            .await
            .map_err(|_| anyhow::anyhow!("gen-proof daemon stdin write timed out after {PROVER_STDIN_WRITE_DEADLINE:?}"))??;
            tokio::time::timeout(PROVER_STDIN_WRITE_DEADLINE, self.stdin.flush())
                .await
                .map_err(|_| anyhow::anyhow!("gen-proof daemon stdin flush timed out after {PROVER_STDIN_WRITE_DEADLINE:?}"))??;

            self.read_response_line_within(PROVER_RESPONSE_DEADLINE)
                .await
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!("gen-proof request {request_id} timed out after {PROVER_PROOF_DEADLINE:?}")
        })??;

        let response: ProverProofResponse = serde_json::from_slice(&response_line)
            .map_err(|error| anyhow::anyhow!("malformed gen-proof response: {error}"))?;
        if response.kind != "proof" {
            anyhow::bail!(
                "gen-proof response kind was '{}', expected 'proof'",
                response.kind
            );
        }
        if response.request_id.as_deref() != Some(request_id) {
            anyhow::bail!(
                "gen-proof response request id {:?} did not match '{request_id}'",
                response.request_id
            );
        }
        if !response.ok {
            let error_message = response
                .error
                .as_deref()
                .unwrap_or("missing error message")
                .to_owned();
            // A daemon-reported timeout (the SP1 process exits 75 after a
            // CUDA phase timeout) is recoverable by restarting the daemon
            // once. Surface it as a plain timeout error rather than a fatal
            // ProverRequestError so run_gen_proof kills and restarts the
            // daemon; other daemon-reported failures fail closed.
            if error_message.contains("timed out") {
                return Err(anyhow::anyhow!(
                    "gen-proof request {request_id} timed out: {error_message}"
                ));
            }
            return Err(anyhow::Error::new(ProverRequestError(format!(
                "gen-proof request {request_id} failed: {error_message}"
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
        let stderr = self.take_stderr().await;
        Ok(ProverOutput {
            stdout: response_line,
            stderr,
            vk_hash: self.vk_hash,
            proof,
            public_values,
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

    /// Read the next protocol response line, bounded by a hard wall-clock
    /// deadline so an alive-but-silent daemon cannot block the pipeline.
    async fn read_response_line_within(
        &mut self,
        deadline: Duration,
    ) -> anyhow::Result<Vec<u8>> {
        tokio::time::timeout(deadline, self.read_response_line())
            .await
            .map_err(|_| {
                anyhow::anyhow!("gen-proof daemon did not respond within {deadline:?}")
            })?
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

    pub(crate) async fn terminate(mut self) {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

struct PreparedTransition {
    height: u32,
    next_checkpoint: PipelineCheckpoint,
    proof_job: ProofJob,
    old_state_bytes: Vec<u8>,
    witness_bytes: Vec<u8>,
    old_header: [u8; HEADER_SIZE],
    new_header: [u8; HEADER_SIZE],
    evaluation: ClaimEvaluation,
    finalized_height: u32,
    finalized_buffers: BlockBufferCommitment,
    idempotency_key: String,
}

struct InflightProof {
    prepared: PreparedTransition,
    ticket: ProofTicket,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SimulatedProofState {
    Pending,
    Ready,
    Failed,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum OrderedProofAction {
    Idle,
    Wait(u32),
    Commit(u32),
    Fail(u32),
}

fn ordered_proof_action(
    committed_height: u32,
    inflight: &BTreeMap<u32, SimulatedProofState>,
) -> OrderedProofAction {
    let Some(expected) = committed_height.checked_add(1) else {
        return OrderedProofAction::Idle;
    };
    match inflight.get(&expected) {
        None => OrderedProofAction::Idle,
        Some(SimulatedProofState::Pending) => OrderedProofAction::Wait(expected),
        Some(SimulatedProofState::Ready) => OrderedProofAction::Commit(expected),
        Some(SimulatedProofState::Failed) => OrderedProofAction::Fail(expected),
    }
}

pub struct BlockPipeline {
    config: BlockPipelineConfig,
    block_rpc: DogeLinkElectrsAsyncClient,
    sender: SolSubmitterClient,
    bridge_client: BridgeClient,
    redis: fred::prelude::Pool,
    checkpoint_key: String,
    checkpoint: PipelineCheckpoint,
    proof_backend: ProofBackend,
    planned_checkpoint: PipelineCheckpoint,
    inflight: BTreeMap<u32, InflightProof>,
    network_retry: NetworkRetryPolicy,
}

impl BlockPipeline {
    pub async fn initialize(config: BlockPipelineConfig) -> anyhow::Result<Self> {
        validate_config(&config)?;
        let redis = build_redis_pool(&config)?;
        tokio::time::timeout(REDIS_INIT_DEADLINE, redis.init())
            .await
            .map_err(|_| anyhow::anyhow!("redis init exceeded {REDIS_INIT_DEADLINE:?}"))??;
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
        let network_retry = NetworkRetryPolicy::default();
        let chain_state = retry_bridge_network(network_retry, "Solana bridge-state lookup", || {
            bridge_client.get_current_bridge_state()
        })
        .await?;
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
        let checkpoint = match tokio::time::timeout(
            REDIS_CHECKPOINT_DEADLINE,
            redis.get::<Option<String>, _>(&checkpoint_key),
        )
        .await
        .map_err(|_| anyhow::anyhow!("redis checkpoint load exceeded {REDIS_CHECKPOINT_DEADLINE:?}"))??
        {
            Some(value) => serde_json::from_str(&value)?,
            None => initialize_checkpoint(&config, &block_rpc).await?,
        };
        let checkpoint = reconcile_checkpoint_journal(
            &redis,
            &checkpoint_key,
            checkpoint,
            &chain_state.bridge_header,
            &bridge_client,
            network_retry,
        )
        .await?;
        assert_checkpoint_matches_chain(&checkpoint, &chain_state.bridge_header)?;

        let proof_backend = match config.prover_backend {
            ProverBackendKind::Local => ProofBackend::local(config.clone()).await?,
            ProverBackendKind::Redis => ProofBackend::redis(
                redis.clone(),
                proof_namespace(
                    &config.proof_queue_prefix,
                    config.network.as_str(),
                    config.redis_seed,
                ),
                config.proof_wait_interval,
            ),
        };
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
            planned_checkpoint: checkpoint.clone(),
            checkpoint,
            config,
            proof_backend,
            inflight: BTreeMap::new(),
            network_retry,
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
        self.proof_backend.shutdown().await;
    }

    pub async fn poll_once(&mut self) -> anyhow::Result<bool> {
        const PHASE_FETCH_TIP: u8 = 0;
        const PHASE_PREPARE: u8 = 1;
        const PHASE_WAIT_PROOF: u8 = 2;
        const PHASE_COMMIT: u8 = 3;
        const POLL_PHASE_NAMES: [&str; 4] = [
            "fetch-electrs-tip",
            "prepare-height",
            "wait-proof",
            "commit-prepared",
        ];
        let phase = Arc::new(AtomicU8::new(PHASE_FETCH_TIP));
        let in_flight_height = Arc::new(AtomicU32::new(0));
        let body_phase = Arc::clone(&phase);
        let body_height = Arc::clone(&in_flight_height);

        let body = async move {
            body_phase.store(PHASE_FETCH_TIP, Ordering::Relaxed);
            let electrs_tip = self.block_rpc.get_block_height().await?;
            let finalized_tip = electrs_tip.saturating_sub(self.config.required_confirmations);
            let mut did_work = false;

            while self.inflight.len() < self.config.proof_prepare_window
                && self.planned_checkpoint.height < finalized_tip
            {
                let height = self
                    .planned_checkpoint
                    .height
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("planned checkpoint height overflow"))?;
                body_height.store(height, Ordering::Relaxed);
                body_phase.store(PHASE_PREPARE, Ordering::Relaxed);
                let base_checkpoint = self.planned_checkpoint.clone();
                let prepared = match self.config.network {
                    DogeNetworkProfile::Regtest => {
                        self.prepare_height::<DogeRegTestConfig, LocalRegtestManagerCustody>(
                            &base_checkpoint,
                            height,
                        )
                        .await?
                    }
                    DogeNetworkProfile::Testnet => {
                        self.prepare_height::<DogeTestNetConfig, OfficialTestnetManagerCustody>(
                            &base_checkpoint,
                            height,
                        )
                        .await?
                    }
                };
                let ticket = self
                    .proof_backend
                    .submit(prepared.proof_job.clone())
                    .await?;
                self.planned_checkpoint = prepared.next_checkpoint.clone();
                if self
                    .inflight
                    .insert(height, InflightProof { prepared, ticket })
                    .is_some()
                {
                    anyhow::bail!("duplicate in-flight proof height {height}");
                }
                did_work = true;
            }

            let expected_height = self
                .checkpoint
                .height
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("checkpoint height overflow"))?;
            if let Some(inflight) = self.inflight.remove(&expected_height) {
                body_height.store(expected_height, Ordering::Relaxed);
                body_phase.store(PHASE_WAIT_PROOF, Ordering::Relaxed);
                let proof = self.proof_backend.wait(inflight.ticket).await.map_err(|error| {
                    anyhow::anyhow!(
                        "proof for ordered height {expected_height} failed; suffix will not be committed: {error:#}"
                    )
                })?;
                body_phase.store(PHASE_COMMIT, Ordering::Relaxed);
                self.commit_prepared(inflight.prepared, proof).await?;
                did_work = true;
            }
            Ok(did_work)
        };

        tokio::select! {
            biased;
            result = body => result,
            _ = tokio::time::sleep(POLL_ONCE_WATCHDOG) => {
                let phase_name = POLL_PHASE_NAMES[phase.load(Ordering::Relaxed) as usize];
                let height = in_flight_height.load(Ordering::Relaxed);
                Err(anyhow::anyhow!(
                    "block pipeline poll_once watchdog exceeded {POLL_ONCE_WATCHDOG:?} at height {height} phase {phase_name}"
                ))
            }
        }
    }


    async fn prepare_height<NC: DogeNetworkConfig, P: ManagerCustodyProfile>(
        &self,
        checkpoint: &PipelineCheckpoint,
        height: u32,
    ) -> anyhow::Result<PreparedTransition> {
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
        let redis_finalized_buffers = checkpoint.pending_finalization.get(&finalized_height);
        let finalized_block = if redis_finalized_buffers.is_some() {
            Some(self.block_rpc.get_qd_block(finalized_height).await?)
        } else {
            None
        };
        let (finalized_buffers, finalized_witness) = resolve_finalized_buffers::<P>(
            finalized_block.as_ref(),
            redis_finalized_buffers,
            &custody_script_config,
            &self.config.recipient_atas,
            deposit_flat_fee(&self.config.config_params),
            deposit_fee_numerator(&self.config.config_params),
            deposit_fee_denominator(&self.config.config_params),
        )?;

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
        let request = ProofRequest {
            request_id: format!("block-{height}"),
            old_state: hex::encode(&old_state_bytes),
            witness: hex::encode(&witness_bytes),
            finalized_witness,
            custody_script_config: hex::encode(self.config.custody_script_config),
            required_confirmations: self.config.required_confirmations,
            flat_fee: deposit_flat_fee(&self.config.config_params),
            fee_num: deposit_fee_numerator(&self.config.config_params),
            fee_den: deposit_fee_denominator(&self.config.config_params),
            old_header: hex::encode(old_header),
            new_header: hex::encode(new_header),
            config_params: hex::encode(self.config.config_params),
        };
        let proof_job = ProofJob::new(
            self.config.network.as_str().to_owned(),
            height,
            sha256_hex(&serde_json::to_vec(checkpoint)?),
            self.config.network.guest_id().to_owned(),
            file_sha256_hex(&self.config.block_elf_path).await?,
            hex::encode(self.config.expected_vk_hash),
            request,
        )?;
        Ok(PreparedTransition {
            height,
            next_checkpoint,
            proof_job,
            old_state_bytes,
            witness_bytes,
            old_header,
            new_header,
            evaluation,
            finalized_height,
            finalized_buffers,
            idempotency_key: format!(
                "doge-block-{height}-{}",
                hex::encode(witness.block_header.header.get_hash())
            ),
        })
    }

    async fn commit_prepared(
        &mut self,
        prepared: PreparedTransition,
        prover_output: ProverOutput,
    ) -> anyhow::Result<()> {
        let expected_height = self
            .checkpoint
            .height
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("checkpoint height overflow"))?;
        if prepared.height != expected_height || prepared.next_checkpoint.height != expected_height {
            anyhow::bail!(
                "prepared height {} is not the next committed height {expected_height}",
                prepared.height
            );
        }
        let expected_parent = sha256_hex(&serde_json::to_vec(&self.checkpoint)?);
        if prepared.proof_job.parent_checkpoint_sha256 != expected_parent {
            anyhow::bail!(
                "prepared height {} parent checkpoint fingerprint no longer matches committed tip",
                prepared.height
            );
        }
        let PreparedTransition {
            height,
            next_checkpoint,
            old_state_bytes,
            witness_bytes,
            old_header,
            new_header,
            evaluation,
            finalized_height,
            finalized_buffers,
            idempotency_key,
            ..
        } = prepared;
        let proof = &prover_output.proof;
        let public_values = &prover_output.public_values;
        ensure_length("SP1 proof", proof, PROOF_SIZE)?;
        ensure_length("SP1 public values", public_values, PUBLIC_VALUES_SIZE)?;
        if proof.iter().all(|byte| *byte == 0) {
            anyhow::bail!("SP1 proof is an all-zero placeholder");
        }
        let old_header_hash = hash_impl_sha256_bytes(&old_header);
        let new_header_hash = hash_impl_sha256_bytes(&new_header);
        let config_hash = hash_impl_sha256_bytes(&self.config.config_params);
        let custodian_hash = self
            .config
            .network
            .custodian_hash(self.config.custody_script_config);
        let transition_hash = hash_impl_sha256_two_to_one_bytes(
            &old_header_hash,
            &new_header_hash,
        );
        let expected_public_values = hash_impl_sha256_bytes(
            &[
                transition_hash.as_slice(),
                config_hash.as_slice(),
                custodian_hash.as_slice(),
            ]
            .concat(),
        );
        if public_values.as_slice() != expected_public_values {
            anyhow::bail!(
                "SP1 public values do not bind the prepared transition at height {height}"
            );
        }

        debug_assert!(commit_may_start(ProofDisposition::Validated));
        let journal_key = checkpoint_journal_key(&self.checkpoint_key);
        self.redis
            .set::<(), _, _>(
                &journal_key,
                serde_json::to_string(&CheckpointJournal {
                    checkpoint: next_checkpoint.clone(),
                    uploaded_buffers: None,
                    finalized_commitment: Some(finalized_buffers.clone()),
                    submission_signature: None,
                    mint_progress: None,
                })?,
                None,
                None,
                false,
            )
            .await?;

        let uploaded_buffers = self
            .upload_finalized_buffers(finalized_height, &finalized_buffers)
            .await?;
        self.redis
            .set::<(), _, _>(
                &journal_key,
                serde_json::to_string(&CheckpointJournal {
                    checkpoint: next_checkpoint.clone(),
                    uploaded_buffers: Some(uploaded_buffers.clone()),
                    finalized_commitment: Some(finalized_buffers.clone()),
                    submission_signature: None,
                    mint_progress: None,
                })?,
                None,
                None,
                false,
            )
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
            proof,
            public_values,
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
                proof_hex: hex::encode(proof),
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
        self.redis
            .set::<(), _, _>(
                &journal_key,
                serde_json::to_string(&CheckpointJournal {
                    checkpoint: next_checkpoint.clone(),
                    uploaded_buffers: Some(uploaded_buffers.clone()),
                    finalized_commitment: Some(finalized_buffers.clone()),
                    submission_signature: Some(response.signature.clone()),
                    mint_progress: None,
                })?,
                None,
                None,
                false,
            )
            .await?;
        let mint_processing = self
            .process_finalized_mints(&finalized_buffers, &uploaded_buffers)
            .await?;
        self.redis
            .set::<(), _, _>(
                &journal_key,
                serde_json::to_string(&CheckpointJournal {
                    checkpoint: next_checkpoint.clone(),
                    uploaded_buffers: Some(uploaded_buffers.clone()),
                    finalized_commitment: Some(finalized_buffers.clone()),
                    submission_signature: Some(response.signature.clone()),
                    mint_progress: Some(MintProgressRecord::from(&mint_processing)),
                })?,
                None,
                None,
                false,
            )
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
        self.redis.del::<(), _>(&journal_key).await?;

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
        let (mint_buffer, mint_buffer_bump) = retry_bridge_network(
            self.network_retry,
            "Solana pending-mint buffer upload",
            || {
                self.bridge_client
                    .setup_pending_mints_buffer(finalized_height, &pending_mints)
            },
        )
        .await?;
        let (txo_buffer, txo_buffer_bump) = retry_bridge_network(
            self.network_retry,
            "Solana TXO buffer upload",
            || {
                self.bridge_client
                    .setup_txo_buffer(finalized_height, &finalized.txo_indices)
            },
        )
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
            self.network_retry
                .run("Solana recipient ATA lookup", || async {
                    match rpc.get_account(&recipient).await {
                        Ok(_) => RetryAction::Success(()),
                        Err(error) if is_retryable_solana_client_error(&error) => {
                            RetryAction::Retry(anyhow::Error::new(error))
                        }
                        Err(_) => RetryAction::Fatal(anyhow::anyhow!(
                            "configured recipient ATA {recipient} does not exist; create it before starting the block pipeline"
                        )),
                    }
                })
                .await?;
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
        let result = retry_bridge_network(
            self.network_retry,
            "Solana pending-mint processing",
            || {
                self.bridge_client.process_remaining_pending_mints_groups(
                    &pending_mints,
                    buffers.mint_buffer,
                    buffers.mint_buffer_bump,
                )
            },
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

async fn retry_bridge_network<T, F, Fut>(
    policy: NetworkRetryPolicy,
    operation_name: &'static str,
    operation: F,
) -> Result<T, BridgeError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, BridgeError>>,
{
    policy
        .run(operation_name, || async {
            match operation().await {
                Ok(value) => RetryAction::Success(value),
                Err(error) if is_retryable_bridge_error(&error) => RetryAction::Retry(error),
                Err(error) => RetryAction::Fatal(error),
            }
        })
        .await
}

fn is_retryable_bridge_error(error: &BridgeError) -> bool {
    match error {
        BridgeError::Rpc(error) => is_retryable_solana_client_error(error),
        BridgeError::RateLimited { .. } | BridgeError::ConnectionTimeout => true,
        // ConfirmationTimeout means the transaction was submitted but its
        // confirmation could not be observed. The BridgeClient interface
        // does not expose the in-flight signature, so the pipeline cannot
        // reconcile against on-chain state by signature. Resending blindly
        // risks a double submission (e.g. a duplicate mint), so fail closed
        // and surface the error to the supervisor instead of retrying.
        BridgeError::ConfirmationTimeout { .. } => false,
        _ => false,
    }
}

fn is_retryable_solana_client_error(error: &solana_client::client_error::ClientError) -> bool {
    match error.kind() {
        ClientErrorKind::Io(error) => crate::network_retry::is_retryable_io_kind(error.kind()),
        ClientErrorKind::Reqwest(error) => {
            error.is_timeout()
                || error.is_connect()
                || error.is_request()
                || error.is_body()
                || has_retryable_io_source(error)
        }
        ClientErrorKind::Middleware(error) => has_retryable_io_source(error.as_ref()),
        ClientErrorKind::RpcError(error) => {
            matches!(
                error,
                solana_client::rpc_request::RpcError::RpcRequestError(_)
                    | solana_client::rpc_request::RpcError::RpcResponseError {
                        code: 429 | -32004 | -32005 | -32007 | -32009,
                        ..
                    }
            )
        }
        _ => false,
    }
}

#[cfg(test)]
mod network_retry_tests {
    use super::*;
    use std::io;

    #[test]
    fn solana_connection_reset_is_retryable() {
        let error = solana_client::client_error::ClientError::from(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "reset",
        ));

        assert!(is_retryable_solana_client_error(&error));
    }

    #[test]
    fn solana_invalid_data_is_not_retryable() {
        let error = solana_client::client_error::ClientError::from(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid response",
        ));

        assert!(!is_retryable_solana_client_error(&error));
    }

    #[test]
    fn solana_confirmation_timeout_is_not_blindly_resent() {
        // The BridgeClient interface does not expose the in-flight signature
        // for a ConfirmationTimeout, so the pipeline cannot reconcile by
        // signature. Treat it as fatal (no blind resend) to avoid duplicate
        // submissions such as a duplicate mint.
        assert!(!is_retryable_bridge_error(&BridgeError::ConfirmationTimeout {
            timeout_ms: 60_000,
        }));
        assert!(is_retryable_bridge_error(&BridgeError::ConnectionTimeout));
        assert!(is_retryable_bridge_error(&BridgeError::RateLimited { retry_after_ms: 100 }));
    }
}
pub async fn recover_checkpoint_from_proof_archive(
    config: &BlockPipelineConfig,
    proof_archive_dir: &Path,
) -> anyhow::Result<u32> {
    validate_config(config)?;
    let redis = build_redis_pool(config)?;
    tokio::time::timeout(REDIS_INIT_DEADLINE, redis.init())
        .await
        .map_err(|_| anyhow::anyhow!("redis init exceeded {REDIS_INIT_DEADLINE:?}"))??;
    let checkpoint_key = format!(
        "{CHECKPOINT_PREFIX}-{}-{}",
        config.network.as_str(),
        config.redis_seed
    );
    let checkpoint_json = tokio::time::timeout(
        REDIS_CHECKPOINT_DEADLINE,
        redis.get::<Option<String>, _>(&checkpoint_key),
    )
    .await
    .map_err(|_| anyhow::anyhow!("redis checkpoint load exceeded {REDIS_CHECKPOINT_DEADLINE:?}"))??
    .ok_or_else(|| anyhow::anyhow!("current Redis checkpoint is missing"))?;
    let checkpoint: PipelineCheckpoint = serde_json::from_str(&checkpoint_json)?;
    validate_checkpoint(&checkpoint)?;
    let height = checkpoint
        .height
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("checkpoint height overflow"))?;

    let old_state_bytes = tokio::fs::read(proof_archive_dir.join("old_state.bin")).await?;
    let old_header = tokio::fs::read(proof_archive_dir.join("old_header.bin")).await?;
    if old_state_bytes != hex::decode(&checkpoint.state_hex)? {
        anyhow::bail!("proof archive old state does not match the current Redis checkpoint");
    }
    if old_header != hex::decode(&checkpoint.header_hex)? {
        anyhow::bail!("proof archive old header does not match the current Redis checkpoint");
    }
    let witness_bytes = tokio::fs::read(proof_archive_dir.join("witness.bin")).await?;
    let witness = PsyDogeBridgeIncomingBlockWitness::read_from_buffer(&witness_bytes)?;
    let custody_script_config = CustodyScriptConfig::new(config.custody_script_config);
    let evaluation = match config.network {
        DogeNetworkProfile::Regtest => evaluate_claim_witness::<LocalRegtestManagerCustody>(
            height,
            &witness.claim_witness,
            witness.block_header.header.merkle_root,
            &custody_script_config,
            deposit_flat_fee(&config.config_params),
            deposit_fee_numerator(&config.config_params),
            deposit_fee_denominator(&config.config_params),
        )?,
        DogeNetworkProfile::Testnet => evaluate_claim_witness::<OfficialTestnetManagerCustody>(
            height,
            &witness.claim_witness,
            witness.block_header.header.merkle_root,
            &custody_script_config,
            deposit_flat_fee(&config.config_params),
            deposit_fee_numerator(&config.config_params),
            deposit_fee_denominator(&config.config_params),
        )?,
    };
    let mut new_state = BridgeState::try_from_slice(&old_state_bytes)?;
    match config.network {
        DogeNetworkProfile::Regtest => new_state.append_block::<DogeRegTestConfig>(
            height,
            &witness.block_header,
            evaluation.transition.new_claimed_txo_tree_root,
            evaluation.transition.new_auto_claimed_deposits_tree_root,
            evaluation.transition.end_auto_claimed_deposits_index,
            evaluation.transition.fees_collected,
            None,
        )?,
        DogeNetworkProfile::Testnet => new_state.append_block::<DogeTestNetConfig>(
            height,
            &witness.block_header,
            evaluation.transition.new_claimed_txo_tree_root,
            evaluation.transition.new_auto_claimed_deposits_tree_root,
            evaluation.transition.end_auto_claimed_deposits_index,
            evaluation.transition.fees_collected,
            None,
        )?,
    }
    let finalized_height = height
        .checked_sub(config.required_confirmations)
        .ok_or_else(|| anyhow::anyhow!("finalized height underflow"))?;
    let block_rpc = DogeLinkElectrsAsyncClient::new(config.electrs_url.clone());
    let redis_finalized_buffers = checkpoint.pending_finalization.get(&finalized_height);
    let finalized_block = if redis_finalized_buffers.is_some() {
        Some(block_rpc.get_qd_block(finalized_height).await?)
    } else {
        None
    };
    let finalized_buffers = match config.network {
        DogeNetworkProfile::Regtest => resolve_finalized_buffers::<LocalRegtestManagerCustody>(
            finalized_block.as_ref(),
            redis_finalized_buffers,
            &custody_script_config,
            &config.recipient_atas,
            deposit_flat_fee(&config.config_params),
            deposit_fee_numerator(&config.config_params),
            deposit_fee_denominator(&config.config_params),
        )?.0,
        DogeNetworkProfile::Testnet => resolve_finalized_buffers::<OfficialTestnetManagerCustody>(
            finalized_block.as_ref(),
            redis_finalized_buffers,
            &custody_script_config,
            &config.recipient_atas,
            deposit_flat_fee(&config.config_params),
            deposit_fee_numerator(&config.config_params),
            deposit_fee_denominator(&config.config_params),
        )?.0,
    };
    let old_header = decode_fixed::<HEADER_SIZE>(&checkpoint.header_hex, "checkpoint header")?;
    let new_header = build_new_solana_header(
        &old_header,
        &new_state,
        config.required_confirmations,
        finalized_buffers.pending_mints_hash()?,
        finalized_buffers.txo_output_list_hash()?,
    )?;
    let archived_header = tokio::fs::read(proof_archive_dir.join("new_header.bin")).await?;
    if archived_header != new_header {
        anyhow::bail!("reconstructed header does not match the proven archived header");
    }

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
    let recovered = PipelineCheckpoint {
        height,
        state_hex: hex::encode(borsh::to_vec(&new_state)?),
        header_hex: hex::encode(new_header),
        claim_frontier,
        txo_block_frontier,
        claim_history,
        txo_block_history,
        pending_finalization,
    };
    validate_checkpoint(&recovered)?;

    let operator = read_pipeline_keypair(&config.operator_keypair, "operator")?;
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
    assert_checkpoint_matches_chain(&recovered, &chain_state.bridge_header)?;
    redis
        .set::<(), _, _>(
            &checkpoint_key,
            serde_json::to_string(&recovered)?,
            None,
            None,
            false,
        )
        .await?;
    Ok(height)
}


async fn initialize_checkpoint(
    config: &BlockPipelineConfig,
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

fn build_hash_only_incoming_witness<P: ManagerCustodyProfile>(
    block: &QDogeBlock,
    custody_script_config: &CustodyScriptConfig,
    recipient_atas: &[[u8; 32]],
) -> anyhow::Result<PsyDogeBridgeIncomingBlockWitness> {
    let (total_outputs, deposit_transactions) =
        scan_manager_custody_deposits::<P>(block, custody_script_config, recipient_atas)?;
    let claim_siblings = core::array::from_fn(|index| SHA256_ZERO_HASHES[index]);
    let txo_siblings = core::array::from_fn(|index| {
        SHA256_ZERO_HASHES[TXO_BLOCK_FULL_MERKLE_TREE_HEIGHT + index]
    });

    Ok(PsyDogeBridgeIncomingBlockWitness {
        block_header: block.to_qdoge_block_header(),
        claim_witness: PsyBridgeClaimBlockWitness::new(
            PsyBridgeClaimBlockWitnessHeader {
                txo_tree_block_siblings: txo_siblings,
                last_auto_claimed_deposits_siblings: claim_siblings,
                total_outputs_hint: total_outputs,
                claim_deposits_last_index: 0,
                claim_deposits_last_value: [0u8; 32],
            },
            recipient_atas.to_vec(),
            deposit_transactions,
        ),
        previous_header_last_rollback_at_secs: 0,
        previous_header_paused_until_secs: 0,
    })
}

fn resolve_finalized_buffers<P: ManagerCustodyProfile>(
    block: Option<&QDogeBlock>,
    redis_buffers: Option<&BlockBufferCommitment>,
    custody_script_config: &CustodyScriptConfig,
    recipient_atas: &[[u8; 32]],
    flat_fee_per_deposit_sats: u64,
    deposit_fee_rate_numerator: u64,
    deposit_fee_rate_denominator: u64,
) -> anyhow::Result<(BlockBufferCommitment, String)> {
    let Some(redis_buffers) = redis_buffers else {
        return Ok((BlockBufferCommitment::empty()?, String::new()));
    };
    let block = block.ok_or_else(|| anyhow::anyhow!("finalized Electrs block is missing"))?;
    let witness =
        build_hash_only_incoming_witness::<P>(block, custody_script_config, recipient_atas)?;
    let (pending_mints_hash, txo_output_list_hash) =
        pending_mint_and_txo_hashes_from_claim_witness::<P>(
            &witness.claim_witness,
            block.header.merkle_root,
            custody_script_config,
            flat_fee_per_deposit_sats,
            deposit_fee_rate_numerator,
            deposit_fee_rate_denominator,
        )?;
    if redis_buffers.pending_mints_hash()? != pending_mints_hash {
        anyhow::bail!("Redis pending-mint hash disagrees with finalized Electrs witness");
    }
    if redis_buffers.txo_output_list_hash()? != txo_output_list_hash {
        anyhow::bail!("Redis TXO-list hash disagrees with finalized Electrs witness");
    }

    let mut finalized_buffers = redis_buffers.clone();
    finalized_buffers.pending_mints_hash_hex = hex::encode(pending_mints_hash);
    finalized_buffers.txo_output_list_hash_hex = hex::encode(txo_output_list_hash);
    Ok((finalized_buffers, hex::encode(witness.write_to_vec()?)))
}

fn scan_manager_custody_deposits<P: ManagerCustodyProfile>(
    block: &QDogeBlock,
    custody_script_config: &CustodyScriptConfig,
    recipient_atas: &[[u8; 32]],
) -> anyhow::Result<(u32, Vec<PsyBridgeClaimBlockTransactionWitness>)> {
    if block.transactions.is_empty() {
        anyhow::bail!("Electrs block contains no transactions");
    }

    let transaction_hashes: Vec<QHash256> = block
        .transactions
        .iter()
        .map(BTCTransaction::get_hash)
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

    Ok((total_outputs, deposit_transactions))
}

fn build_deposit_claim_witness<P: ManagerCustodyProfile>(
    block: &QDogeBlock,
    checkpoint: &PipelineCheckpoint,
    custody_script_config: &CustodyScriptConfig,
    recipient_atas: &[[u8; 32]],
) -> anyhow::Result<PsyDogeBridgeIncomingBlockWitness> {
    let (total_outputs, deposit_transactions) =
        scan_manager_custody_deposits::<P>(block, custody_script_config, recipient_atas)?;

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
    let (pending_mints_hash, txo_output_list_hash) =
        pending_mint_and_txo_hashes_from_claim_witness::<P>(
            witness,
            block_transaction_tree_merkle_root,
            custody_script_config,
            flat_fee_per_deposit_sats,
            deposit_fee_rate_numerator,
            deposit_fee_rate_denominator,
        )?;

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




pub(crate) async fn prepare_prover_request(
    config: &BlockPipelineConfig,
    request: &crate::proof_queue::ProofRequest,
) -> anyhow::Result<()> {
    if std::env::var_os("DOGE_SAVE_PROVER_ARGS").is_some() {
        let args = serde_json::json!({
            "network": config.network.as_str(),
            "old_state": request.old_state,
            "witness": request.witness,
            "finalized_witness": request.finalized_witness,
            "custody_script_config": request.custody_script_config,
            "required_confirmations": request.required_confirmations,
            "flat_fee": request.flat_fee,
            "fee_num": request.fee_num,
            "fee_den": request.fee_den,
            "old_header": request.old_header,
            "new_header": request.new_header,
            "config_params": request.config_params,
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
    config: &BlockPipelineConfig,
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
    if identity.guest_id != config.network.guest_id() {
        anyhow::bail!(
            "SP1 prover guest id mismatch: expected {}, got {}",
            config.network.guest_id(),
            identity.guest_id
        );
    }
    validate_prover_elf_sha256(config, &identity.block_elf_sha256).await?;
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

/// Validate the daemon's embedded ELF by its SHA-256 digest against the
/// configured block-transition ELF. The daemon identity is path-independent,
/// so the daemon's local ELF path is intentionally not compared.
async fn validate_prover_elf_sha256(
    config: &BlockPipelineConfig,
    prover_elf_sha256: &str,
) -> anyhow::Result<()> {
    let configured_elf = tokio::fs::read(&config.block_elf_path)
        .await
        .map_err(|error| {
            anyhow::anyhow!(
                "failed to read configured block-transition ELF {}: {error}",
                config.block_elf_path.display()
            )
        })?;
    let configured_elf_sha256 = sha256_hex(&configured_elf);
    if prover_elf_sha256 != configured_elf_sha256 {
        anyhow::bail!(
            "SP1 ELF hash mismatch: gen-proof embeds {prover_elf_sha256}, configured ELF is {configured_elf_sha256}"
        );
    }
    Ok(())
}

fn validate_proof_response(
    config: &BlockPipelineConfig,
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
    if response.guest_id.as_deref() != Some(config.network.guest_id()) {
        anyhow::bail!(
            "gen-proof response guest id {:?} did not match {}",
            response.guest_id,
            config.network.guest_id()
        );
    }
    let configured_elf = std::fs::read(&config.block_elf_path)?;
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
    config: &BlockPipelineConfig,
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

fn validate_gen_proof_path(
    prover_backend: ProverBackendKind,
    gen_proof_path: &Path,
) -> anyhow::Result<()> {
    if prover_backend == ProverBackendKind::Redis {
        return Ok(());
    }
    if !gen_proof_path.is_file() {
        anyhow::bail!(
            "SP1 gen-proof executable does not exist at {}",
            gen_proof_path.display()
        );
    }
    ensure_release_path(gen_proof_path, "SP1 gen-proof")
}

fn validate_config(config: &BlockPipelineConfig) -> anyhow::Result<()> {
    if cfg!(debug_assertions) {
        anyhow::bail!("block pipeline must be built in release mode");
    }
    validate_gen_proof_path(config.prover_backend, &config.gen_proof_path)?;
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
    if config.proof_prepare_window == 0 {
        anyhow::bail!("proof prepare window must be at least 1");
    }
    if config.proof_queue_prefix.trim().is_empty() {
        anyhow::bail!("proof queue prefix must not be empty");
    }
    if config.proof_wait_interval.is_zero() {
        anyhow::bail!("proof wait interval must be non-zero");
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

async fn reconcile_checkpoint_journal(
    redis: &fred::prelude::Pool,
    checkpoint_key: &str,
    checkpoint: PipelineCheckpoint,
    chain_header: &PsyBridgeHeader,
    bridge_client: &BridgeClient,
    network_retry: NetworkRetryPolicy,
) -> anyhow::Result<PipelineCheckpoint> {
    let journal_key = checkpoint_journal_key(checkpoint_key);

    // First compare the committed checkpoint against the on-chain header. When
    // they match, the last block_update was NOT submitted, so any lingering
    // journal records only partial pre-submission work. Delete the raw journal
    // key without deserializing it: a corrupt or bad-JSON stale journal must
    // not block this safe discard, and poll_once reprocesses the height
    // deterministically.
    if assert_checkpoint_matches_chain(&checkpoint, chain_header).is_ok() {
        let journal_present = redis
            .get::<Option<String>, _>(&journal_key)
            .await?
            .is_some();
        if matches!(
            stale_journal_action(true, journal_present),
            StaleJournalAction::DiscardRaw
        ) {
            redis.del::<(), _>(&journal_key).await?;
            eprintln!(
                "discarded stale checkpoint journal for height {} (on-chain header not yet advanced); pipeline will reprocess",
                checkpoint.height + 1
            );
        }
        return Ok(checkpoint);
    }

    // The on-chain header advanced past the committed checkpoint, so the
    // block_update was submitted. Recovery requires a usable journal; a
    // missing or corrupt journal fails closed.
    let journal_json = redis
        .get::<Option<String>, _>(&journal_key)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "on-chain bridge header advanced past Redis checkpoint height {} without a pending journal; cannot reconcile, fail closed",
                checkpoint.height
            )
        })?;
    let journal: CheckpointJournal = serde_json::from_str(&journal_json)?;
    validate_checkpoint(&journal.checkpoint)?;

    match plan_journal_recovery(&checkpoint, chain_header, Some(&journal)) {
        JournalRecoveryPlan::UpToDate | JournalRecoveryPlan::DiscardStaleJournal => {
            // Handled above by the raw-key discard path; unreachable here.
            Ok(checkpoint)
        }
        JournalRecoveryPlan::PromoteCheckpoint { checkpoint } => {
            validate_checkpoint(&checkpoint)?;
            redis
                .set::<(), _, _>(
                    checkpoint_key,
                    serde_json::to_string(&checkpoint)?,
                    None,
                    None,
                    false,
                )
                .await?;
            redis.del::<(), _>(&journal_key).await?;
            eprintln!(
                "recovered committed checkpoint {} from pending journal (mint already completed)",
                checkpoint.height
            );
            Ok(checkpoint)
        }
        JournalRecoveryPlan::CompleteMint {
            checkpoint,
            finalized_commitment,
            uploaded_buffers,
        } => {
            validate_checkpoint(&checkpoint)?;
            // Idempotently complete mint processing. The on-chain pending-mint
            // tracker skips groups that were already claimed, so re-running is
            // safe; a ConfirmationTimeout fails closed (no blind resend).
            if !finalized_commitment.pending_mints.is_empty() {
                let pending_mints: Vec<PendingMint> = finalized_commitment
                    .pending_mints
                    .iter()
                    .map(PendingMintPayload::to_bridge_mint)
                    .collect();
                let result = retry_bridge_network(
                    network_retry,
                    "Solana pending-mint recovery",
                    || {
                        bridge_client.process_remaining_pending_mints_groups(
                            &pending_mints,
                            uploaded_buffers.mint_buffer,
                            uploaded_buffers.mint_buffer_bump,
                        )
                    },
                )
                .await?;
                // Verify completion by the BridgeClient's fully_completed
                // contract only. Do NOT compare this run's newly-processed
                // count against the original pending-mint total: groups that
                // were already claimed in a prior partial run are skipped, so
                // total_mints_processed can be 0 even when minting is fully
                // complete. A cumulative-semantics fix for the count lives in
                // the BridgeClient (separate task); here we rely on
                // fully_completed plus the on-chain tracker's idempotency.
                mint_recovery_succeeded(&result)?;
            }
            redis
                .set::<(), _, _>(
                    checkpoint_key,
                    serde_json::to_string(&checkpoint)?,
                    None,
                    None,
                    false,
                )
                .await?;
            redis.del::<(), _>(&journal_key).await?;
            eprintln!(
                "recovered committed checkpoint {} from pending journal after completing mint recovery",
                checkpoint.height
            );
            Ok(checkpoint)
        }
        JournalRecoveryPlan::FailClosed(reason) => {
            anyhow::bail!("{reason}")
        }
    }
}

/// What to do about a lingering journal key when the committed checkpoint
/// matches the on-chain header (block_update was NOT submitted). This decision
/// depends only on raw key presence, never on the journal's JSON contents, so
/// a corrupt or bad-JSON stale journal is safely discarded.
enum StaleJournalAction {
    UpToDate,
    DiscardRaw,
}

fn stale_journal_action(committed_matches_chain: bool, journal_present: bool) -> StaleJournalAction {
    if committed_matches_chain {
        if journal_present {
            StaleJournalAction::DiscardRaw
        } else {
            StaleJournalAction::UpToDate
        }
    } else {
        // The advanced case is handled by plan_journal_recovery; this helper is
        // only consulted on the committed-matches path.
        StaleJournalAction::UpToDate
    }
}

/// Verify that an idempotent mint recovery run fully completed. Relies on the
/// BridgeClient's `fully_completed` contract rather than comparing this run's
/// processed count to the original pending-mint total (already-claimed groups
/// are skipped, so the count can be 0 on a successful no-op recovery).
fn mint_recovery_succeeded(result: &ProcessMintsResult) -> anyhow::Result<()> {
    if !result.fully_completed {
        anyhow::bail!(
            "mint recovery did not fully complete pending mints (BridgeClient reported fully_completed=false)"
        );
    }
    Ok(())
}

/// Pure decision logic for journal recovery, separated from the Redis and
/// Solana execution so the recovery contract can be tested without live
/// services.
#[derive(Debug)]
enum JournalRecoveryPlan {
    /// The committed checkpoint matches the on-chain header; nothing to do.
    UpToDate,
    /// The committed checkpoint matches the on-chain header but a stale
    /// journal lingers from a crash before `block_update` was submitted.
    /// Discard it and reprocess the height deterministically.
    DiscardStaleJournal,
    /// The on-chain header advanced and minting was already completed; just
    /// promote the journal checkpoint.
    PromoteCheckpoint { checkpoint: PipelineCheckpoint },
    /// The on-chain header advanced and minting is incomplete; idempotently
    /// complete mint processing, then promote the journal checkpoint.
    CompleteMint {
        checkpoint: PipelineCheckpoint,
        finalized_commitment: BlockBufferCommitment,
        uploaded_buffers: UploadedBuffers,
    },
    /// Recovery is impossible without losing safety; the supervisor must
    /// fail closed.
    FailClosed(String),
}

fn plan_journal_recovery(
    checkpoint: &PipelineCheckpoint,
    chain_header: &PsyBridgeHeader,
    journal: Option<&CheckpointJournal>,
) -> JournalRecoveryPlan {
    if assert_checkpoint_matches_chain(checkpoint, chain_header).is_ok() {
        // The last block_update was NOT submitted (chain still at the
        // committed checkpoint). A lingering journal records partial
        // pre-submission work and must be discarded so poll_once reprocesses
        // the height deterministically.
        return if journal.is_some() {
            JournalRecoveryPlan::DiscardStaleJournal
        } else {
            JournalRecoveryPlan::UpToDate
        };
    }

    // The on-chain header advanced past the committed checkpoint, so the
    // block_update was submitted. Recovery requires the journal.
    let Some(journal) = journal else {
        return JournalRecoveryPlan::FailClosed(format!(
            "on-chain bridge header advanced past Redis checkpoint height {} without a pending journal; cannot reconcile, fail closed",
            checkpoint.height
        ));
    };
    if journal.checkpoint.height != checkpoint.height.saturating_add(1) {
        return JournalRecoveryPlan::FailClosed(format!(
            "checkpoint journal height {} is not exactly one above committed checkpoint height {}",
            journal.checkpoint.height,
            checkpoint.height
        ));
    }
    if assert_checkpoint_matches_chain(&journal.checkpoint, chain_header).is_err() {
        return JournalRecoveryPlan::FailClosed(format!(
            "journal checkpoint height {} does not match the advanced on-chain bridge header",
            journal.checkpoint.height
        ));
    }

    if journal.mint_progress.is_some() {
        // Minting already completed; promote the journal checkpoint.
        return JournalRecoveryPlan::PromoteCheckpoint {
            checkpoint: journal.checkpoint.clone(),
        };
    }

    // block_update was submitted but minting is incomplete (the
    // block_update-after / mint-before crash window). Idempotently complete
    // the mints, which requires the finalized commitment and uploaded
    // buffers; without them, fail closed.
    let Some(finalized_commitment) = journal.finalized_commitment.clone() else {
        return JournalRecoveryPlan::FailClosed(format!(
            "on-chain header advanced to height {} but the journal lacks the finalized commitment; cannot idempotently complete mint, fail closed",
            journal.checkpoint.height
        ));
    };
    let Some(uploaded_buffers) = journal.uploaded_buffers.clone() else {
        return JournalRecoveryPlan::FailClosed(format!(
            "on-chain header advanced to height {} but the journal lacks the uploaded buffer addresses; cannot complete mint, fail closed",
            journal.checkpoint.height
        ));
    };
    JournalRecoveryPlan::CompleteMint {
        checkpoint: journal.checkpoint.clone(),
        finalized_commitment,
        uploaded_buffers,
    }
}

/// Build a Redis connection pool with explicit command/unresponsive timeouts
/// and exponential reconnection. The Redis URL and any Redis values are never
/// logged by this helper; only operational errors surface.
fn build_redis_pool(config: &BlockPipelineConfig) -> anyhow::Result<fred::prelude::Pool> {
    let redis_config = Config::from_url(&config.redis_url)?;
    let pool = Builder::from_config(redis_config)
        .with_connection_config(|connection| {
            connection.connection_timeout = REDIS_CONNECTION_TIMEOUT;
            connection.internal_command_timeout = REDIS_CONNECTION_TIMEOUT;
            connection.unresponsive.max_timeout = Some(REDIS_UNRESPONSIVE_TIMEOUT);
            connection.unresponsive.interval = Duration::from_secs(2);
        })
        .with_performance_config(|performance| {
            performance.default_command_timeout = REDIS_COMMAND_TIMEOUT;
        })
        .set_policy(ReconnectPolicy::new_exponential(0, 100, 30_000, 2))
        .build_pool(2)?;
    Ok(pool)
}

fn checkpoint_journal_key(checkpoint_key: &str) -> String {
    format!("{checkpoint_key}-{CHECKPOINT_JOURNAL_SUFFIX}")
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

    #[test]
    fn redis_backend_does_not_require_local_gen_proof_path() {
        let missing_path = std::env::temp_dir().join(format!(
            "missing-redis-gen-proof-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("unnamed")
        ));
        assert!(!missing_path.exists());

        validate_gen_proof_path(ProverBackendKind::Redis, &missing_path)
            .expect("Redis proving must not require a local gen-proof executable");
    }

    #[test]
    fn local_backend_rejects_missing_gen_proof_path() {
        let missing_path = std::env::temp_dir().join(format!(
            "missing-local-gen-proof-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("unnamed")
        ));
        assert!(!missing_path.exists());

        let error = validate_gen_proof_path(ProverBackendKind::Local, &missing_path)
            .expect_err("local proving must require a gen-proof executable");
        assert!(
            error
                .to_string()
                .contains("SP1 gen-proof executable does not exist"),
            "{error:#}"
        );
    }

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

    #[cfg(unix)]
    #[tokio::test]
    async fn daemon_alive_but_silent_times_out() {
        // A daemon that stays alive but never emits a protocol line must not
        // block the pipeline. The deadline-bounded read returns a timeout
        // error promptly instead of hanging until the child exits.
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 30")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn silent child");
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr_task = tokio::spawn(async { Ok(()) });
        let stderr = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let mut daemon = ProverDaemon {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            stderr_task: Some(stderr_task),
            stderr,
            vk_hash: [0; 32],
        };

        let deadline = Duration::from_millis(500);
        let result = tokio::time::timeout(Duration::from_secs(3), async {
            daemon.read_response_line_within(deadline).await
        })
        .await
        .expect("the deadline-bounded read must not hang past its own budget");
        let error = result.expect_err("a silent daemon must time out");
        assert!(
            error.to_string().contains("did not respond"),
            "{}",
            error
        );
        daemon.terminate().await;
    }

    #[test]
    fn global_journal_starts_only_after_proof_validation() {
        assert!(!commit_may_start(ProofDisposition::Preparing));
        assert!(!commit_may_start(ProofDisposition::Waiting));
        assert!(!commit_may_start(ProofDisposition::Failed));
        assert!(commit_may_start(ProofDisposition::Validated));
    }
    #[test]
    fn out_of_order_ready_proofs_commit_strictly_in_height_order() {
        // Completion order is H+2, H, H+1. Only the committed tip's successor
        // is ever eligible to commit.
        let mut states = BTreeMap::from([
            (43, SimulatedProofState::Pending),
            (44, SimulatedProofState::Pending),
            (45, SimulatedProofState::Ready),
        ]);
        let mut committed = 42;
        assert_eq!(ordered_proof_action(committed, &states), OrderedProofAction::Wait(43));

        states.insert(43, SimulatedProofState::Ready);
        assert_eq!(ordered_proof_action(committed, &states), OrderedProofAction::Commit(43));
        states.remove(&43);
        committed = 43;
        assert_eq!(ordered_proof_action(committed, &states), OrderedProofAction::Wait(44));

        states.insert(44, SimulatedProofState::Ready);
        let mut order = Vec::new();
        while let OrderedProofAction::Commit(height) = ordered_proof_action(committed, &states) {
            order.push(height);
            states.remove(&height);
            committed = height;
        }
        assert_eq!(order, vec![44, 45]);
        assert_eq!(committed, 45);
    }

    #[test]
    fn earlier_pending_or_failed_proof_blocks_ready_suffix() {
        let mut states = BTreeMap::from([
            (43, SimulatedProofState::Pending),
            (44, SimulatedProofState::Ready),
            (45, SimulatedProofState::Ready),
        ]);
        assert_eq!(ordered_proof_action(42, &states), OrderedProofAction::Wait(43));
        states.insert(43, SimulatedProofState::Failed);
        assert_eq!(ordered_proof_action(42, &states), OrderedProofAction::Fail(43));
        assert_ne!(ordered_proof_action(42, &states), OrderedProofAction::Commit(44));
    }

    #[test]
    fn window_one_retains_single_height_behavior() {
        let states = BTreeMap::from([(43, SimulatedProofState::Ready)]);
        assert_eq!(ordered_proof_action(42, &states), OrderedProofAction::Commit(43));
        assert_eq!(ordered_proof_action(43, &states), OrderedProofAction::Idle);
    }

    #[test]
    fn ordered_proof_action_never_commits_past_a_missing_height() {
        // A gap in the in-flight map blocks the suffix: even though H+2 is
        // Ready, the missing H+1 yields Idle rather than a Commit that would
        // skip an unproven height. This is the strict H+1 ordering invariant.
        let states = BTreeMap::from([(44, SimulatedProofState::Ready)]);
        assert_eq!(ordered_proof_action(42, &states), OrderedProofAction::Idle);
        assert_ne!(ordered_proof_action(42, &states), OrderedProofAction::Commit(44));
        // An entry sitting at the committed tip is never the next height, so a
        // stale/duplicate Ready at H does not re-commit H.
        let states = BTreeMap::from([(42, SimulatedProofState::Ready)]);
        assert_eq!(ordered_proof_action(42, &states), OrderedProofAction::Idle);
    }

    #[test]
    fn ordered_proof_action_failed_height_blocks_every_ready_suffix_never_skips() {
        // H+1 (43) permanently failed. In production the committed tip never
        // advances past a failed height (the pipeline halts), so the only
        // reachable state is committed==42 with 43==Failed. No matter how many
        // suffix heights are Ready, the action stays Fail(43). A plausible bug
        // that scanned for the first Ready height would Commit(44) and bypass
        // the failure; this test reddens that bug.
        let states = BTreeMap::from([
            (43, SimulatedProofState::Failed),
            (44, SimulatedProofState::Ready),
            (45, SimulatedProofState::Ready),
            (46, SimulatedProofState::Ready),
        ]);
        let action = ordered_proof_action(42, &states);
        assert_eq!(action, OrderedProofAction::Fail(43));
        assert_ne!(action, OrderedProofAction::Commit(44));
        assert_ne!(action, OrderedProofAction::Commit(45));
        assert_ne!(action, OrderedProofAction::Wait(43));
    }

    #[test]
    fn ordered_proof_action_pending_height_waits_even_if_suffix_is_ready() {
        // H+1 still proving while H+2 is Ready: the suffix must not commit
        // ahead of the in-flight predecessor.
        let states = BTreeMap::from([
            (43, SimulatedProofState::Pending),
            (44, SimulatedProofState::Ready),
        ]);
        assert_eq!(ordered_proof_action(42, &states), OrderedProofAction::Wait(43));
        assert_ne!(ordered_proof_action(42, &states), OrderedProofAction::Commit(44));
    }

    #[test]
    fn ordered_proof_action_at_u32_max_returns_idle_without_overflow() {
        // The committed tip can reach u32::MAX; the next-height computation
        // must use checked_add and yield Idle rather than panicking on wrap.
        assert_eq!(
            ordered_proof_action(u32::MAX, &BTreeMap::new()),
            OrderedProofAction::Idle
        );
        // Even a Ready entry beyond the tip must not trigger an overflow.
        let states = BTreeMap::from([(u32::MAX, SimulatedProofState::Ready)]);
        assert_eq!(
            ordered_proof_action(u32::MAX, &states),
            OrderedProofAction::Idle
        );
    }


    #[test]
    fn checkpoint_journal_key_is_namespaced() {
        assert_eq!(
            checkpoint_journal_key("PDOGE-E2E-BLOCK-CHECKPOINT-V3-testnet-1337"),
            "PDOGE-E2E-BLOCK-CHECKPOINT-V3-testnet-1337-pending"
        );
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

    fn test_journal(
        checkpoint: PipelineCheckpoint,
        finalized: bool,
        uploaded: bool,
        signature: bool,
        mint_done: bool,
    ) -> CheckpointJournal {
        CheckpointJournal {
            checkpoint,
            uploaded_buffers: if uploaded {
                Some(UploadedBuffers {
                    mint_buffer: Pubkey::default(),
                    mint_buffer_bump: 0,
                    txo_buffer: Pubkey::default(),
                    txo_buffer_bump: 0,
                })
            } else {
                None
            },
            finalized_commitment: if finalized {
                Some(BlockBufferCommitment::empty().unwrap())
            } else {
                None
            },
            submission_signature: if signature {
                Some("sig".to_string())
            } else {
                None
            },
            mint_progress: if mint_done {
                Some(MintProgressRecord {
                    signatures: Vec::new(),
                    groups_processed: 0,
                    total_mints_processed: 0,
                })
            } else {
                None
            },
        }
    }

    #[test]
    fn journal_recovery_plan_up_to_date_when_committed_matches_chain() {
        let committed = empty_checkpoint(42);
        let chain_header = PsyBridgeHeader::default();
        assert!(matches!(
            plan_journal_recovery(&committed, &chain_header, None),
            JournalRecoveryPlan::UpToDate
        ));
    }

    #[test]
    fn journal_recovery_plan_discards_stale_journal_when_chain_not_advanced() {
        // The committed checkpoint matches the on-chain header, so
        // block_update was NOT submitted. A lingering journal records
        // partial pre-submission work and must be discarded.
        let committed = empty_checkpoint(42);
        let chain_header = PsyBridgeHeader::default();
        let journal = test_journal(empty_checkpoint(43), true, true, false, false);
        assert!(matches!(
            plan_journal_recovery(&committed, &chain_header, Some(&journal)),
            JournalRecoveryPlan::DiscardStaleJournal
        ));
    }

    #[test]
    fn stale_journal_action_discards_raw_key_without_inspecting_json() {
        // When the committed checkpoint matches the on-chain header, a
        // lingering journal key is discarded by raw presence alone. The
        // decision never inspects the journal JSON, so a corrupt or bad-JSON
        // stale journal (which cannot be deserialized into a CheckpointJournal)
        // is still safely discarded instead of blocking recovery.
        assert!(matches!(
            stale_journal_action(true, true),
            StaleJournalAction::DiscardRaw
        ));
        assert!(matches!(
            stale_journal_action(true, false),
            StaleJournalAction::UpToDate
        ));
        // The advanced case is handled elsewhere; this helper is only consulted
        // on the committed-matches path and reports UpToDate.
        assert!(matches!(
            stale_journal_action(false, true),
            StaleJournalAction::UpToDate
        ));
    }

    #[test]
    fn mint_recovery_succeeds_when_already_complete_even_if_fourth_write_missing() {
        // The fourth journal write (mint_progress) did not happen, so recovery
        // re-runs mint processing. If every group was already claimed on-chain,
        // the BridgeClient skips them and reports total_mints_processed == 0
        // this run but fully_completed == true. Recovery must accept this and
        // NOT compare this run's processed count to the original total.
        let already_complete = ProcessMintsResult::new(0, 0, Vec::new(), true);
        assert!(mint_recovery_succeeded(&already_complete).is_ok());

        // A genuinely incomplete recovery (fully_completed == false) must fail.
        let incomplete = ProcessMintsResult::new(2, 10, Vec::new(), false);
        assert!(mint_recovery_succeeded(&incomplete).is_err());
    }

    #[test]
    fn journal_recovery_plan_completes_mint_after_block_update_before_mint_crash() {
        // block_update was submitted (on-chain header advanced to H+1) but
        // the pipeline crashed before processing mints. Recovery must
        // idempotently complete minting using the persisted finalized
        // commitment and uploaded buffers, then promote the journal
        // checkpoint.
        let mut committed = empty_checkpoint(42);
        committed.header_hex = hex::encode([1u8; HEADER_SIZE]); // stale vs advanced chain
        let chain_header = PsyBridgeHeader::default(); // matches journal checkpoint (zeros)
        let journal = test_journal(empty_checkpoint(43), true, true, true, false);
        match plan_journal_recovery(&committed, &chain_header, Some(&journal)) {
            JournalRecoveryPlan::CompleteMint {
                checkpoint,
                finalized_commitment,
                uploaded_buffers,
            } => {
                assert_eq!(checkpoint.height, 43);
                assert!(finalized_commitment.pending_mints.is_empty());
                assert_eq!(uploaded_buffers.mint_buffer, Pubkey::default());
            }
            other => panic!("expected CompleteMint, got {other:?}"),
        }
    }

    #[test]
    fn journal_recovery_plan_promotes_checkpoint_when_mint_already_done() {
        let mut committed = empty_checkpoint(42);
        committed.header_hex = hex::encode([1u8; HEADER_SIZE]);
        let chain_header = PsyBridgeHeader::default();
        let journal = test_journal(empty_checkpoint(43), true, true, true, true);
        match plan_journal_recovery(&committed, &chain_header, Some(&journal)) {
            JournalRecoveryPlan::PromoteCheckpoint { checkpoint } => {
                assert_eq!(checkpoint.height, 43);
            }
            other => panic!("expected PromoteCheckpoint, got {other:?}"),
        }
    }

    #[test]
    fn journal_recovery_plan_fails_closed_without_journal() {
        let mut committed = empty_checkpoint(42);
        committed.header_hex = hex::encode([1u8; HEADER_SIZE]);
        let chain_header = PsyBridgeHeader::default();
        match plan_journal_recovery(&committed, &chain_header, None) {
            JournalRecoveryPlan::FailClosed(reason) => {
                assert!(reason.contains("without a pending journal"), "{reason}");
            }
            other => panic!("expected FailClosed, got {other:?}"),
        }
    }

    #[test]
    fn journal_recovery_plan_fails_closed_without_finalized_commitment() {
        let mut committed = empty_checkpoint(42);
        committed.header_hex = hex::encode([1u8; HEADER_SIZE]);
        let chain_header = PsyBridgeHeader::default();
        let journal = test_journal(empty_checkpoint(43), false, true, true, false);
        match plan_journal_recovery(&committed, &chain_header, Some(&journal)) {
            JournalRecoveryPlan::FailClosed(reason) => {
                assert!(reason.contains("finalized commitment"), "{reason}");
            }
            other => panic!("expected FailClosed, got {other:?}"),
        }
    }

    #[test]
    fn journal_recovery_plan_fails_closed_without_uploaded_buffers() {
        let mut committed = empty_checkpoint(42);
        committed.header_hex = hex::encode([1u8; HEADER_SIZE]);
        let chain_header = PsyBridgeHeader::default();
        let journal = test_journal(empty_checkpoint(43), true, false, true, false);
        match plan_journal_recovery(&committed, &chain_header, Some(&journal)) {
            JournalRecoveryPlan::FailClosed(reason) => {
                assert!(reason.contains("uploaded buffer addresses"), "{reason}");
            }
            other => panic!("expected FailClosed, got {other:?}"),
        }
    }

    #[test]
    fn journal_recovery_plan_fails_closed_on_height_mismatch() {
        let mut committed = empty_checkpoint(42);
        committed.header_hex = hex::encode([1u8; HEADER_SIZE]);
        let chain_header = PsyBridgeHeader::default();
        // Journal checkpoint is two ahead, not exactly one.
        let journal = test_journal(empty_checkpoint(44), true, true, true, false);
        match plan_journal_recovery(&committed, &chain_header, Some(&journal)) {
            JournalRecoveryPlan::FailClosed(reason) => {
                assert!(reason.contains("not exactly one above"), "{reason}");
            }
            other => panic!("expected FailClosed, got {other:?}"),
        }
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
        let helper_hashes = pending_mint_and_txo_hashes_from_claim_witness::<
            LocalRegtestManagerCustody,
        >(
            &witness.claim_witness,
            block.header.merkle_root,
            &CUSTODY_SCRIPT_CONFIG,
            0,
            0,
            100,
        )
        .unwrap();
        assert_eq!(
            helper_hashes,
            (evaluation.pending_mints_hash, evaluation.txo_output_list_hash)
        );
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
    fn poisoned_redis_hashes_fail_finalized_resolver() {
        let (empty_buffers, empty_witness) = resolve_finalized_buffers::<
            LocalRegtestManagerCustody,
        >(None, None, &CUSTODY_SCRIPT_CONFIG, &[RECIPIENT_ATA], 0, 0, 100)
        .unwrap();
        assert!(empty_witness.is_empty());
        assert_eq!(
            empty_buffers.pending_mints_hash().unwrap(),
            PendingMintsGroupsBuilder::new_with_hint(0)
                .finalize()
                .unwrap()
        );
        assert_eq!(
            empty_buffers.txo_output_list_hash().unwrap(),
            hash_impl_sha256_bytes(&[])
        );

        let height = 101;
        let deposit_amount = 100_000_000;
        let block = block(
            height,
            vec![transaction(vec![BTCTransactionOutput {
                value: deposit_amount,
                script: get_manager_custody_output_script::<LocalRegtestManagerCustody>(
                    &CUSTODY_SCRIPT_CONFIG,
                    &RECIPIENT_ATA,
                )
                .to_vec(),
            }])],
        );
        let witness = build_hash_only_incoming_witness::<LocalRegtestManagerCustody>(
            &block,
            &CUSTODY_SCRIPT_CONFIG,
            &[RECIPIENT_ATA],
        )
        .unwrap();
        let (mint_hash, txo_hash) = pending_mint_and_txo_hashes_from_claim_witness::<
            LocalRegtestManagerCustody,
        >(
            &witness.claim_witness,
            block.header.merkle_root,
            &CUSTODY_SCRIPT_CONFIG,
            0,
            0,
            100,
        )
        .unwrap();
        let mut poisoned = BlockBufferCommitment {
            pending_mints_hash_hex: hex::encode(mint_hash),
            txo_output_list_hash_hex: hex::encode(txo_hash),
            pending_mints: vec![PendingMintPayload {
                recipient: RECIPIENT_ATA,
                amount: deposit_amount,
            }],
            txo_indices: vec![0],
            deposit_count: 1,
            minted_amount_sats: deposit_amount,
            auto_claim_start_index: 0,
            auto_claim_end_index: 1,
            fees_collected: 0,
        };
        poisoned.pending_mints_hash_hex = hex::encode([0xAA; 32]);
        let error = resolve_finalized_buffers::<LocalRegtestManagerCustody>(
            Some(&block),
            Some(&poisoned),
            &CUSTODY_SCRIPT_CONFIG,
            &[RECIPIENT_ATA],
            0,
            0,
            100,
        )
        .unwrap_err();
        assert!(error.to_string().contains("pending-mint hash disagrees"));

        poisoned.pending_mints_hash_hex = hex::encode(mint_hash);
        poisoned.txo_output_list_hash_hex = hex::encode([0xBB; 32]);
        let error = resolve_finalized_buffers::<LocalRegtestManagerCustody>(
            Some(&block),
            Some(&poisoned),
            &CUSTODY_SCRIPT_CONFIG,
            &[RECIPIENT_ATA],
            0,
            0,
            100,
        )
        .unwrap_err();
        assert!(error.to_string().contains("TXO-list hash disagrees"));
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
