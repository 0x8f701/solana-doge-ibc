use std::{path::Path, time::Duration};

use anyhow::Context;
use doge_light_client::hash::sha256_impl::hash_impl_sha256_bytes;
use fred::{
    error::ErrorKind,
    prelude::{KeysInterface, ListInterface, LuaInterface},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::block_pipeline::{
    prepare_prover_request, BlockPipelineConfig, ProverDaemon, ProverOutput, ProverRequestError,
};

pub const PROOF_SCHEMA_VERSION: u32 = 2;
pub const DEFAULT_QUEUE_PREFIX: &str = "PDOGE-SP1-PROOF-V2";

/// Atomically creates a durable job, or accepts an identical previously-created job.
/// Completed/failed jobs are intentionally retained so a restarted submitter can reuse the
/// terminal result for the same content-addressed identity.
pub const SUBMIT_JOB_SCRIPT: &str = r#"
local existing = redis.call('GET', KEYS[1])
if existing then
  if existing ~= ARGV[1] then return redis.error_reply('JOB_ID_COLLISION') end
  return 0
end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('SET', KEYS[2], '{\"status\":\"Queued\"}')
redis.call('SET', KEYS[3], '0')
redis.call('RPUSH', KEYS[4], ARGV[2])
return 1
"#;


#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum ProverBackendKind {
    #[default]
    Local,
    Redis,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProofRequest {
    pub request_id: String,
    pub old_state: String,
    pub witness: String,
    pub finalized_witness: String,
    pub custody_script_config: String,
    pub required_confirmations: u32,
    pub flat_fee: u64,
    pub fee_num: u64,
    pub fee_den: u64,
    pub old_header: String,
    pub new_header: String,
    pub config_params: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProofJob {
    pub schema_version: u32,
    pub job_id: String,
    pub network: String,
    pub height: u32,
    pub parent_checkpoint_sha256: String,
    pub input_fingerprint: String,
    pub guest_id: String,
    pub block_elf_sha256: String,
    pub vkey_hash: String,
    pub request: ProofRequest,
}

impl ProofJob {
    pub fn new(
        network: String,
        height: u32,
        parent_checkpoint_sha256: String,
        guest_id: String,
        block_elf_sha256: String,
        vkey_hash: String,
        request: ProofRequest,
    ) -> anyhow::Result<Self> {
        let request_bytes = canonical_request_bytes(&request)?;
        let input_fingerprint = sha256_hex(&request_bytes);
        let mut job = Self {
            schema_version: PROOF_SCHEMA_VERSION,
            job_id: String::new(),
            network,
            height,
            parent_checkpoint_sha256,
            input_fingerprint,
            guest_id,
            block_elf_sha256,
            vkey_hash,
            request,
        };
        job.job_id = job.compute_job_id()?;
        job.validate()?;
        Ok(job)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.schema_version != PROOF_SCHEMA_VERSION {
            anyhow::bail!(
                "proof job schema version {} is unsupported",
                self.schema_version
            );
        }
        validate_hex_32(&self.parent_checkpoint_sha256, "parent checkpoint SHA-256")?;
        validate_hex_32(&self.input_fingerprint, "input fingerprint")?;
        validate_hex_32(&self.block_elf_sha256, "block ELF SHA-256")?;
        validate_hex_32(&self.vkey_hash, "vkey hash")?;
        let request_bytes = canonical_request_bytes(&self.request)?;
        let actual_fingerprint = sha256_hex(&request_bytes);
        if self.input_fingerprint != actual_fingerprint {
            anyhow::bail!(
                "proof job input fingerprint mismatch: expected {}, got {}",
                actual_fingerprint,
                self.input_fingerprint
            );
        }
        let actual_job_id = self.compute_job_id()?;
        if self.job_id != actual_job_id {
            anyhow::bail!(
                "proof job id mismatch: expected {actual_job_id}, got {}",
                self.job_id
            );
        }
        Ok(())
    }

    pub fn compute_job_id(&self) -> anyhow::Result<String> {
        let request_bytes = canonical_request_bytes(&self.request)?;
        let mut identity = Vec::with_capacity(request_bytes.len() + 256);
        identity.extend_from_slice(&self.schema_version.to_be_bytes());
        append_len_prefixed(&mut identity, self.network.as_bytes())?;
        identity.extend_from_slice(&self.height.to_be_bytes());
        identity.extend_from_slice(&decode_hex_32(
            &self.parent_checkpoint_sha256,
            "parent checkpoint SHA-256",
        )?);
        identity.extend_from_slice(&decode_hex_32(
            &self.input_fingerprint,
            "input fingerprint",
        )?);
        append_len_prefixed(&mut identity, self.guest_id.as_bytes())?;
        identity.extend_from_slice(&decode_hex_32(
            &self.block_elf_sha256,
            "block ELF SHA-256",
        )?);
        identity.extend_from_slice(&decode_hex_32(&self.vkey_hash, "vkey hash")?);
        append_len_prefixed(&mut identity, &request_bytes)?;
        Ok(sha256_hex(&identity))
    }

    pub fn identity_matches(&self, result: &ProofResult) -> bool {
        self.schema_version == result.schema_version
            && self.job_id == result.job_id
            && self.network == result.network
            && self.height == result.height
            && self.parent_checkpoint_sha256 == result.parent_checkpoint_sha256
            && self.input_fingerprint == result.input_fingerprint
            && self.guest_id == result.guest_id
            && self.block_elf_sha256 == result.block_elf_sha256
            && self.vkey_hash == result.vkey_hash
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status")]
pub enum ProofJobState {
    Queued,
    Claimed {
        worker_id: String,
        lease_id: String,
        lease_expires_ms: u64,
        attempt: u32,
    },
    Completed {
        completed_ms: u64,
        attempt: u32,
    },
    Failed {
        failed_ms: u64,
        attempt: u32,
        error: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProofResult {
    pub schema_version: u32,
    pub job_id: String,
    pub network: String,
    pub height: u32,
    pub parent_checkpoint_sha256: String,
    pub input_fingerprint: String,
    pub guest_id: String,
    pub block_elf_sha256: String,
    pub vkey_hash: String,
    pub outcome: ProofOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status")]
pub enum ProofOutcome {
    Success {
        response: serde_json::Value,
        stderr: String,
    },
    Error {
        error: String,
    },
}

impl ProofResult {
    fn from_job(job: &ProofJob, outcome: ProofOutcome) -> Self {
        Self {
            schema_version: job.schema_version,
            job_id: job.job_id.clone(),
            network: job.network.clone(),
            height: job.height,
            parent_checkpoint_sha256: job.parent_checkpoint_sha256.clone(),
            input_fingerprint: job.input_fingerprint.clone(),
            guest_id: job.guest_id.clone(),
            block_elf_sha256: job.block_elf_sha256.clone(),
            vkey_hash: job.vkey_hash.clone(),
            outcome,
        }
    }

    pub fn success(
        job: &ProofJob,
        response: serde_json::Value,
        stderr: Vec<u8>,
    ) -> Self {
        Self::from_job(
            job,
            ProofOutcome::Success {
                response,
                stderr: hex::encode(stderr),
            },
        )
    }

    pub fn error(job: &ProofJob, error: String) -> Self {
        Self::from_job(job, ProofOutcome::Error { error })
    }

    pub fn validate_for(&self, job: &ProofJob) -> anyhow::Result<()> {
        job.validate()?;
        if !job.identity_matches(self) {
            anyhow::bail!(
                "proof result identity mismatch for expected job {}",
                job.job_id
            );
        }
        Ok(())
    }

    pub(crate) fn into_prover_output(self, job: &ProofJob) -> anyhow::Result<ProverOutput> {
        self.validate_for(job)?;
        let (response, stderr) = match self.outcome {
            ProofOutcome::Error { error } => {
                return Err(anyhow::Error::new(ProverRequestError(format!(
                    "proof job {} failed: {error}",
                    job.job_id
                ))));
            }
            ProofOutcome::Success { response, stderr } => {
                let stderr = hex::decode(&stderr).context("invalid proof result stderr hex")?;
                (response, stderr)
            }
        };
        let response_bytes = serde_json::to_vec(&response)?;
        let response: ProverProofResponse = serde_json::from_value(response)
            .context("malformed proof result daemon response")?;
        validate_daemon_response(job, &response)?;
        let proof = decode_required_response_bytes(response.proof_bytes.as_deref(), "proof_bytes")?;
        let public_values =
            decode_required_response_bytes(response.public_values.as_deref(), "public_values")?;
        if response.proof_size != Some(proof.len()) {
            anyhow::bail!(
                "proof result proof_size {:?} did not match {} returned bytes",
                response.proof_size,
                proof.len()
            );
        }
        if response.public_values_size != Some(public_values.len()) {
            anyhow::bail!(
                "proof result public_values_size {:?} did not match {} returned bytes",
                response.public_values_size,
                public_values.len()
            );
        }
        Ok(ProverOutput {
            stdout: response_bytes,
            stderr,
            vk_hash: decode_hex_32(&job.vkey_hash, "vkey hash")?,
            proof,
            public_values,
        })
    }
}

#[derive(Debug, Deserialize)]
struct ProverProofResponse {
    kind: String,
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

fn validate_daemon_response(job: &ProofJob, response: &ProverProofResponse) -> anyhow::Result<()> {
    if response.kind != "proof" {
        anyhow::bail!("proof response kind was '{}', expected 'proof'", response.kind);
    }
    if response.request_id.as_deref() != Some(job.request.request_id.as_str()) {
        anyhow::bail!("proof response request_id did not match queued request");
    }
    if !response.ok {
        anyhow::bail!(
            "successful proof result wrapped a daemon error: {}",
            response.error.as_deref().unwrap_or("missing error")
        );
    }
    for (name, actual, expected) in [
        ("network", response.network.as_deref(), job.network.as_str()),
        ("guest_id", response.guest_id.as_deref(), job.guest_id.as_str()),
        (
            "block_elf_sha256",
            response.block_elf_sha256.as_deref(),
            job.block_elf_sha256.as_str(),
        ),
        ("vkey_hash", response.vkey_hash.as_deref(), job.vkey_hash.as_str()),
    ] {
        if actual != Some(expected) {
            anyhow::bail!(
                "proof response {name} mismatch: expected {expected}, got {:?}",
                actual
            );
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseCasError {
    NotQueued,
    NotClaimed,
    FenceMismatch,
    Expired,
}

pub fn claim_state(
    state: &ProofJobState,
    worker_id: String,
    lease_id: String,
    lease_expires_ms: u64,
    attempt: u32,
) -> Result<ProofJobState, LeaseCasError> {
    if !matches!(state, ProofJobState::Queued) {
        return Err(LeaseCasError::NotQueued);
    }
    Ok(ProofJobState::Claimed {
        worker_id,
        lease_id,
        lease_expires_ms,
        attempt,
    })
}

pub fn heartbeat_state(
    state: &ProofJobState,
    worker_id: &str,
    lease_id: &str,
    lease_expires_ms: u64,
    now_ms: u64,
) -> Result<ProofJobState, LeaseCasError> {
    match state {
        ProofJobState::Claimed {
            worker_id: claimed_worker,
            lease_id: claimed_lease,
            lease_expires_ms: current_expiry,
            attempt,
        } if claimed_worker == worker_id && claimed_lease == lease_id => {
            if *current_expiry <= now_ms {
                return Err(LeaseCasError::Expired);
            }
            Ok(ProofJobState::Claimed {
                worker_id: claimed_worker.clone(),
                lease_id: claimed_lease.clone(),
                lease_expires_ms,
                attempt: *attempt,
            })
        }
        ProofJobState::Claimed { .. } => Err(LeaseCasError::FenceMismatch),
        _ => Err(LeaseCasError::NotClaimed),
    }
}

pub fn complete_state(
    state: &ProofJobState,
    worker_id: &str,
    lease_id: &str,
    completed_ms: u64,
) -> Result<ProofJobState, LeaseCasError> {
    match state {
        ProofJobState::Claimed {
            worker_id: claimed_worker,
            lease_id: claimed_lease,
            lease_expires_ms,
            attempt,
        } if claimed_worker == worker_id && claimed_lease == lease_id => {
            if *lease_expires_ms <= completed_ms {
                return Err(LeaseCasError::Expired);
            }
            Ok(ProofJobState::Completed {
                completed_ms,
                attempt: *attempt,
            })
        }
        ProofJobState::Claimed { .. } => Err(LeaseCasError::FenceMismatch),
        _ => Err(LeaseCasError::NotClaimed),
    }
}

pub fn fail_state(
    state: &ProofJobState,
    worker_id: &str,
    lease_id: &str,
    failed_ms: u64,
    error: String,
) -> Result<ProofJobState, LeaseCasError> {
    match complete_state(state, worker_id, lease_id, failed_ms)? {
        ProofJobState::Completed { attempt, .. } => Ok(ProofJobState::Failed {
            failed_ms,
            attempt,
            error,
        }),
        _ => unreachable!("complete_state only returns Completed"),
    }
}

#[derive(Clone)]
pub enum ProofBackend {
    Local(LocalProofBackend),
    Redis(RedisProofBackend),
}

pub enum ProofTicket {
    Local {
        job: ProofJob,
        result: oneshot::Receiver<anyhow::Result<ProofResult>>,
    },
    Redis {
        job: ProofJob,
    },
}

impl ProofBackend {
    pub async fn local(config: BlockPipelineConfig) -> anyhow::Result<Self> {
        Ok(Self::Local(LocalProofBackend::start(config).await?))
    }

    pub fn redis(
        pool: fred::prelude::Pool,
        namespace: String,
        wait_interval: Duration,
    ) -> Self {
        Self::Redis(RedisProofBackend {
            pool,
            keys: QueueKeys::new(namespace),
            wait_interval,
        })
    }

    pub async fn submit(&self, job: ProofJob) -> anyhow::Result<ProofTicket> {
        job.validate()?;
        match self {
            Self::Local(backend) => backend.submit(job).await,
            Self::Redis(backend) => backend.submit(job).await,
        }
    }

    pub async fn wait(&self, ticket: ProofTicket) -> anyhow::Result<ProverOutput> {
        let (job, result) = match (self, ticket) {
            (Self::Local(_), ProofTicket::Local { job, result }) => {
                let result = result
                    .await
                    .map_err(|_| anyhow::anyhow!("local proof task stopped before returning a result"))??;
                (job, result)
            }
            (Self::Redis(backend), ProofTicket::Redis { job }) => {
                let result = backend.wait_result(&job).await?;
                (job, result)
            }
            _ => anyhow::bail!("proof ticket belongs to a different backend"),
        };
        result.into_prover_output(&job)
    }

    pub async fn shutdown(&self) {
        if let Self::Local(backend) = self {
            backend.shutdown().await;
        }
    }
}

struct LocalRequest {
    job: ProofJob,
    result: oneshot::Sender<anyhow::Result<ProofResult>>,
}

#[derive(Clone)]
pub struct LocalProofBackend {
    sender: mpsc::Sender<LocalRequest>,
    shutdown: mpsc::Sender<()>,
}

impl LocalProofBackend {
    async fn start(config: BlockPipelineConfig) -> anyhow::Result<Self> {
        let daemon = ProverDaemon::start(&config).await?;
        let (sender, mut requests) = mpsc::channel::<LocalRequest>(config.proof_prepare_window);
        let (shutdown, mut shutdown_rx) = mpsc::channel::<()>(1);
        tokio::spawn(async move {
            let mut daemon = Some(daemon);
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_rx.recv() => break,
                    request = requests.recv() => {
                        let Some(request) = request else { break; };
                        let result = run_local_job(&config, &mut daemon, &request.job).await;
                        let _ = request.result.send(result);
                    }
                }
            }
            if let Some(daemon) = daemon {
                daemon.terminate().await;
            }
        });
        Ok(Self { sender, shutdown })
    }

    async fn submit(&self, job: ProofJob) -> anyhow::Result<ProofTicket> {
        let (result_tx, result_rx) = oneshot::channel();
        self.sender
            .send(LocalRequest {
                job: job.clone(),
                result: result_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("local proof backend is stopped"))?;
        Ok(ProofTicket::Local {
            job,
            result: result_rx,
        })
    }

    async fn shutdown(&self) {
        let _ = self.shutdown.send(()).await;
    }
}

async fn run_local_job(
    config: &BlockPipelineConfig,
    daemon: &mut Option<ProverDaemon>,
    job: &ProofJob,
) -> anyhow::Result<ProofResult> {
    let request = &job.request;
    prepare_prover_request(config, request).await?;
    let first = daemon
        .as_mut()
        .ok_or_else(|| anyhow::anyhow!("local gen-proof daemon is stopped"))?
        .prove(config, request)
        .await;
    match first {
        Ok(output) => local_output_result(job, output),
        Err(error) if error.downcast_ref::<ProverRequestError>().is_some() => {
            Ok(ProofResult::error(job, format!("{error:#}")))
        }
        Err(first_error) => {
            if let Some(old) = daemon.take() {
                old.terminate().await;
            }
            let mut restarted = ProverDaemon::start(config).await.map_err(|restart_error| {
                anyhow::anyhow!(
                    "gen-proof daemon request failed: {first_error:#}; restart/revalidation failed: {restart_error:#}"
                )
            })?;
            let retry = restarted.prove(config, request).await;
            match retry {
                Ok(output) => {
                    *daemon = Some(restarted);
                    local_output_result(job, output)
                }
                Err(retry_error) => {
                    restarted.terminate().await;
                    Ok(ProofResult::error(
                        job,
                        format!(
                            "gen-proof daemon request failed: {first_error:#}; retry after restart failed: {retry_error:#}"
                        ),
                    ))
                }
            }
        }
    }
}

fn local_output_result(job: &ProofJob, output: ProverOutput) -> anyhow::Result<ProofResult> {
    let response = serde_json::from_slice(&output.stdout)
        .context("local gen-proof returned malformed JSON")?;
    Ok(ProofResult::success(job, response, output.stderr))
}

#[derive(Debug, Clone)]
pub struct QueueKeys {
    pub namespace: String,
    pub queue: String,
    pub notify: String,
    pub leases: String,
}

impl QueueKeys {
    pub fn new(namespace: String) -> Self {
        Self {
            queue: format!("{namespace}:queue"),
            notify: format!("{namespace}:notify"),
            leases: format!("{namespace}:leases"),
            namespace,
        }
    }

    pub fn job(&self, job_id: &str) -> String {
        format!("{}:job:{job_id}", self.namespace)
    }

    pub fn state(&self, job_id: &str) -> String {
        format!("{}:state:{job_id}", self.namespace)
    }

    pub fn result(&self, job_id: &str) -> String {
        format!("{}:result:{job_id}", self.namespace)
    }

    pub fn attempt(&self, job_id: &str) -> String {
        format!("{}:attempt:{job_id}", self.namespace)
    }
}

#[derive(Clone)]
pub struct RedisProofBackend {
    pool: fred::prelude::Pool,
    keys: QueueKeys,
    wait_interval: Duration,
}

impl RedisProofBackend {
    async fn submit(&self, job: ProofJob) -> anyhow::Result<ProofTicket> {
        let job_json = serde_json::to_string(&job)?;
        self.pool
            .eval::<i64, _, _, _>(
                SUBMIT_JOB_SCRIPT,
                vec![
                    self.keys.job(&job.job_id),
                    self.keys.state(&job.job_id),
                    self.keys.attempt(&job.job_id),
                    self.keys.queue.clone(),
                ],
                vec![job_json, job.job_id.clone()],
            )
            .await
            .context("failed to durably enqueue proof job")?;
        Ok(ProofTicket::Redis { job })
    }

    async fn wait_result(&self, job: &ProofJob) -> anyhow::Result<ProofResult> {
        loop {
            if let Some(result_json) = self
                .pool
                .get::<Option<String>, _>(self.keys.result(&job.job_id))
                .await
                .context("failed to read proof result")?
            {
                let result: ProofResult = serde_json::from_str(&result_json)
                    .context("malformed durable proof result")?;
                result.validate_for(job)?;
                return Ok(result);
            }

            let state_json = self
                .pool
                .get::<Option<String>, _>(self.keys.state(&job.job_id))
                .await
                .context("failed to read proof job state")?
                .ok_or_else(|| anyhow::anyhow!("durable proof job {} disappeared", job.job_id))?;
            let state: ProofJobState = serde_json::from_str(&state_json)
                .context("malformed durable proof job state")?;
            match state {
                ProofJobState::Failed { error, .. } => {
                    anyhow::bail!("proof job {} failed permanently: {error}", job.job_id);
                }
                ProofJobState::Completed { .. } => {
                    anyhow::bail!(
                        "proof job {} is Completed but its durable result is missing",
                        job.job_id
                    );
                }
                ProofJobState::Queued | ProofJobState::Claimed { .. } => {}
            }

            let wait_secs = self.wait_interval.as_secs_f64().max(0.001);
            let notification = tokio::time::timeout(
                self.wait_interval + Duration::from_secs(1),
                self.pool
                    .brpop::<Option<(String, String)>, _>(&self.keys.notify, wait_secs),
            )
            .await;
            if let Ok(Err(error)) = notification {
                // A notification is only a wake hint; a transient blocking-pop failure does not
                // change the durable job/result state checked at the top of the loop.
                if error.kind() != &ErrorKind::Timeout {
                    tokio::time::sleep(self.wait_interval).await;
                }
            }
        }
    }
}

pub fn proof_namespace(prefix: &str, network: &str, seed: u64) -> String {
    format!("{prefix}-{network}-{seed}")
}

pub async fn file_sha256_hex(path: &Path) -> anyhow::Result<String> {
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("failed to read {}", path.display()))?;
    Ok(sha256_hex(&bytes))
}

pub fn canonical_request_bytes(request: &ProofRequest) -> anyhow::Result<Vec<u8>> {
    Ok(serde_json::to_vec(request)?)
}

fn append_len_prefixed(output: &mut Vec<u8>, bytes: &[u8]) -> anyhow::Result<()> {
    let len = u32::try_from(bytes.len()).context("proof identity field exceeds u32")?;
    output.extend_from_slice(&len.to_be_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

fn decode_hex_32(value: &str, name: &str) -> anyhow::Result<[u8; 32]> {
    let bytes = hex::decode(value).with_context(|| format!("invalid {name} hex"))?;
    bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| anyhow::anyhow!("{name} must be 32 bytes, got {}", bytes.len()))
}

fn validate_hex_32(value: &str, name: &str) -> anyhow::Result<()> {
    let _ = decode_hex_32(value, name)?;
    if value.len() != 64 || value.bytes().any(|byte| byte.is_ascii_uppercase()) {
        anyhow::bail!("{name} must use 64 lowercase hexadecimal characters");
    }
    Ok(())
}

fn decode_required_response_bytes(value: Option<&str>, name: &str) -> anyhow::Result<Vec<u8>> {
    let value = value.ok_or_else(|| anyhow::anyhow!("proof response is missing {name}"))?;
    hex::decode(value).with_context(|| format!("invalid proof response {name} hex"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(hash_impl_sha256_bytes(bytes))
}


#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> ProofRequest {
        ProofRequest {
            request_id: "block-42".to_owned(),
            old_state: "00".to_owned(),
            witness: "11".to_owned(),
            finalized_witness: String::new(),
            custody_script_config: "22".repeat(32),
            required_confirmations: 6,
            flat_fee: 1,
            fee_num: 2,
            fee_den: 3,
            old_header: "33".repeat(320),
            new_header: "44".repeat(320),
            config_params: "55".repeat(48),
        }
    }

    fn job() -> ProofJob {
        ProofJob::new(
            "testnet".to_owned(),
            42,
            "66".repeat(32),
            "block-transition-testnet".to_owned(),
            "77".repeat(32),
            "88".repeat(32),
            request(),
        )
        .unwrap()
    }

    #[test]
    fn job_identity_binds_request_and_parent() {
        let job = job();
        job.validate().unwrap();
        let mut changed = job.clone();
        changed.request.witness = "12".to_owned();
        assert!(changed.validate().is_err());
        let mut changed = job.clone();
        changed.request.finalized_witness = "13".to_owned();
        assert!(changed.validate().is_err());
        let mut changed = job.clone();
        changed.parent_checkpoint_sha256 = "67".repeat(32);
        assert!(changed.validate().is_err());
    }

    #[test]
    fn restart_with_same_inputs_reuses_the_same_job_id() {
        let first = job();
        let restarted = ProofJob::new(
            first.network.clone(),
            first.height,
            first.parent_checkpoint_sha256.clone(),
            first.guest_id.clone(),
            first.block_elf_sha256.clone(),
            first.vkey_hash.clone(),
            first.request.clone(),
        )
        .unwrap();
        assert_eq!(first.input_fingerprint, restarted.input_fingerprint);
        assert_eq!(first.job_id, restarted.job_id);
    }

    #[test]
    fn result_identity_mismatch_fails_closed() {
        let job = job();
        let mut result = ProofResult::error(&job, "failed".to_owned());
        result.input_fingerprint = "99".repeat(32);
        assert!(result.validate_for(&job).is_err());
    }

    #[test]
    fn lease_transitions_require_matching_fence_and_live_lease() {
        let claimed = claim_state(
            &ProofJobState::Queued,
            "worker-a".to_owned(),
            "lease-a".to_owned(),
            200,
            1,
        )
        .unwrap();
        assert_eq!(
            heartbeat_state(&claimed, "worker-b", "lease-a", 300, 100),
            Err(LeaseCasError::FenceMismatch)
        );
        let heartbeat = heartbeat_state(&claimed, "worker-a", "lease-a", 300, 100).unwrap();
        assert!(matches!(
            complete_state(&heartbeat, "worker-a", "lease-a", 250).unwrap(),
            ProofJobState::Completed { attempt: 1, .. }
        ));
        assert_eq!(
            complete_state(&claimed, "worker-a", "lease-a", 200),
            Err(LeaseCasError::Expired)
        );
    }


    #[test]
    fn namespace_and_keys_match_protocol() {
        let namespace = proof_namespace(DEFAULT_QUEUE_PREFIX, "testnet", 1337);
        assert_eq!(namespace, "PDOGE-SP1-PROOF-V2-testnet-1337");
        let keys = QueueKeys::new(namespace);
        assert_eq!(keys.queue, "PDOGE-SP1-PROOF-V2-testnet-1337:queue");
        assert_eq!(keys.job("abc"), "PDOGE-SP1-PROOF-V2-testnet-1337:job:abc");
        assert_eq!(keys.state("abc"), "PDOGE-SP1-PROOF-V2-testnet-1337:state:abc");
        assert_eq!(keys.result("abc"), "PDOGE-SP1-PROOF-V2-testnet-1337:result:abc");
    }
    fn daemon_response_json(job: &ProofJob, proof: &[u8], public_values: &[u8]) -> serde_json::Value {
        serde_json::json!({
            "kind": "proof",
            "request_id": job.request.request_id,
            "ok": true,
            "network": job.network,
            "guest_id": job.guest_id,
            "block_elf_sha256": job.block_elf_sha256,
            "vkey_hash": job.vkey_hash,
            "proof_size": proof.len(),
            "proof_bytes": hex::encode(proof),
            "public_values_size": public_values.len(),
            "public_values": hex::encode(public_values),
        })
    }

    fn response_overriding(job: &ProofJob, overrides: serde_json::Value) -> serde_json::Value {
        let mut base = daemon_response_json(job, &[0xAB; 356], &[0xCD; 32]);
        if let (serde_json::Value::Object(base), serde_json::Value::Object(over)) = (&mut base, overrides)
        {
            for (key, value) in over {
                base.insert(key, value);
            }
        }
        base
    }

    #[test]
    fn into_prover_output_decodes_well_formed_success_result() {
        // The happy path of the durable-result -> prover-output decoder. A
        // well-formed daemon response with matching identity, correct
        // proof/public-values sizes, and valid hex decodes into the exact
        // bytes the worker produced, and the VK hash is bound to the job.
        let job = job();
        let proof = vec![0xABu8; 356];
        let public_values = vec![0xCDu8; 32];
        let result = ProofResult::success(
            &job,
            daemon_response_json(&job, &proof, &public_values),
            Vec::new(),
        );
        let output = result
            .into_prover_output(&job)
            .expect("well-formed success result must decode");
        assert_eq!(output.proof, proof);
        assert_eq!(output.public_values, public_values);
        assert_eq!(output.vk_hash, decode_hex_32(&job.vkey_hash, "vkey hash").unwrap());
    }

    #[test]
    fn into_prover_output_fails_closed_on_malformed_or_mismatched_responses() {
        // Every boundary the decoder defends is exercised: a wrong kind,
        // wrong request_id, daemon-reported error, mismatched identity
        // fields, missing bytes, or size/byte-count disagreement must all
        // fail closed rather than committing a bad proof.
        let job = job();
        let cases: &[(&str, serde_json::Value)] = &[
            ("wrong kind", response_overriding(&job, serde_json::json!({"kind": "identity"}))),
            (
                "wrong request_id",
                response_overriding(&job, serde_json::json!({"request_id": "block-999"})),
            ),
            (
                "daemon reported error",
                response_overriding(&job, serde_json::json!({"ok": false, "error": "boom"})),
            ),
            (
                "wrong network",
                response_overriding(&job, serde_json::json!({"network": "regtest"})),
            ),
            (
                "wrong guest_id",
                response_overriding(&job, serde_json::json!({"guest_id": "other"})),
            ),
            (
                "wrong block_elf_sha256",
                response_overriding(&job, serde_json::json!({"block_elf_sha256": "00".repeat(32)})),
            ),
            (
                "wrong vkey_hash",
                response_overriding(&job, serde_json::json!({"vkey_hash": "00".repeat(32)})),
            ),
            (
                "missing proof_bytes",
                response_overriding(&job, serde_json::json!({"proof_bytes": null})),
            ),
            (
                "missing public_values",
                response_overriding(&job, serde_json::json!({"public_values": null})),
            ),
            (
                "proof_size disagrees with bytes",
                response_overriding(&job, serde_json::json!({"proof_size": 999})),
            ),
            (
                "public_values_size disagrees with bytes",
                response_overriding(&job, serde_json::json!({"public_values_size": 999})),
            ),
            (
                "invalid proof_bytes hex",
                response_overriding(&job, serde_json::json!({"proof_bytes": "zz"})),
            ),
        ];
        for (name, response) in cases {
            let result = ProofResult::success(&job, response.clone(), Vec::new());
            assert!(
                result.into_prover_output(&job).is_err(),
                "case '{name}' should fail closed"
            );
        }

        // An Error outcome (permanent proof failure) must surface as a
        // ProverRequestError, never decode into a usable prover output.
        let error_result = ProofResult::error(&job, "permanent failure".to_owned());
        let error = error_result.into_prover_output(&job).err().expect("error outcome must fail closed");
        assert!(error.downcast_ref::<ProverRequestError>().is_some());
    }

    #[test]
    fn completed_proof_result_validates_for_recreated_job() {
        // Restart reconstructs the job from the same content-addressed
        // inputs. The previously-completed result must still validate
        // against the recreated job and decode back into the same proof
        // bytes, so a restarted submitter reuses the terminal result.
        let job = job();
        let proof = vec![0xABu8; 356];
        let public_values = vec![0xCDu8; 32];
        let result = ProofResult::success(
            &job,
            daemon_response_json(&job, &proof, &public_values),
            Vec::new(),
        );
        let restarted = ProofJob::new(
            job.network.clone(),
            job.height,
            job.parent_checkpoint_sha256.clone(),
            job.guest_id.clone(),
            job.block_elf_sha256.clone(),
            job.vkey_hash.clone(),
            job.request.clone(),
        )
        .unwrap();
        assert_eq!(restarted.job_id, job.job_id);
        assert!(restarted.identity_matches(&result));
        result
            .validate_for(&restarted)
            .expect("completed result validates for the recreated job");
        let output = result
            .into_prover_output(&restarted)
            .expect("restart reuses the completed proof");
        assert_eq!(output.proof, proof);
        assert_eq!(output.public_values, public_values);
    }

    #[test]
    fn fail_state_requires_matching_fence_and_live_lease_and_preserves_error() {
        // fail_state delegates to complete_state's CAS, so it must inherit
        // every fence/expiry/state rejection and preserve the error + attempt
        // on the only legal transition (matching worker/lease, live lease).
        let claimed = claim_state(
            &ProofJobState::Queued,
            "worker-a".to_owned(),
            "lease-a".to_owned(),
            200,
            7,
        )
        .unwrap();

        assert_eq!(
            fail_state(&claimed, "worker-b", "lease-a", 100, "e".to_owned()),
            Err(LeaseCasError::FenceMismatch)
        );
        assert_eq!(
            fail_state(&claimed, "worker-a", "lease-b", 100, "e".to_owned()),
            Err(LeaseCasError::FenceMismatch)
        );
        // lease_expires_ms == failed_ms is expired (the script uses <=).
        assert_eq!(
            fail_state(&claimed, "worker-a", "lease-a", 200, "e".to_owned()),
            Err(LeaseCasError::Expired)
        );
        assert_eq!(
            fail_state(
                &ProofJobState::Queued,
                "worker-a",
                "lease-a",
                100,
                "e".to_owned()
            ),
            Err(LeaseCasError::NotClaimed)
        );
        assert_eq!(
            fail_state(
                &ProofJobState::Completed {
                    completed_ms: 90,
                    attempt: 1
                },
                "worker-a",
                "lease-a",
                100,
                "e".to_owned()
            ),
            Err(LeaseCasError::NotClaimed)
        );

        let failed = fail_state(&claimed, "worker-a", "lease-a", 100, "boom".to_owned())
            .expect("matching fence + live lease transitions to Failed");
        match failed {
            ProofJobState::Failed {
                attempt,
                error,
                failed_ms,
            } => {
                assert_eq!(attempt, 7, "attempt is carried from the claim");
                assert_eq!(error, "boom");
                assert_eq!(failed_ms, 100);
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn complete_state_rejects_wrong_worker_fence() {
        // complete_state's fence check is the gate both completion and
        // failure rely on; a wrong worker must be rejected even on a live
        // lease (previously only heartbeat's fence mismatch was covered).
        let claimed = claim_state(
            &ProofJobState::Queued,
            "worker-a".to_owned(),
            "lease-a".to_owned(),
            200,
            1,
        )
        .unwrap();
        assert_eq!(
            complete_state(&claimed, "worker-b", "lease-a", 100),
            Err(LeaseCasError::FenceMismatch)
        );
        assert_eq!(
            complete_state(&claimed, "worker-a", "lease-b", 100),
            Err(LeaseCasError::FenceMismatch)
        );
        // The matching fence on a live lease still completes.
        assert!(matches!(
            complete_state(&claimed, "worker-a", "lease-a", 100).unwrap(),
            ProofJobState::Completed { attempt: 1, .. }
        ));
    }

    #[test]
    fn validate_rejects_uppercase_hex_wrong_byte_length_and_schema_mismatch() {
        // Uppercase hex decodes to 32 bytes but must still be rejected: the
        // canonical identity is lowercase-only, so a swapped-case fingerprint
        // can never sneak through as the same content-addressed job.
        let uppercase_parent = "ab".repeat(32).to_uppercase();
        assert!(ProofJob::new(
            "testnet".to_owned(),
            42,
            uppercase_parent,
            "block-transition-testnet".to_owned(),
            "77".repeat(32),
            "88".repeat(32),
            request(),
        )
        .is_err());

        // 66 hex chars decode to 33 bytes, which is not a 32-byte hash.
        assert!(ProofJob::new(
            "testnet".to_owned(),
            42,
            "6".repeat(66),
            "block-transition-testnet".to_owned(),
            "77".repeat(32),
            "88".repeat(32),
            request(),
        )
        .is_err());

        // Schema version mismatch is rejected before the job_id check.
        let mut tampered = job();
        tampered.schema_version = PROOF_SCHEMA_VERSION + 1;
        assert!(tampered.validate().is_err());
    }
    /// Real-Redis behavior test for durable submission and coordinator waiting.
    /// Ignored by default; runs only when `TEST_REDIS_URL` points at an isolated
    /// instance the test is allowed to mutate. Asserts observable Redis state
    /// rather than script text.
    #[tokio::test]
    #[ignore = "requires an isolated Redis reachable via TEST_REDIS_URL"]
    async fn redis_behavior_submit_and_wait_preserves_expired_claim() {
        use fred::prelude::{Builder, ClientLike, Config, KeysInterface, ListInterface, LuaInterface};
        use std::time::{SystemTime, UNIX_EPOCH};

        let Some(url) = std::env::var("TEST_REDIS_URL")
            .ok()
            .filter(|s| !s.is_empty())
        else {
            eprintln!(
                "redis_behavior_submit_and_wait_preserves_expired_claim: TEST_REDIS_URL not set; skipping"
            );
            return;
        };

        let config = Config::from_url(&url).expect("invalid TEST_REDIS_URL");
        let pool = Builder::from_config(config)
            .build_pool(2)
            .expect("failed to build Redis pool");
        // fred pools must be explicitly connected before commands; mirror
        // the production init with a bounded deadline so a dead instance
        // fails fast instead of hanging the whole test binary.
        let _connect = tokio::time::timeout(Duration::from_secs(5), pool.init())
            .await
            .expect("redis init exceeded 5s")
            .expect("failed to connect to isolated Redis");

        // Unique namespace so concurrent runs and anything else on the shared
        // test instance never collide.
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let namespace = format!("qa-behavior-{}-{}", std::process::id(), nanos);
        let keys = QueueKeys::new(namespace.clone());

        // Run the assertions through a Result-returning helper so cleanup
        // always executes, even when an assertion fails.
        let outcome = run_submit_and_wait_behavior(&pool, &keys).await;

        // Exhaustive, hermetic cleanup: delete every key under this unique
        // namespace regardless of which ones the test actually created.
        let pattern = format!("{namespace}*");
        let deleted: i64 = pool
            .eval::<i64, _, _, _>(
                r#"local ks = redis.call('KEYS', ARGV[1]) for _, k in ipairs(ks) do redis.call('DEL', k) end return #ks"#,
                Vec::<String>::new(),
                vec![pattern],
            )
            .await
            .expect("cleanup script must succeed");
        assert!(
            deleted >= 0,
            "cleanup should have deleted keys under {namespace}* (deleted {deleted})"
        );

        outcome.expect("submit/wait behavior contract must hold");
    }

    async fn run_submit_and_wait_behavior(
        pool: &fred::prelude::Pool,
        keys: &QueueKeys,
    ) -> anyhow::Result<()> {
        use fred::prelude::{KeysInterface, ListInterface, LuaInterface};

        // ---- SUBMIT: first durable enqueue of a brand-new job. ----
        let job = job();
        let job_json = serde_json::to_string(&job).context("serialize proof job")?;
        let created: i64 = pool
            .eval::<i64, _, _, _>(
                SUBMIT_JOB_SCRIPT,
                vec![
                    keys.job(&job.job_id),
                    keys.state(&job.job_id),
                    keys.attempt(&job.job_id),
                    keys.queue.clone(),
                ],
                vec![job_json.clone(), job.job_id.clone()],
            )
            .await
            .context("first SUBMIT_JOB_SCRIPT eval failed")?;
        anyhow::ensure!(created == 1, "first submit must return 1, got {created}");

        let state: Option<String> = pool.get(keys.state(&job.job_id)).await?;
        anyhow::ensure!(
            state.as_deref() == Some(r#"{"status":"Queued"}"#),
            "first submit must write the Queued state, got {state:?}"
        );
        let attempt: Option<String> = pool.get(keys.attempt(&job.job_id)).await?;
        anyhow::ensure!(
            attempt.as_deref() == Some("0"),
            "first submit must initialize attempt to '0', got {attempt:?}"
        );
        let queue: Vec<String> = pool.lrange(keys.queue.clone(), 0, -1).await?;
        anyhow::ensure!(
            queue == vec![job.job_id.clone()],
            "first submit must enqueue the job id exactly once, got {queue:?}"
        );

        // ---- SUBMIT: identical resubmit is idempotent (accepted, no dup). ----
        let dup: i64 = pool
            .eval::<i64, _, _, _>(
                SUBMIT_JOB_SCRIPT,
                vec![
                    keys.job(&job.job_id),
                    keys.state(&job.job_id),
                    keys.attempt(&job.job_id),
                    keys.queue.clone(),
                ],
                vec![job_json.clone(), job.job_id.clone()],
            )
            .await
            .context("idempotent SUBMIT_JOB_SCRIPT eval failed")?;
        anyhow::ensure!(dup == 0, "idempotent resubmit must return 0, got {dup}");
        let queue_after_dup: Vec<String> = pool.lrange(keys.queue.clone(), 0, -1).await?;
        anyhow::ensure!(
            queue_after_dup == vec![job.job_id.clone()],
            "idempotent resubmit must not duplicate the queue entry, got {queue_after_dup:?}"
        );

        // ---- SUBMIT: a different payload under the same job id collides. ----
        pool.set::<(), _, _>(keys.job(&job.job_id), "garbage", None, None, false)
            .await?;
        let collide = pool
            .eval::<i64, _, _, _>(
                SUBMIT_JOB_SCRIPT,
                vec![
                    keys.job(&job.job_id),
                    keys.state(&job.job_id),
                    keys.attempt(&job.job_id),
                    keys.queue.clone(),
                ],
                vec![job_json.clone(), job.job_id.clone()],
            )
            .await;
        let err = collide
            .err()
            .ok_or_else(|| anyhow::anyhow!("collision must surface an error, got Ok"))?;
        anyhow::ensure!(
            err.to_string().contains("JOB_ID_COLLISION"),
            "collision error must map to JOB_ID_COLLISION, got {err}"
        );

        // ---- WAIT: an expired Claimed lease remains exclusively worker-owned. ----
        // Model a worker claim by removing the queued entry, recording the
        // durable Claimed state, and adding its expired lease. The coordinator
        // must remain pending without changing any of those worker-owned keys.
        let claimed_state = ProofJobState::Claimed {
            worker_id: "worker-a".to_owned(),
            lease_id: "lease-a".to_owned(),
            lease_expires_ms: 1,
            attempt: 3,
        };
        let claimed_json = serde_json::to_string(&claimed_state)?;
        pool.eval::<i64, _, _, _>(
            r#"
redis.call('LREM', KEYS[1], 0, ARGV[1])
redis.call('SET', KEYS[2], ARGV[2])
redis.call('ZADD', KEYS[3], ARGV[3], ARGV[1])
return 1
"#,
            vec![
                keys.queue.clone(),
                keys.state(&job.job_id),
                keys.leases.clone(),
            ],
            vec![job.job_id.clone(), claimed_json.clone(), "1".to_owned()],
        )
        .await
        .context("failed to seed expired Claimed lease")?;

        let backend = RedisProofBackend {
            pool: pool.clone(),
            keys: keys.clone(),
            wait_interval: Duration::from_millis(10),
        };
        let wait = tokio::time::timeout(
            Duration::from_millis(150),
            backend.wait_result(&job),
        )
        .await;
        anyhow::ensure!(
            wait.is_err(),
            "coordinator wait must remain pending while the worker owns an expired claim"
        );

        let state_after: Option<String> = pool.get(keys.state(&job.job_id)).await?;
        anyhow::ensure!(
            state_after.as_deref() == Some(claimed_json.as_str()),
            "coordinator wait must not rewrite expired Claimed state, got {state_after:?}"
        );
        let queue_after_wait: Vec<String> = pool.lrange(keys.queue.clone(), 0, -1).await?;
        anyhow::ensure!(
            queue_after_wait.is_empty(),
            "coordinator wait must not re-enqueue an expired claim, got {queue_after_wait:?}"
        );
        let lease_count: i64 = pool
            .eval::<i64, _, _, _>(
                r#"return redis.call('ZCARD', KEYS[1])"#,
                vec![keys.leases.clone()],
                Vec::<String>::new(),
            )
            .await?;
        anyhow::ensure!(
            lease_count == 1,
            "coordinator wait must leave the worker-owned expired lease intact, got ZCARD {lease_count}"
        );

        Ok(())
    }
}
