/*
Copyright (C) 2025 Zero Knowledge Labs Limited, Psy Protocol

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

Additional terms under GNU AGPL version 3 section 7:

As permitted by section 7(b) of the GNU Affero General Public License, 
you must retain the following attribution notice in all copies or 
substantial portions of the software:

"This software was created by Psy (https://Psy.xyz)
with contributions from Carter Feldman (https://x.com/cmpeq)."
*/

use std::time::Duration;

use async_trait::async_trait;
use borsh::BorshDeserialize;
use doge_light_client::{
    core_data::QDogeBlockHeader,
    hash::{sha256::QSha256Hasher, traits::BytesHasher},
};
use fred::prelude::{HashesInterface, KeysInterface, ListInterface, Pool};
use kvq::traits::KVQSerializable;
use qed_dsol_bridge_core::{
    data::{
        base_types::hash256::Hash256,
        scrypt_proof::{DogeBlockScryptProofInput, DogeBlockScryptProofOutput},
        state::{IBCBlockState, IBCBlockStateStoreReaderAsync, IBCBlockStateStoreWriterAsyncImm},
    },
    job::{
        drain_queue::{
            CheckpointDrainQueueConsumerAsyncImm, CheckpointDrainQueueEmitterAsyncImm,
            DQSerializable, DrainQueueMetadata,
        },
        g16_scrypt_traits::{
            BlockHeaderQueueItem, BlockProcessorWorkerEventReceiverAsyncImm, BlockProcessorWorkerEventTransmitterAsyncImm, QScryptProofStoreReaderAsync, QScryptProofStoreWriterAsyncImm, ScryptProverWorkerEventReceiverAsyncImm, ScryptProverWorkerEventTransmitterAsyncImm
        },
        history_queue::{
            CheckpointHistoryQueueConsumerAsyncImm, CheckpointHistoryQueueEmitterAsyncImm,
            HQSerializable,
        },
        id::QProvingJobDataID,
        worker_queue::{WorkerEventReceiverAsyncImm, WorkerEventTransmitterAsyncImm},
    },
};
use tokio::time::sleep;

pub const PROOF_STORE_KEY_PREFIX: &'static str = "PSV1";
pub const PROOF_STORE_COUNTERS_PREFIX: &'static str = "proof_counters";

pub const PS_DRAIN_QUEUE_KEY_PREFIX: &'static str = "PSDQV1_";
pub const PS_WORKER_QUEUE_KEY_PREFIX: &'static str = "PSWQV1";
pub const PS_NOTIFICATIONS_QUEUE_KEY_PREFIX: &'static str = "PSNQV1";
pub const PS_HISTORY_QUEUE_KEY_PREFIX: &'static str = "PSHQV1";

pub const PS_SCRYPT_WORKER_QUEUE_KEY_PREFIX: &'static str = "PSSCWQV1";
pub const PS_BLOCK_PROCESSOR_WORKER_QUEUE_KEY_PREFIX: &'static str = "PBPSCWQV1";

pub const PS_IBC_STATE_STORE_PREFIX: &'static str = "PSIBCV1";
pub const PS_IBC_STATE_STORE_LATEST_PREFIX: &'static str = "LPSIBCV1";

#[derive(Clone)]
pub struct ProofStoreFred {
    pool: Pool,
    worker_queue_id: String,
    scrypt_worker_queue_id: String,
    notifications_queue_id: String,
    block_processor_queue_id: String,
    ibc_state_store_suffix: String,
}

impl ProofStoreFred {
    pub fn new_with_seed(pool: Pool, seed: u64) -> Self {
        let worker_queue_suffix = format!("$wq{}a", seed);
        let scrypt_worker_queue_suffix = format!("$sq{}b", seed);
        let notifications_queue_suffix = format!("$nq{}c", seed);
        let block_processor_queue_suffix = format!("$bpq{}d", seed);
        let ibc_state_store_suffix = format!("$iss{}e", seed);
        Self::new(
            pool,
            worker_queue_suffix,
            scrypt_worker_queue_suffix,
            notifications_queue_suffix,
            block_processor_queue_suffix,
            ibc_state_store_suffix,
        )
    }
    pub fn new(
        pool: Pool,
        worker_queue_suffix: String,
        scrypt_worker_queue_suffix: String,
        notifications_queue_suffix: String,
        block_processor_queue_suffix: String,
        ibc_state_store_suffix: String,
    ) -> Self {
        Self {
            pool,
            worker_queue_id: format!("{}-{}", PS_WORKER_QUEUE_KEY_PREFIX, worker_queue_suffix),
            scrypt_worker_queue_id: format!(
                "{}-{}",
                PS_SCRYPT_WORKER_QUEUE_KEY_PREFIX, scrypt_worker_queue_suffix
            ),
            notifications_queue_id: format!(
                "{}-{}",
                PS_NOTIFICATIONS_QUEUE_KEY_PREFIX, notifications_queue_suffix
            ),
            block_processor_queue_id: format!(
                "{}-{}",
                PS_BLOCK_PROCESSOR_WORKER_QUEUE_KEY_PREFIX, block_processor_queue_suffix
            ),
            ibc_state_store_suffix: ibc_state_store_suffix,
        }
    }
}

#[async_trait]
impl QScryptProofStoreReaderAsync for ProofStoreFred {
    async fn get_scrypt_proof_by_scrypt_hash(
        &self,
        scrypt_hash: Hash256,
    ) -> anyhow::Result<DogeBlockScryptProofOutput> {
        let data = self
            .pool
            .hget::<Vec<u8>, _, &[u8]>(PROOF_STORE_KEY_PREFIX, &scrypt_hash.0)
            .await?;
        Ok(bincode::deserialize(&data)?)
    }
    async fn get_scrypt_proof_by_block_header(
        &self,
        block_header: [u8; 80],
    ) -> anyhow::Result<DogeBlockScryptProofOutput> {
        let hash = QSha256Hasher::hash_bytes(&block_header);
        self.get_scrypt_proof_by_block_hash(Hash256(hash)).await
    }
    async fn get_scrypt_proof_by_block_hash(
        &self,
        block_hash: Hash256,
    ) -> anyhow::Result<DogeBlockScryptProofOutput> {
        let data = self
            .pool
            .hget::<Vec<u8>, _, &[u8]>(PROOF_STORE_KEY_PREFIX, &block_hash.0)
            .await?;
        Ok(bincode::deserialize(&data)?)
    }
    async fn contains_scrypt_proof_for_block_hash(
        &self,
        block_hash: Hash256,
    ) -> anyhow::Result<bool> {
        let data = self
            .pool
            .hget::<Vec<u8>, _, &[u8]>(PROOF_STORE_KEY_PREFIX, &block_hash.0)
            .await?;
        Ok(data.len() != 0)
    }
}

#[async_trait]
impl QScryptProofStoreWriterAsyncImm for ProofStoreFred {
    async fn injest_scrypt_proof_result_imm(
        &self,
        proof: &DogeBlockScryptProofOutput,
    ) -> anyhow::Result<()> {
        let block_hash = QSha256Hasher::hash_bytes(&proof.block_header);
        self.pool
            .hset::<(), &str, (&[u8], Vec<u8>)>(
                PROOF_STORE_KEY_PREFIX,
                (&block_hash,
                bincode::serialize(proof)?),
            )
            .await?;
        self.pool
            .hset::<(), &str, (&[u8], Vec<u8>)>(
                PROOF_STORE_KEY_PREFIX,
                (&proof.scrypt_hash,
                bincode::serialize(proof)?),
            )
            .await?;;
        Ok(())
    }
}

#[async_trait]
impl CheckpointDrainQueueEmitterAsyncImm for ProofStoreFred {
    async fn cdq_push_imm<T: DQSerializable>(&self, item: T) -> anyhow::Result<()> {
        let metadata: DrainQueueMetadata = item.get_dq_metadata();
        let bytes = item.to_bytes()?;
        self.pool
            .lpush::<(), String, &[u8]>(
                format!(
                    "{}-{}_{}",
                    PS_DRAIN_QUEUE_KEY_PREFIX, metadata.channel_id, metadata.checkpoint_id
                ),
                &bytes,
            )
            .await?;

        Ok(())
    }
}

#[async_trait]
impl CheckpointDrainQueueConsumerAsyncImm for ProofStoreFred {
    async fn cdq_drain_imm<T: DQSerializable>(
        &self,
        channel_id: u64,
        checkpoint_id: u64,
    ) -> anyhow::Result<Vec<T>> {
        let key = format!(
            "{}-{}_{}",
            PS_DRAIN_QUEUE_KEY_PREFIX, channel_id, checkpoint_id
        );
        let members: Vec<Vec<u8>> = self
            .pool
            .lrange::<Vec<Vec<u8>>, String>(key.clone(), 0, -1)
            .await?;
        self.pool.del::<(), String>(key).await?;

        members.into_iter().map(|x| T::from_bytes(&x)).collect()
    }
}

#[async_trait]
impl BlockProcessorWorkerEventReceiverAsyncImm for ProofStoreFred {
    async fn wait_for_next_block_header(&self) -> anyhow::Result<BlockHeaderQueueItem> {
        loop {
            let job_res = self
                .pool
                .rpop::<Vec<u8>, _>(&self.block_processor_queue_id, None)
                .await;
            match job_res {
                Ok(g) => {
                    //println!("got it: {:?}",g);
                    if g.len() > 6 {
                        match bincode::deserialize::<BlockHeaderQueueItem>(&g) {
                            Ok(x) => return Ok(x),
                            Err(_) => {
                                // no-oop
                            }
                        }
                    }
                }
                Err(e) => println!("error: {:?}", e),
            };
            sleep(Duration::from_millis(1000)).await;
        }
    }
    async fn enqueue_block_headers_imm(&self, jobs: &[BlockHeaderQueueItem]) -> anyhow::Result<()> {
        self.pool
            .lpush::<(), _, Vec<Vec<u8>>>(
                &self.block_processor_queue_id,
                jobs.iter()
                    .map(|x| bincode::serialize(x).map_err(|e| anyhow::anyhow!("{:?}", e)))
                    .collect::<anyhow::Result<_>>()?,
            )
            .await?;

        Ok(())
    }
}
#[async_trait]
impl IBCBlockStateStoreReaderAsync for ProofStoreFred {
    async fn get_state_for_block(&self, block_height: u64) -> anyhow::Result<IBCBlockState> {
        let key = format!(
            "{}-{}",
            PS_IBC_STATE_STORE_PREFIX, self.ibc_state_store_suffix
        );
        let data = self
            .pool
            .hget::<Vec<u8>, _, &[u8]>(key, &block_height.to_be_bytes())
            .await?;

        println!("data: {:?}",data);

        IBCBlockState::from_bytes(&data)
    }
    async fn get_latest_state(&self) -> anyhow::Result<IBCBlockState> {
        let key = format!(
            "{}-{}",
            PS_IBC_STATE_STORE_LATEST_PREFIX, self.ibc_state_store_suffix
        );
        let data = self.pool.hget::<Vec<u8>, _, &[u8]>(key, &0u64.to_be_bytes()).await?;
        IBCBlockState::from_bytes(&data)
    }
    async fn get_latest_state_if_exists(&self) -> anyhow::Result<Option<IBCBlockState>> {
        let key = format!(
            "{}-{}",
            PS_IBC_STATE_STORE_LATEST_PREFIX, self.ibc_state_store_suffix
        );
        let data = self.pool.hget::<Vec<u8>, _, &[u8]>(key, &0u64.to_be_bytes()).await?;
        if data.len() == 0 {
            return Ok(None);
        }
        Ok(Some(IBCBlockState::from_bytes(&data)?))
    }
}

/*

    async fn get_bytes_by_id(&self, id: QProvingJobDataID) -> anyhow::Result<Vec<u8>> {
        let data = self
            .pool
            .hget::<Vec<u8>, _, &[u8]>(PROOF_STORE_KEY_PREFIX, &id.to_fixed_bytes())
            .await?;

        Ok(data)
    }
}

#[async_trait]
impl QProofStoreWriterAsyncImm for ProofStoreFred {
    async fn set_proof_by_id<C: GenericConfig<D>, const D: usize>(
        &self,
        id: QProvingJobDataID,
        proof: &ProofWithPublicInputs<C::F, C, D>,
    ) -> anyhow::Result<()> {
        self.pool
            .hsetnx::<(), _, &[u8], Vec<u8>>(
                PROOF_STORE_KEY_PREFIX,
                &id.to_fixed_bytes(),
                bincode::serialize(&proof)?,
            )
            .await?;
        Ok(())
    }
    
*/

#[async_trait]
impl IBCBlockStateStoreWriterAsyncImm for ProofStoreFred {
    async fn injest_ibc_block_state_imm(&self, state: &IBCBlockState) -> anyhow::Result<()> {
        let block_height = state.chain_state.get_tip_block_number() as u64;
        println!("saved state for block_height: {}",block_height);

        let key = format!(
            "{}-{}",
            PS_IBC_STATE_STORE_PREFIX, self.ibc_state_store_suffix
        );
        let latest_key = format!(
            "{}-{}",
            PS_IBC_STATE_STORE_LATEST_PREFIX, self.ibc_state_store_suffix
        );
        let bytes = state.to_bytes()?;
        self.pool
            .hset::<(), &str, (&[u8], Vec<u8>)>(&key, (&block_height.to_be_bytes(), bytes.clone()))
            .await?;
        self.pool
            .hset::<(), &str, (&[u8], Vec<u8>)>(&latest_key, (&0u64.to_be_bytes(), bytes))
            .await?;


        Ok(())
    }
}
#[async_trait]
impl BlockProcessorWorkerEventTransmitterAsyncImm for ProofStoreFred {
    async fn enqueue_block_headers_imm(&self, jobs: &[BlockHeaderQueueItem]) -> anyhow::Result<()> {
        self.pool
            .lpush::<(), _, Vec<Vec<u8>>>(
                &self.block_processor_queue_id,
                jobs.iter()
                    .map(|x| bincode::serialize(x).map_err(|e| anyhow::anyhow!("{:?}",e)))
                    .collect::<anyhow::Result<_>>()?,
            )
            .await?;

        Ok(())
    }
}


#[async_trait]
impl WorkerEventReceiverAsyncImm for ProofStoreFred {
    async fn wait_for_next_job_imm(&self) -> anyhow::Result<QProvingJobDataID> {
        loop {
            let job_res = self
                .pool
                .rpop::<[u8; 24], _>(&self.worker_queue_id, None)
                .await;
            match job_res {
                Ok(g) => {
                    return Ok(QProvingJobDataID::try_from_byte_vec(&g)?);
                }
                Err(e) => println!("error: {:?}", e),
            };
            sleep(Duration::from_millis(100)).await;
        }
    }
    async fn enqueue_jobs_imm(&self, jobs: &[QProvingJobDataID]) -> anyhow::Result<()> {
        self.pool
            .lpush::<(), _, Vec<Vec<u8>>>(
                &self.worker_queue_id,
                jobs.iter().map(|x| x.to_fixed_bytes().to_vec()).collect(),
            )
            .await?;

        Ok(())
    }
    async fn notify_core_goal_completed_imm(&self, job: QProvingJobDataID) -> anyhow::Result<()> {
        self.pool
            .lpush::<(), _, &[u8]>(&self.notifications_queue_id, &job.to_fixed_bytes())
            .await?;

        Ok(())
    }
    /*
    fn wait_for_next_job_mut(&mut self) -> anyhow::Result<QProvingJobDataID> {
        loop {
            let job = self.job_queue.pop_one(Q_JOB)?;
            if job.is_some() {
                return Ok(serde_json::from_slice(&job.unwrap())?)
            }else{
                std::thread::sleep(Duration::from_millis(250));
                continue;
            }
        }
    }

    fn enqueue_jobs_mut(&mut self, jobs: &[QProvingJobDataID]) -> anyhow::Result<()> {
        for job in jobs {
            self.job_queue.dispatch(Q_JOB, job.clone())?;
        }
        Ok(())
    }

    fn notify_core_goal_completed_mut(&mut self, _job: QProvingJobDataID) -> anyhow::Result<()> {
        self.job_queue.dispatch(Q_NOTIFICATIONS, QueueNotification::CoreJobCompleted)?;
        Ok(())
    }

    fn record_job_bench_mut(&mut self, job: QProvingJobDataID, duration: u64) -> anyhow::Result<()> {
        if self.benckmarks_enabled {
            self.benchmarks.push(QWorkerJobBenchmark {
                job_id: job.to_fixed_bytes(),
                duration,
            });
        }
        Ok(())
    }*/
}

#[async_trait]
impl WorkerEventTransmitterAsyncImm for ProofStoreFred {
    async fn enqueue_jobs_imm(&self, jobs: &[QProvingJobDataID]) -> anyhow::Result<()> {
        self.pool
            .lpush::<(), _, Vec<Vec<u8>>>(
                &self.worker_queue_id,
                jobs.iter().map(|x| x.to_fixed_bytes().to_vec()).collect(),
            )
            .await?;

        Ok(())
    }
    async fn wait_for_block_proving_jobs_imm(&self, _checkpoint_id: u64) -> anyhow::Result<bool> {
        loop {
            let job_res = self
                .pool
                .rpop::<Vec<u8>, _>(&self.notifications_queue_id, None)
                .await;
            match job_res {
                Ok(g) => {
                    //println!("got it1!");
                    if g.len() == 24 {
                        match QProvingJobDataID::try_from_byte_vec(&g) {
                            Ok(job) => {
                                println!("got it!");
                                if job.is_notify_orchestrator_complete() {
                                    println!("doneee!");
                                    return Ok(true)
                                }
                            },
                            Err(e1) => println!("error deserializing job id in wait_for_block_proving_jobs_imm: {:?}", e1),
                        }
                    }
                }
                Err(e2) => println!(
                    "error deserializing job id in wait_for_block_proving_jobs_imm: {:?}",
                    e2
                ),
            };
            sleep(Duration::from_millis(500)).await;
        }
    }
    /*
    fn enqueue_jobs_mut(&mut self, jobs: &[QProvingJobDataID]) -> anyhow::Result<()> {
        for job in jobs {
            self.job_queue.dispatch(Q_JOB, job.clone())?;
        }
        Ok(())
    }

    fn wait_for_block_proving_jobs_mut(&mut self, _checkpoint_id: u64) -> anyhow::Result<bool> {
        loop {
            match self
                .job_queue
                .pop_one(Q_NOTIFICATIONS)?
                .map(|v| serde_json::from_slice::<QueueNotification>(&v))
            {
                Some(Ok(QueueNotification::CoreJobCompleted)) => return Ok::<_, anyhow::Error>(true),
                Some(Err(_)) | None => {
                    std::thread::sleep(Duration::from_millis(500));
                    continue;
                }
            }
        }
    }*/
}

#[async_trait]
impl ScryptProverWorkerEventReceiverAsyncImm for ProofStoreFred {
    async fn wait_for_next_job_imm(&self) -> anyhow::Result<DogeBlockScryptProofInput> {
        loop {
            let job_res = self
                .pool
                .rpop::<Vec<u8>, _>(&self.scrypt_worker_queue_id, None)
                .await;
            match job_res {
                Ok(g) => {
                    if g.len() == 80 {
                        return Ok(DogeBlockScryptProofInput {
                            block_header: g.try_into().unwrap(),
                        });
                    } else {
                        // wrong size
                    }
                }
                Err(e) => println!("error: {:?}", e),
            };
            sleep(Duration::from_millis(100)).await;
        }
    }
    async fn enqueue_jobs_imm_scrypt(&self, jobs: &[DogeBlockScryptProofInput]) -> anyhow::Result<()> {
        self.pool
            .lpush::<(), _, Vec<Vec<u8>>>(
                &self.scrypt_worker_queue_id,
                jobs.iter().map(|x| x.to_fixed_bytes().to_vec()).collect(),
            )
            .await?;

        Ok(())
    }
    async fn notify_block_hash_completed(
        &self,
        proof_output: DogeBlockScryptProofOutput,
    ) -> anyhow::Result<()> {
        let key = format!(
            "{}#scrypt#{}",
            self.notifications_queue_id,
            Hash256(proof_output.get_block_hash()).to_hex_string()
        );
        println!("notify_block_hash_completed: key: {:?}",&key);
        self.pool
            .lpush::<(), _, &[u8]>(
                &key,
                &proof_output.to_bytes()?,
            )
            .await?;

        Ok(())
    }
}

#[async_trait]
impl ScryptProverWorkerEventTransmitterAsyncImm for ProofStoreFred {
    async fn enqueue_jobs_imm_scrypt(&self, jobs: &[DogeBlockScryptProofInput]) -> anyhow::Result<()> {
        self.pool
            .lpush::<(), _, Vec<Vec<u8>>>(
                &self.scrypt_worker_queue_id,
                jobs.iter().map(|x| x.to_fixed_bytes().to_vec()).collect(),
            )
            .await?;

        Ok(())
    }
    async fn wait_for_block_hash_proof(
        &self,
        block_hash: Hash256,
    ) -> anyhow::Result<DogeBlockScryptProofOutput> {
        let queue_key = format!(
            "{}#scrypt#{}",
            self.notifications_queue_id,
            block_hash.to_hex_string()
        );
        println!("waiting for block_hash_queue_key: {}", &queue_key);
        loop {
            let job_res = self.pool.rpop::<Vec<u8>, _>(&queue_key, None).await;
            match job_res {
                Ok(g) => {
                    //println!("got it scr: {:?}",g);
                    if g.len() > 1 {
                        match DogeBlockScryptProofOutput::from_bytes(&g) {
                            Ok(job) => {
                                //println!("got it!");
                                return Ok(job)
                            },
                            Err(e1) => println!("error deserializing job id in wait_for_block_proving_jobs_imm: {:?}", e1),
                        }
                    }
                }
                Err(e2) => println!(
                    "error deserializing job id in wait_for_block_proving_jobs_imm: {:?}",
                    e2
                ),
            };
            sleep(Duration::from_millis(500)).await;
        }
    }
    /*
    fn enqueue_jobs_mut(&mut self, jobs: &[QProvingJobDataID]) -> anyhow::Result<()> {
        for job in jobs {
            self.job_queue.dispatch(Q_JOB, job.clone())?;
        }
        Ok(())
    }

    fn wait_for_block_proving_jobs_mut(&mut self, _checkpoint_id: u64) -> anyhow::Result<bool> {
        loop {
            match self
                .job_queue
                .pop_one(Q_NOTIFICATIONS)?
                .map(|v| serde_json::from_slice::<QueueNotification>(&v))
            {
                Some(Ok(QueueNotification::CoreJobCompleted)) => return Ok::<_, anyhow::Error>(true),
                Some(Err(_)) | None => {
                    std::thread::sleep(Duration::from_millis(500));
                    continue;
                }
            }
        }
    }*/
}

#[async_trait]
impl CheckpointHistoryQueueEmitterAsyncImm for ProofStoreFred {
    async fn chq_push_imm<T: HQSerializable>(&self, item: T) -> anyhow::Result<()> {
        let metadata = item.get_hq_metadata();
        let bytes = item.to_bytes()?;
        self.pool
            .set::<(), String, &[u8]>(
                format!(
                    "{}-{}_{}",
                    PS_HISTORY_QUEUE_KEY_PREFIX, metadata.channel_id, metadata.checkpoint_id,
                ),
                &bytes,
                None,
                None,
                false,
            )
            .await?;
        self.pool
            .set::<(), String, u64>(
                format!("{}-{}", PS_HISTORY_QUEUE_KEY_PREFIX, metadata.channel_id,),
                metadata.checkpoint_id,
                None,
                None,
                false,
            )
            .await?;

        Ok(())
    }
}

#[async_trait]
impl CheckpointHistoryQueueConsumerAsyncImm for ProofStoreFred {
    async fn chq_listen_from_imm<T: HQSerializable>(
        &self,
        channel_id: u64,
        start_checkpoint_id: u64,
    ) -> anyhow::Result<Vec<T>> {
        let cur_checkpoint_id = self
            .pool
            .get::<Option<u64>, String>(format!("{}-{}", PS_HISTORY_QUEUE_KEY_PREFIX, channel_id,))
            .await?;
        match cur_checkpoint_id {
            Some(r) => {
                if r >= start_checkpoint_id {
                    let mut results = Vec::with_capacity((r - start_checkpoint_id + 1) as usize);

                    for i in start_checkpoint_id..=r {
                        let result = self
                            .pool
                            .get::<Vec<u8>, String>(format!(
                                "{}-{}_{}",
                                PS_HISTORY_QUEUE_KEY_PREFIX, channel_id, i,
                            ))
                            .await?;
                        results.push(T::from_bytes(&result)?);
                    }

                    Ok(results)
                } else {
                    Ok(Vec::new())
                }
            }
            None => Ok(Vec::new()),
        }
    }
    async fn wait_for_next_item_imm<T: HQSerializable>(
        &self,
        channel_id: u64,
        start_checkpoint_id: u64,
    ) -> anyhow::Result<T> {
        let cur_checkpoint_id = self
            .pool
            .get::<Option<u64>, String>(format!("{}-{}", PS_HISTORY_QUEUE_KEY_PREFIX, channel_id,))
            .await?;
        let mut checkpoint_current: i64 = match cur_checkpoint_id {
            Some(x) => x as i64,
            None => -1,
        };

        let start_i64 = start_checkpoint_id as i64;
        while checkpoint_current < start_i64 {
            sleep(Duration::from_millis(100)).await;
            let cur_checkpoint_id = self
                .pool
                .get::<Option<u64>, String>(format!(
                    "{}-{}",
                    PS_HISTORY_QUEUE_KEY_PREFIX, channel_id,
                ))
                .await?;
            checkpoint_current = match cur_checkpoint_id {
                Some(x) => x as i64,
                None => -1,
            };
        }
        let result = self
            .pool
            .get::<Vec<u8>, String>(format!(
                "{}-{}_{}",
                PS_HISTORY_QUEUE_KEY_PREFIX, channel_id, checkpoint_current,
            ))
            .await?;
        Ok(T::from_bytes(&result)?)
    }
}
