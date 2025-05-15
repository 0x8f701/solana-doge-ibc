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

use async_trait::async_trait;
use borsh::{BorshDeserialize, BorshSerialize};
use doge_light_client::core_data::QDogeBlockHeader;
use serde::{Deserialize, Serialize};


use crate::data::{base_types::hash256::Hash256, scrypt_proof::{DogeBlockScryptProofInput, DogeBlockScryptProofOutput}};


#[derive(Clone, Debug, PartialEq, Default, Eq, Ord, PartialOrd, Serialize, Deserialize, BorshDeserialize, BorshSerialize)]
pub struct BlockHeaderQueueItemBlockHeader {
    pub block_number: u32,
    pub block_header: QDogeBlockHeader,
}
#[derive(Clone, Debug, PartialEq, Default, Eq, Ord, PartialOrd, Serialize, Deserialize, BorshDeserialize, BorshSerialize)]
pub struct BlockHeaderQueueItemRevertFork {
    pub last_good_block_number: u32,
    pub headers: Vec<QDogeBlockHeader>,
}
#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, BorshDeserialize, BorshSerialize)]
pub enum BlockHeaderQueueItem {
    BlockHeader(BlockHeaderQueueItemBlockHeader),
    RevertFork(BlockHeaderQueueItemRevertFork),
}





pub trait QScryptProofStoreReaderSync {
    fn get_scrypt_proof_by_scrypt_hash(
        &self,
        scrypt_hash: Hash256,
    ) -> anyhow::Result<DogeBlockScryptProofOutput>;
    fn get_scrypt_proof_by_block_header(
        &self,
        block_header: [u8; 80],
    ) -> anyhow::Result<DogeBlockScryptProofOutput>;
    fn get_scrypt_proof_by_block_hash(
        &self,
        block_hash: Hash256,
    ) -> anyhow::Result<DogeBlockScryptProofOutput>;
    fn contains_scrypt_proof_for_block_hash(
        &self,
        block_hash: Hash256,
    ) -> anyhow::Result<bool>;
}

pub trait QScryptProofStoreWriterSync {
    fn injest_scrypt_proof_result_mut(
        &mut self,
        proof: &DogeBlockScryptProofOutput,
    ) -> anyhow::Result<()>;
}

pub trait QScryptProofStoreWriterSyncImm {
    fn injest_scrypt_proof_result_imm(
        &self,
        proof: &DogeBlockScryptProofOutput,
    ) -> anyhow::Result<()>;
}



pub trait QScryptProofStore: QScryptProofStoreReaderSync + QScryptProofStoreWriterSync {

}

impl<T: QScryptProofStoreReaderSync + QScryptProofStoreWriterSync> QScryptProofStore for T {}

#[async_trait]
pub trait QScryptProofStoreReaderAsync {
    async fn get_scrypt_proof_by_scrypt_hash(
        &self,
        scrypt_hash: Hash256,
    ) -> anyhow::Result<DogeBlockScryptProofOutput>;
    async fn get_scrypt_proof_by_block_header(
        &self,
        block_header: [u8; 80],
    ) -> anyhow::Result<DogeBlockScryptProofOutput>;
    async fn get_scrypt_proof_by_block_hash(
        &self,
        block_hash: Hash256,
    ) -> anyhow::Result<DogeBlockScryptProofOutput>;
    async fn contains_scrypt_proof_for_block_hash(
        &self,
        block_hash: Hash256,
    ) -> anyhow::Result<bool>;
}

#[async_trait]
pub trait QScryptProofStoreWriterAsyncImm {
    async fn injest_scrypt_proof_result_imm(
        &self,
        proof: &DogeBlockScryptProofOutput,
    ) -> anyhow::Result<()>;
}


pub trait QScryptProofStoreAsyncImm: QScryptProofStoreReaderAsync + QScryptProofStoreWriterAsyncImm {

}

impl<T: QScryptProofStoreReaderAsync + QScryptProofStoreWriterAsyncImm> QScryptProofStoreAsyncImm for T {}


#[derive(Clone, Copy, Debug)]
pub struct QDummyProofStore {}

impl QDummyProofStore {
    pub fn new() -> Self {
        Self {}
    }
}

impl QScryptProofStoreReaderSync for QDummyProofStore {
    fn get_scrypt_proof_by_scrypt_hash(
        &self,
        _scrypt_hash: Hash256,
    ) -> anyhow::Result<DogeBlockScryptProofOutput> {
        todo!()
    }

    fn get_scrypt_proof_by_block_header(
        &self,
        _block_header: [u8; 80],
    ) -> anyhow::Result<DogeBlockScryptProofOutput> {
        todo!()
    }

    fn get_scrypt_proof_by_block_hash(
        &self,
        _block_hash: Hash256,
    ) -> anyhow::Result<DogeBlockScryptProofOutput> {
        todo!()
    }
    
    fn contains_scrypt_proof_for_block_hash(
        &self,
        block_hash: Hash256,
    ) -> anyhow::Result<bool> {
        todo!()
    }
}
impl QScryptProofStoreWriterSync for QDummyProofStore {
    fn injest_scrypt_proof_result_mut(
        &mut self,
        _proof: &DogeBlockScryptProofOutput,
    ) -> anyhow::Result<()> {
        todo!()
    }
}


pub trait QScryptGenericProverSyncImm {
    fn scrypt_worker_prove_sync_imm(&self, input: &DogeBlockScryptProofInput) -> anyhow::Result<DogeBlockScryptProofOutput>;
}

pub trait QScryptGenericProverSyncMut {
    fn scrypt_worker_prove_sync_mut(&mut self, input: &DogeBlockScryptProofInput) -> anyhow::Result<DogeBlockScryptProofOutput>;
}


#[async_trait]
pub trait QScryptGenericProverAsyncImm {
    async fn scrypt_worker_prove_async_imm(&self, input: &DogeBlockScryptProofInput) -> anyhow::Result<DogeBlockScryptProofOutput>;
}
#[async_trait]
pub trait QScryptGenericProverAsyncMut {
    async fn scrypt_worker_prove_async_mut(&mut self, input: &DogeBlockScryptProofInput) -> anyhow::Result<DogeBlockScryptProofOutput>;
}




#[async_trait]
pub trait ScryptProverWorkerEventReceiverAsyncImm {
    async fn wait_for_next_job_imm(&self) -> anyhow::Result<DogeBlockScryptProofInput>;
    async fn enqueue_jobs_imm_scrypt(&self, jobs: &[DogeBlockScryptProofInput]) -> anyhow::Result<()>;
    async fn notify_block_hash_completed(&self, proof_output: DogeBlockScryptProofOutput) -> anyhow::Result<()>;
}


#[async_trait]
pub trait ScryptProverWorkerEventTransmitterAsyncImm {
    async fn enqueue_jobs_imm_scrypt(&self, jobs: &[DogeBlockScryptProofInput]) -> anyhow::Result<()>;
    async fn wait_for_block_hash_proof(&self, block_hash: Hash256) -> anyhow::Result<DogeBlockScryptProofOutput>;
}




#[async_trait]
pub trait BlockProcessorWorkerEventReceiverAsyncImm {
    async fn wait_for_next_block_header(&self) -> anyhow::Result<BlockHeaderQueueItem>;
    async fn enqueue_block_headers_imm(&self, jobs: &[BlockHeaderQueueItem]) -> anyhow::Result<()>;
}


#[async_trait]
pub trait BlockProcessorWorkerEventTransmitterAsyncImm {
    async fn enqueue_block_headers_imm(&self, jobs: &[BlockHeaderQueueItem]) -> anyhow::Result<()>;
}
