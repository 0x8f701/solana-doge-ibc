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
use borsh::BorshDeserialize;
use kvq::traits::KVQSerializable;

use crate::config::network_constants::QEDDogeChainState;

use super::scrypt_proof::DogeBlockScryptProofOutput;
use zerocopy::{IntoBytes, FromBytes};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct IBCBlockState {
    pub chain_state: QEDDogeChainState,
}


impl KVQSerializable for IBCBlockState {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(borsh::to_vec(&self.chain_state)?)
    }

    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let res = match QEDDogeChainState::try_from_slice(&bytes) {
            Ok(x) => x,
            Err(e) => anyhow::bail!("{:?}",e),
        };
        Ok(Self {
            chain_state: res,
        })

    }
}


pub trait IBCBlockStateStoreReaderSync {
    fn get_state_for_block_sync(&self, block_height: u64) -> anyhow::Result<IBCBlockState>;
    fn get_latest_state_sync(&self) -> anyhow::Result<IBCBlockState>;
    fn get_latest_state_sync_if_exists(&self) -> anyhow::Result<IBCBlockState>;

}

#[async_trait]
pub trait IBCBlockStateStoreReaderAsync {
    async fn get_state_for_block(&self, block_height: u64) -> anyhow::Result<IBCBlockState>;
    async fn get_latest_state(&self) -> anyhow::Result<IBCBlockState>;
    async fn get_latest_state_if_exists(&self) -> anyhow::Result<Option<IBCBlockState>>;
}

pub trait IBCBlockStateStoreWriterSyncImm {
    fn injest_ibc_block_state_sync_imm(&self, state: &IBCBlockState) -> anyhow::Result<()>;
}
pub trait IBCBlockStateStoreWriterSyncMut {
    fn injest_ibc_block_state_sync_mut(&mut self, state: &IBCBlockState) -> anyhow::Result<()>;
}


#[async_trait]
pub trait IBCBlockStateStoreWriterAsyncImm {
    async fn injest_ibc_block_state_imm(&self, state: &IBCBlockState) -> anyhow::Result<()>;
}

#[async_trait]
pub trait IBCBlockStateStoreWriterAsyncMut {
    async fn injest_ibc_block_state_mut(&mut self, state: &IBCBlockState) -> anyhow::Result<()>;
}

pub trait IBCBlockStateStoreAsyncImm: IBCBlockStateStoreReaderAsync + IBCBlockStateStoreWriterAsyncImm {}
pub trait IBCBlockStateStoreAsyncMut: IBCBlockStateStoreReaderAsync + IBCBlockStateStoreWriterAsyncMut {}
pub trait IBCBlockStateStoreSyncImm: IBCBlockStateStoreReaderSync + IBCBlockStateStoreWriterSyncImm {}
pub trait IBCBlockStateStoreSyncMut: IBCBlockStateStoreReaderSync + IBCBlockStateStoreWriterSyncMut {}

impl<T: IBCBlockStateStoreReaderAsync + IBCBlockStateStoreWriterAsyncImm> IBCBlockStateStoreAsyncImm for T {}
impl<T: IBCBlockStateStoreReaderAsync + IBCBlockStateStoreWriterAsyncMut> IBCBlockStateStoreAsyncMut for T {}
impl<T: IBCBlockStateStoreReaderSync + IBCBlockStateStoreWriterSyncImm> IBCBlockStateStoreSyncImm for T {}
impl<T: IBCBlockStateStoreReaderSync + IBCBlockStateStoreWriterSyncMut> IBCBlockStateStoreSyncMut for T {}



#[async_trait]
pub trait IBCBlockSubmitterAsync {
    async fn submit_block_result(&self, block_height: u64, state: &IBCBlockState, header_bytes: &[u8], proof_result: &DogeBlockScryptProofOutput) -> anyhow::Result<()>;
}