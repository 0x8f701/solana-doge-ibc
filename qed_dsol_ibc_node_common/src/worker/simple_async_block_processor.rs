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

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use crate::{doge_link_rpc_async::DogeLinkElectrsAsyncClient, sol_submitter::SolSubmitterClient};
use doge_light_client::{
    constants::DogeNetworkConfig, core_data::QDogeBlockHeader, init_params::InitBlockDataIBC,
};
use qed_dsol_bridge_core::{
    config::network_constants::{
        QEDDogeChainState, QDOGE_BRIDGE_BLOCK_HASH_CACHE_SIZE, QDOGE_BRIDGE_REQUIRED_CONFIRMATIONS,
    },
    data::{
        base_types::hash256::Hash256,
        scrypt_proof::DogeBlockScryptProofInput,
        state::{IBCBlockState, IBCBlockStateStoreAsyncImm, IBCBlockSubmitterAsync},
    },
    job::g16_scrypt_traits::{
        BlockHeaderQueueItem, BlockProcessorWorkerEventReceiverAsyncImm,
        QScryptProofStoreReaderAsync, ScryptProverWorkerEventTransmitterAsyncImm,
    },
    utils::debug_timer::DebugTimer,
};
use zerocopy::IntoBytes;

#[derive(Clone, Debug)]
pub struct SimpleAsyncBlockProcessor {
    pub state: IBCBlockState,
    pub sol_submitter_client: SolSubmitterClient,
    pub block_rpc: DogeLinkElectrsAsyncClient,
    pub timer: DebugTimer,
    pub seen_error_block_hashes: HashMap<[u8; 32], usize>,
}
impl SimpleAsyncBlockProcessor {
    pub fn inc_get_seen_error_block_hashes(&mut self, block_hash: [u8; 32]) -> usize {
        let count = self.seen_error_block_hashes.entry(block_hash).or_insert(0);
        *count += 1;
        *count
    }
    pub fn get_seen_error_block_hashes(&self, block_hash: [u8; 32]) -> usize {
        *self.seen_error_block_hashes.get(&block_hash).unwrap_or(&0)
    }
    pub async fn init_block_processor<BSS: IBCBlockStateStoreAsyncImm>(
        bss: &BSS,
        sol_submitter_client: SolSubmitterClient,
        block_rpc: DogeLinkElectrsAsyncClient,
    ) -> anyhow::Result<Self> {
        let mut timer = DebugTimer::new("block_processor");
        let mut latest_state = bss.get_latest_state_if_exists().await?;
        let sol_state = sol_submitter_client.get_ibc_program_state_full().await?;
        if latest_state.is_some() {
            timer.event(format!(
                "got latest redis state: [Tip Block Number: {}], [Tip Block Hash: {}]",
                latest_state
                    .as_ref()
                    .unwrap()
                    .chain_state
                    .get_tip_block_number(),
                hex::encode(
                    &latest_state
                        .as_ref()
                        .unwrap()
                        .chain_state
                        .get_tip_block_hash()
                )
            ));
        }
        if sol_state.is_some() {
            let mut sol_state = sol_state.unwrap();
            timer.event(format!(
                "got solana state: [Tip Block Number: {}], [Tip Block Hash: {}]",
                sol_state.get_tip_block_number(),
                hex::encode(&sol_state.get_tip_block_hash())
            ));
            if latest_state.is_none() {
                latest_state = Some(IBCBlockState {
                    chain_state: sol_state,
                });
            } else {
                let latest_block_number = latest_state
                    .as_ref()
                    .unwrap()
                    .chain_state
                    .get_tip_block_number();
                if latest_block_number != sol_state.get_tip_block_number() {
                    if latest_block_number < sol_state.get_tip_block_number() {
                        latest_state = Some(IBCBlockState {
                            chain_state: sol_state,
                        });
                    } else {
                        for _ in 0..10 {
                            timer.lap("sol state not sync'd with redis state, waiting for 5 seconds in case solana state to catches up");
                            tokio::time::sleep(Duration::from_millis(5_000)).await;
                            sol_state = sol_submitter_client
                                .get_ibc_program_state_full()
                                .await?
                                .unwrap();
                            if sol_state.get_tip_block_number() >= latest_block_number {
                                break;
                            }
                        }
                        latest_state = Some(IBCBlockState {
                            chain_state: sol_state,
                        });
                    }
                }
            }
        }else{
            timer.lap("sol state not initialized, waiting for 5 seconds in case solana state to catches up");
            tokio::time::sleep(Duration::from_millis(5_000)).await;
            let sol_state = sol_submitter_client.get_ibc_program_state_full().await?;
            if sol_state.is_some() {
                latest_state = Some(IBCBlockState {
                    chain_state: sol_state.unwrap(),
                });
            }else{
                timer.lap("solana state still not initialized, we have to do it instead");
                latest_state = None;
            }

        }

        let state = if !latest_state.is_some() {
            timer.lap("latest state doesn't exist, creating new state from RPC");
            let mut start_tip = block_rpc.get_block_height().await?;
            while start_tip < (QDOGE_BRIDGE_BLOCK_HASH_CACHE_SIZE as u32) {
                tokio::time::sleep(Duration::from_millis(500)).await;
                start_tip = block_rpc.get_block_height().await?;
            }
            timer.event(format!("got latest tip: {}", start_tip));
            let init_headers = block_rpc
                .get_qd_block_headers_range_parallel(
                    start_tip - (QDOGE_BRIDGE_BLOCK_HASH_CACHE_SIZE as u32 - 1),
                    start_tip,
                )
                .await?;

            timer.event(format!(
                "got init block headers with new tip hash: {}",
                hex::encode(&init_headers.last().unwrap().header.get_hash())
            ));
            let chain_state = QEDDogeChainState::from_init_data(
                &InitBlockDataIBC::new_from_block_headers_empty_tree(
                    &init_headers.try_into().unwrap(),
                    start_tip,
                ),
            );

            let init_chain_state_bytes = chain_state.as_bytes().to_vec();
            bss.injest_ibc_block_state_imm(&IBCBlockState {
                chain_state: chain_state.clone(),
            })
            .await?;
            timer.lap("sending init_chain_state_bytes to solana sender");
            sol_submitter_client
                .init_ibc_program_state(&init_chain_state_bytes)
                .await?;
            timer.lap("sent init_chain_state_bytes to solana sender");

            IBCBlockState { chain_state }
        } else {
            latest_state.unwrap()
        };
        bss.injest_ibc_block_state_imm(&state)
        .await?;
    //println!("state: {:?}", state);
        timer.event(format!(
            "starting block processor at tip: {} (block_hash = {})",
            state.chain_state.get_tip_block_number(),
            Hash256(state.chain_state.get_tip_block_hash()).to_reversed_hex_string()
        ));

        Ok(Self {
            state: state,
            sol_submitter_client,
            block_rpc,
            timer,
            seen_error_block_hashes: HashMap::new(),
        })
    }
    pub async fn run_worker<
        NC: DogeNetworkConfig,
        PS: QScryptProofStoreReaderAsync + Send + Sync,
        ER: ScryptProverWorkerEventTransmitterAsyncImm,
        BP: BlockProcessorWorkerEventReceiverAsyncImm,
        BSS: IBCBlockStateStoreAsyncImm,
        SUB: IBCBlockSubmitterAsync,
    >(
        &mut self,
        store: &PS,
        zk_transmitter: &ER,
        bp_receiver: &BP,
        bss: &BSS,
        submitter: &SUB,
    ) -> anyhow::Result<()> {
        loop {
            self.process_next_job::<NC, PS, ER, BP, BSS, SUB>(
                store,
                zk_transmitter,
                bp_receiver,
                bss,
                submitter,
            )
            .await?;
        }
    }
    pub async fn process_next_job<
        NC: DogeNetworkConfig,
        PS: QScryptProofStoreReaderAsync + Send + Sync,
        ER: ScryptProverWorkerEventTransmitterAsyncImm,
        BP: BlockProcessorWorkerEventReceiverAsyncImm,
        BSS: IBCBlockStateStoreAsyncImm,
        SUB: IBCBlockSubmitterAsync,
    >(
        &mut self,
        store: &PS,
        zk_transmitter: &ER,
        bp_receiver: &BP,
        bss: &BSS,
        submitter: &SUB,
    ) -> anyhow::Result<()> {
        self.timer.lap("waiting for next block header");
        let last_block_hash = self.state.chain_state.get_tip_block_hash();
        let last_block_number = self.state.chain_state.get_tip_block_number();
        let new_block_number = last_block_number + 1;
        let item = bp_receiver.wait_for_next_block_header().await?;
        let item_b = item.clone();
        let old_chain_state = self.state.chain_state.clone();
        match item {
            BlockHeaderQueueItem::BlockHeader(header) => {
                self.timer.event(format!(
                    "got incoming block header for block # {}",
                    header.block_number
                ));
                if header.block_number < new_block_number {
                    self.timer.event(format!("block # {} is less than the expected new block number ({}), ignoring block header", header.block_number,new_block_number));
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    return Ok(());
                }else if header.block_number > new_block_number {
                    self.timer.event(format!("block # {} is more than the expected new block number ({}), pushing it back on the queue", header.block_number,new_block_number));
                    bp_receiver.enqueue_block_headers_imm(&[item_b]).await?;
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    return Ok(());
                }
                let incoming_prev_block_hash = header.block_header.header.previous_block_hash;
                let incoming_block_hash = header.block_header.header.get_hash();
                if incoming_prev_block_hash != last_block_hash {
                    self.timer.event(format!("[incoming_block_hash: {}] incoming block header's previous_block_hash field ({}) does not match the tip block hash ({})", hex::encode(&incoming_block_hash),hex::encode(&incoming_prev_block_hash), hex::encode(&last_block_hash)));
                    let retry_count = self.inc_get_seen_error_block_hashes(incoming_block_hash);
                    if retry_count <= 3 {
                        self.state.chain_state = old_chain_state;
                        bss.injest_ibc_block_state_imm(&self.state).await?;
                        self.timer.event(format!(
                            "re-enqueing incoming block header with hash ({}) (retry {}/3)",
                            hex::encode(&incoming_block_hash),
                            retry_count
                        ));
                        bp_receiver.enqueue_block_headers_imm(&[item_b]).await?;
                        tokio::time::sleep(Duration::from_millis(750)).await;
                        return Ok(());
                    } else {
                        self.state.chain_state = old_chain_state;
                        self.timer.event(format!("block header with hash ({}) failed too many times, ({}/3 times) removing from queue and moving on",hex::encode(&incoming_block_hash),retry_count));
                        return Ok(());
                    }
                }

                match self
                    .process_next_job_header_internal::<NC, PS, ER, BP, BSS, SUB>(
                        store,
                        zk_transmitter,
                        bp_receiver,
                        bss,
                        submitter,
                        header.block_header,
                        header.block_number,
                    )
                    .await
                {
                    Ok(_) => {
                        self.timer
                            .event(format!("[SUCCESS] Processed block # {}", new_block_number));
                    }
                    Err(err) => {
                        self.state.chain_state = old_chain_state;
                        bss.injest_ibc_block_state_imm(&self.state).await?;
                        eprintln!("error processing next job: {:?}", err);
                        self.timer.event(format!("failed to process incoming block header with hash {} at tip block number {} with error {:?}", hex::encode(&incoming_block_hash),new_block_number, err));
                        let retry_count = self.inc_get_seen_error_block_hashes(incoming_block_hash);
                        if retry_count <= 10 {
                            self.timer.event(format!(
                                "re-enqueing incoming block header with hash ({}) (retry {}/10)",
                                hex::encode(&incoming_block_hash),
                                retry_count
                            ));
                            bp_receiver.enqueue_block_headers_imm(&[item_b]).await?;
                            tokio::time::sleep(Duration::from_millis(250)).await;
                            return Ok(());
                        } else {
                            self.timer.event(format!("block header with hash ({}) failed too many times, ({}/10 times) removing from queue and moving on",hex::encode(&incoming_block_hash),retry_count));
                            return Ok(());
                        }
                    }
                }
            }
            BlockHeaderQueueItem::RevertFork(revert) => {
                if last_block_number < revert.last_good_block_number
                    || last_block_number == revert.last_good_block_number
                {
                    self.timer.event(format!("revert block # {} is less than or equal to the last good block number ({}), ignoring revert", revert.last_good_block_number, last_block_number));
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    return Ok(());
                } else if last_block_number - revert.last_good_block_number
                    >= (QDOGE_BRIDGE_REQUIRED_CONFIRMATIONS as u32)
                {
                    self.timer.event(format!("revert block # {} is more than or equal to the required confirmations ({}), ignoring revert (THIS COULD BE REALLY BAD)", revert.last_good_block_number, QDOGE_BRIDGE_REQUIRED_CONFIRMATIONS));
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    return Ok(());
                } else {
                    let revert_backpedal_length = last_block_number - revert.last_good_block_number;
                    let revert_forward_length = revert.headers.len() as u32;
                    if revert_backpedal_length <= revert_forward_length {
                        self.timer.event(format!("revert block # {} is less eq the length of the revert headers ({}), ignoring revert", revert.last_good_block_number, revert_forward_length));
                        tokio::time::sleep(Duration::from_millis(150)).await;
                        return Ok(());
                    } else {
                        self.timer.event(format!("revert block # {} is more than the length of the revert headers ({}), reverting back to block # {}", revert.last_good_block_number, revert_forward_length, revert.last_good_block_number));
                        match self
                            .process_next_job_revert_internal::<NC, PS, ER, BP, BSS, SUB>(
                                store,
                                zk_transmitter,
                                bp_receiver,
                                bss,
                                submitter,
                                &revert.headers,
                                revert.last_good_block_number,
                            )
                            .await
                        {
                            Ok(_) => {
                                self.timer.event(format!(
                                    "[SUCCESS] Processed revert block # {}",
                                    revert.last_good_block_number
                                ));
                            }
                            Err(err) => {
                                eprintln!("error processing next job: {:?}", err);
                                self.timer.event(format!(
                                    "failed to process incoming revert block # {} with error {:?}",
                                    revert.last_good_block_number, err
                                ));
                                let retry_count = self.inc_get_seen_error_block_hashes(
                                    revert.headers.last().unwrap().header.get_hash(),
                                );
                                if retry_count <= 10 {
                                    self.timer.event(format!(
                                        "re-enqueing incoming revert block # {} (retry {}/10)",
                                        revert.last_good_block_number, retry_count
                                    ));
                                    bp_receiver.enqueue_block_headers_imm(&[item_b]).await?;
                                    tokio::time::sleep(Duration::from_millis(250)).await;
                                    return Ok(());
                                } else {
                                    self.timer.event(format!("revert block # {} failed too many times, ({}/10 times) removing from queue and moving on",revert.last_good_block_number, retry_count));
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
    async fn process_next_job_header_internal<
        NC: DogeNetworkConfig,
        PS: QScryptProofStoreReaderAsync + Send + Sync,
        ER: ScryptProverWorkerEventTransmitterAsyncImm,
        BP: BlockProcessorWorkerEventReceiverAsyncImm,
        BSS: IBCBlockStateStoreAsyncImm,
        SUB: IBCBlockSubmitterAsync,
    >(
        &mut self,
        store: &PS,
        zk_transmitter: &ER,
        bp_receiver: &BP,
        bss: &BSS,
        submitter: &SUB,
        header: QDogeBlockHeader,
        block_number: u32,
    ) -> anyhow::Result<()> {
        //let mut timer = TraceTimer::new("process_next_job");

        let last_block_hash = self.state.chain_state.get_tip_block_hash();
        let last_block_number = self.state.chain_state.get_tip_block_number();
        let new_block_number = last_block_number + 1;
        if header.header.is_aux_pow() != header.aux_pow.is_some() {
            anyhow::bail!("header.is_aux_pow() != header.aux_pow.is_some()");
        }
        if block_number != new_block_number {
            anyhow::bail!(
                "block number ({}) does not match the expected new block number ({})",
                block_number,
                new_block_number
            );
        }

        self.timer.event(format!(
            "got incoming block header for block # {}",
            new_block_number
        ));

        let incoming_previous_block_hash = header.header.previous_block_hash;

        if incoming_previous_block_hash != last_block_hash {
            anyhow::bail!("incoming block header's previous_block_hash field ({}) does not match the tip block hash ({})", hex::encode(&incoming_previous_block_hash), hex::encode(&last_block_hash));
        }

        match self
            .state
            .chain_state
            .append_block::<NC>(new_block_number, &header, None)
        {
            Ok(_) => {
                self.timer.event(format!(
                    "appended block # {} with hash {} to chain state",
                    new_block_number,
                    Hash256(header.header.get_hash()).to_reversed_hex_string()
                ));
            }
            Err(e) => {
                anyhow::bail!("error appending block to chain state: {}", e);
            }
        }
        self.timer
            .event(format!("appended block # {}", new_block_number));

        //println!("header: {:?}", header);

        // end validity stuff, TODO: retry the stuff below infinite times
        let pow_block_header = if header.aux_pow.is_some() {
            header.aux_pow.as_ref().unwrap().parent_block
        } else {
            header.header
        };

        let scrypt_block_hash = Hash256(pow_block_header.get_hash());

        let proof_output = if store
            .contains_scrypt_proof_for_block_hash(scrypt_block_hash)
            .await?
        {
            store
                .get_scrypt_proof_by_block_hash(scrypt_block_hash)
                .await?
        } else {
            zk_transmitter
                .enqueue_jobs_imm_scrypt(&[DogeBlockScryptProofInput {
                    block_header: pow_block_header.to_bytes_fixed(),
                }])
                .await?;
            zk_transmitter
                .wait_for_block_hash_proof(scrypt_block_hash)
                .await?
        };
        self.timer
            .event(format!("got scrypt proof for block # {}", new_block_number));

        submitter
            .submit_block_result(
                self.state.chain_state.get_tip_block_number() as u64,
                &self.state,
                &borsh::to_vec(&header)?,
                &proof_output,
            )
            .await?;
        bss.injest_ibc_block_state_imm(&self.state).await?;
        self.timer
            .event(format!("🎉 Submitted block # {} with hash {} to Solana!", new_block_number,
            Hash256(header.header.get_hash()).to_reversed_hex_string()));
        Ok(())
    }

    async fn process_next_job_revert_internal<
        NC: DogeNetworkConfig,
        PS: QScryptProofStoreReaderAsync + Send + Sync,
        ER: ScryptProverWorkerEventTransmitterAsyncImm,
        BP: BlockProcessorWorkerEventReceiverAsyncImm,
        BSS: IBCBlockStateStoreAsyncImm,
        SUB: IBCBlockSubmitterAsync,
    >(
        &mut self,
        store: &PS,
        zk_transmitter: &ER,
        bp_receiver: &BP,
        bss: &BSS,
        submitter: &SUB,
        new_block_headers: &[QDogeBlockHeader],
        last_good_block_number: u32,
    ) -> anyhow::Result<()> {
        //let mut timer = TraceTimer::new("process_next_job");

        todo!("implement revert");
        Ok(())
    }
}
