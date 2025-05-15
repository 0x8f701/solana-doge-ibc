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

use std::
    time::Duration
;

use crate::doge_link_rpc_async::DogeLinkElectrsAsyncClient;
use doge_light_client::{
    constants::DogeNetworkConfig, core_data::QDogeBlockHeader,
};
use qed_dsol_bridge_core::{
    config::network_constants::
        QDOGE_BRIDGE_REQUIRED_CONFIRMATIONS
    ,
    data::{
        base_types::hash256::Hash256,
        state::{IBCBlockState, IBCBlockStateStoreAsyncImm},
    },
    job::g16_scrypt_traits::{
        BlockHeaderQueueItem, BlockHeaderQueueItemBlockHeader, BlockProcessorWorkerEventTransmitterAsyncImm
    },
    utils::debug_timer::DebugTimer,
};

#[derive(Clone, Debug)]
pub struct SimpleRPCBlockNotifier {
    pub block_rpc: DogeLinkElectrsAsyncClient,
    pub timer: DebugTimer,
    pub state: IBCBlockState,
}
impl SimpleRPCBlockNotifier {
    pub async fn init_block_notifier<BSS: IBCBlockStateStoreAsyncImm>(
        bss: &BSS,
        block_rpc: DogeLinkElectrsAsyncClient,
    ) -> anyhow::Result<Self> {
        let mut timer = DebugTimer::new("block_notifier");
        let mut latest_state = bss.get_latest_state_if_exists().await?;
        //timer.event(format!("got init latest state: {:?}", latest_state));
        let first_is_none = latest_state.is_none();

        while (&latest_state).is_none() {
            latest_state = bss.get_latest_state_if_exists().await?;
            tokio::time::sleep(Duration::from_millis(750)).await;
        }
        //timer.event(format!("got good latest state: {:?}", latest_state));

        let state = if first_is_none {
            latest_state.unwrap()
        } else {
            let mut old_latest_state = latest_state.unwrap();
            let mut has_update = true;
            while (has_update) {
                timer.lap("sleeping for 2 minutes to detect any changes in the chain state");
                tokio::time::sleep(Duration::from_millis(1_000)).await;
                let latest = bss.get_latest_state().await?;
                if latest.chain_state.eq(&old_latest_state.chain_state) {
                    has_update = false;
                    break;
                } else {
                    old_latest_state = latest;
                }
            }
            timer.lap("detected an empty queue, init block notifier with latest state");
            old_latest_state
        };
        timer.event(format!(
            "init notifier at tip: {} (block_hash = {})",
            state.chain_state.get_tip_block_number(),
            Hash256(state.chain_state.get_tip_block_hash()).to_reversed_hex_string()
        ));

        Ok(Self {
            state,
            block_rpc,
            timer,
        })
    }
    pub async fn run_worker<
        NC: DogeNetworkConfig,
        BP: BlockProcessorWorkerEventTransmitterAsyncImm,
        BSS: IBCBlockStateStoreAsyncImm,
    >(
        &mut self,
        bp_transmitter: &BP,
        bss: &BSS,
    ) -> anyhow::Result<()> {
        loop {
            match self.process_next_job::<NC, BP, BSS>(bp_transmitter, bss).await {
                Ok(_) => {},
                Err(err) => {
                    self.timer.event(format!("error processing next job, sleeping for 1 second: {:?}", err));
                },
            }
            tokio::time::sleep(Duration::from_millis(1_000)).await;
        }
    }
    pub async fn process_next_job<
        NC: DogeNetworkConfig,
        BP: BlockProcessorWorkerEventTransmitterAsyncImm,
        BSS: IBCBlockStateStoreAsyncImm,
    >(
        &mut self,
        bp_transmitter: &BP,
        bss: &BSS,
    ) -> anyhow::Result<()> {
        self.timer.lap("at for next block header");
        let last_block_hash = self.state.chain_state.get_tip_block_hash();
        let last_block_number = self.state.chain_state.get_tip_block_number();
        let status = self.block_rpc.get_block_status_by_hash(Hash256(last_block_hash)).await?;
        if status.in_best_chain {
            match status.next_best {
                Some(next_hash) => {
                    self.timer.event(format!(
                        "block # {} with hash {} is in best chain, next best hash is {}",
                        last_block_number,
                        Hash256(last_block_hash).to_reversed_hex_string(),
                        next_hash.to_reversed_hex_string(),
                    ));
                    let next_header = self.block_rpc.get_qd_block_header_by_hash(next_hash).await?;
                    self.process_append_block::<NC, BP, BSS>(bp_transmitter, bss, next_header, last_block_number+1).await?;
                    return Ok(())
                    

                },
                None => {
                    self.timer.event(format!(
                        "block # {} with hash {} is in best chain, waiting for next hash",
                        last_block_number,
                        Hash256(last_block_hash).to_reversed_hex_string(),
                    ));
                    return Ok(());
                }
            }

        }else{
            self.timer.event(format!("block # {} with hash {} no longer in best chain, reverting!", self.state.chain_state.get_tip_block_number(), Hash256(last_block_hash).to_reversed_hex_string()));

            let last_finalized = self.state.chain_state.get_finalized_block_hash();
            let last_finalized_state = self.block_rpc.get_block_status_by_hash(Hash256(last_finalized)).await?;
            if last_finalized_state.in_best_chain == false {
                self.timer.event(format!("THIS IS BAD: the last finalized block ({} at #{}) is no longer in the best chain, lets keep praying it will be again", hex::encode(&self.state.chain_state.get_finalized_block_hash()), self.state.chain_state.get_finalized_block_number()));
                return Ok(());
            }
            for i in 0..QDOGE_BRIDGE_REQUIRED_CONFIRMATIONS {
                let h = self.state.chain_state.block_data_tracker.get_block_hash(last_block_number-(i as u32))?;
                let alt_status = self.block_rpc.get_block_status_by_hash(Hash256(h)).await?;
                if alt_status.in_best_chain {
                    self.timer.event(format!("found an alternative block # {} with hash {} in the best chain, reverting to this block", last_block_number-(i as u32), hex::encode(&h)));
                    todo!("handle revert");
                    //bp_receiver.enqueue_revert_imm(revert).await?;
                    return Ok(());
                }
            }
            self.timer.event(format!("THIS IS BAD: the last finalized block ({} at #{}) is no longer in the best chain, lets keep praying it will be again", hex::encode(&self.state.chain_state.get_finalized_block_hash()), self.state.chain_state.get_finalized_block_number()));
            return Ok(());
        }

        Ok(())

    }
    pub async fn process_append_block<
        NC: DogeNetworkConfig,
        BP: BlockProcessorWorkerEventTransmitterAsyncImm,
        BSS: IBCBlockStateStoreAsyncImm,
    >(
        &mut self,
        bp_transmitter: &BP,
        bss: &BSS,
        header: QDogeBlockHeader,
        block_number: u32,
    ) -> anyhow::Result<()> {
        
        //let mut timer = TraceTimer::new("process_next_job");

        let last_block_hash = self.state.chain_state.get_tip_block_hash();
        let last_block_number = self.state.chain_state.get_tip_block_number();
        let new_block_number = last_block_number + 1;
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

        let old_state = self.state.chain_state.clone();

        match self
            .state
            .chain_state
            .append_block::<NC>(new_block_number, &header, None)
        {
            Ok(_) => {
                self.timer.event(format!(
                    "appended block # {} to chain state",
                    new_block_number
                ));
            }
            Err(e) => {
                self.state.chain_state = old_state;
                anyhow::bail!("error appending block to chain state: {}", e);
            }
        }
        
        


        //bss.injest_ibc_block_state_imm(&self.state).await?;
        bp_transmitter.enqueue_block_headers_imm(&[BlockHeaderQueueItem::BlockHeader(BlockHeaderQueueItemBlockHeader{
            block_header: header,
            block_number: block_number,
        })]).await?;
        Ok(())
    }
    /*
    async fn process_next_job_revert_internal<
        NC: DogeNetworkConfig,
        BP: BlockProcessorWorkerEventTransmitterAsyncImm,
        BSS: IBCBlockStateStoreAsyncImm,
    >(
        &mut self,
        bp_receiver: &BP,
        bss: &BSS,
        new_block_headers: &[QDogeBlockHeader],
        last_good_block_number: u32,
    ) -> anyhow::Result<()> {
        //let mut timer = TraceTimer::new("process_next_job");

        todo!("implement revert");
        Ok(())
    }*/
}
