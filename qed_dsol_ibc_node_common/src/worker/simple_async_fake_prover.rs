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


use doge_light_client::hash::scrypt_doge::scrypt_1024_1_1_256;
use qed_dsol_bridge_core::{
    data::scrypt_proof::DogeBlockScryptProofOutput,
    job::g16_scrypt_traits::{
        QScryptProofStoreAsyncImm,
        ScryptProverWorkerEventReceiverAsyncImm,
    }, utils::debug_timer::DebugTimer,
};

pub struct SimpleAsyncFakeProver {
    timer: DebugTimer,
}
impl SimpleAsyncFakeProver {
    pub fn new() -> Self {
        Self {
            timer: DebugTimer::new("fake_prover"),
        }
    }
    pub async fn run_worker<
        PS: QScryptProofStoreAsyncImm + Send + Sync,
        ER: ScryptProverWorkerEventReceiverAsyncImm,
    >(
        &mut self,
        store: &PS,
        event_receiver: &ER,
    ) -> anyhow::Result<()> {
        loop {
            self.process_next_job(store, event_receiver).await?;
        }
    }
    pub async fn process_next_job<
        PS: QScryptProofStoreAsyncImm + Send + Sync,
        ER: ScryptProverWorkerEventReceiverAsyncImm,
    >(
        &mut self,
        store: &PS,
        event_receiver: &ER,
    ) -> anyhow::Result<()> {
        //let mut timer = TraceTimer::new("process_next_job");
        self.timer.lap("waiting for new job...");
        let job = event_receiver.wait_for_next_job_imm().await?;

        self.timer.event(format!("got new job with header_bytes: {}", hex::encode(&job.block_header)));
        let header_scrypt_hash = scrypt_1024_1_1_256(&job.block_header);
        self.timer.event(format!("header_scrypt_hash: {}", hex::encode(&header_scrypt_hash)));

        

        let fake_proof = [0xffu8; 260];

        let result = DogeBlockScryptProofOutput {
            block_header: job.block_header,
            scrypt_hash: header_scrypt_hash,
            groth16_proof: fake_proof,
        };

        store.injest_scrypt_proof_result_imm(&result).await?;
        self.timer.lap("wrote proof to store");

        event_receiver.notify_block_hash_completed(result).await?;
        self.timer.lap("notified complete");
        Ok(())
    }
}
