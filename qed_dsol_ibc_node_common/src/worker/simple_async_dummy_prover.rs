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

use doge_light_client::hash::{
    scrypt_doge::scrypt_1024_1_1_256, sha256::QSha256Hasher, traits::BytesHasher,
};
use qed_dsol_bridge_core::data::scrypt_proof::{
    DogeBlockScryptProofInput, DogeBlockScryptProofOutput,
};

use qed_dsol_bridge_core::{
    job::g16_scrypt_traits::{QScryptProofStoreAsyncImm, ScryptProverWorkerEventReceiverAsyncImm},
    utils::debug_timer::DebugTimer,
};

const DUMMY_ZKP_PRIVATE_KEY: [u8; 32] = [
    0xfb, 0xea, 0x92, 0x40, 0x02, 0xcd, 0x14, 0x04, 0x69, 0x71, 0x3f, 0x15, 0x5a, 0x99, 0x68, 0xcc,
    0xed, 0x24, 0x01, 0xb3, 0x83, 0x39, 0x68, 0x50, 0xf8, 0x0a, 0x80, 0xe5, 0x18, 0xaf, 0x9a, 0x81,
];

pub struct SimpleAsyncDummyProver {
    timer: DebugTimer,
}
impl SimpleAsyncDummyProver {
    pub fn new() -> Self {
        Self {
            timer: DebugTimer::new("scrypt_prover"),
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

        self.timer.event(format!(
            "got new job with header_bytes: {}",
            hex::encode(&job.block_header)
        ));
        let result = gen_dummy_zkp(job)?;

        self.timer.event(format!(
            "header_scrypt_hash: {}",
            hex::encode(&result.scrypt_hash)
        ));


        store.injest_scrypt_proof_result_imm(&result).await?;
        self.timer.lap("wrote proof to store");

        event_receiver.notify_block_hash_completed(result).await?;
        self.timer.lap("notified complete");
        Ok(())
    }
}
pub fn get_public_inputs_from_output(output: &DogeBlockScryptProofOutput) -> [u8; 112] {
    let mut public_inputs = [0u8; 112];
    public_inputs[0..80].copy_from_slice(&output.block_header);
    public_inputs[80..112].copy_from_slice(&output.scrypt_hash);
    public_inputs
}

pub fn gen_dummy_zkp(
    data: DogeBlockScryptProofInput,
) -> anyhow::Result<DogeBlockScryptProofOutput> {

    // I have pruned out the SP1 code since we are switching to risc0 but you can check https://github.com/PsyProtocol/sp1-dogecoin-scrypt-hash for an example of how to generate DogeBlockScryptProofOutput from DogeBlockScryptProofInput
    let scrypt_hash = scrypt_1024_1_1_256(&data.block_header);
    let sha_hash = QSha256Hasher::hash_bytes(&data.block_header);
    let sig_payload = QSha256Hasher::hash_bytes(&[sha_hash, scrypt_hash].concat());
    let (sig, rec_id) = k256::ecdsa::SigningKey::from_bytes(&DUMMY_ZKP_PRIVATE_KEY.into())?
        .sign_prehash_recoverable(&sig_payload)?;

    let r_bytes: [u8; 32] = sig.r().to_bytes().into();
    let s_bytes: [u8; 32] = sig.s().to_bytes().into();

    let mut proof_data = [0u8; 260];
    proof_data[0..32].copy_from_slice(&r_bytes);
    proof_data[32..64].copy_from_slice(&s_bytes);

    let rec_id_u8: u8 = rec_id.into();

    proof_data[64] = rec_id_u8;

    Ok(DogeBlockScryptProofOutput {
        block_header: data.block_header,
        scrypt_hash,
        groth16_proof: proof_data,
    })
}
