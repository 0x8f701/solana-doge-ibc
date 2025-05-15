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

use plonky2::{hash::hash_types::RichField, plonk::proof::ProofWithPublicInputs};
use qed_core::{data::{base_types::hash256::Hash256, qhashout::QHashOut}, job::id::{QJobTopic, QProvingJobDataID}};
use qed_crypto::hash::merkle::core::MerkleProofCore;
use qed_data::guta::header::GlobalUserTreeAggregatorHeader;

use crate::common::api_request_id::QEDAPIWriteRequestId;





pub trait CoordinatorBlockAPIInputQueueImm<F: RichField> {
    fn add_user_registration_request_imm(&self, request_id: QEDAPIWriteRequestId, fingerprint: QHashOut<F>, public_key_param: QHashOut<F>) -> anyhow::Result<()>;
    fn add_contract_deploy_request_imm(&self, request_id: QEDAPIWriteRequestId, public_key: QHashOut<F>) -> anyhow::Result<()>;
}


pub trait CoordinatorBlockAPINodeImmRead<F: RichField> {
    fn get_guta_sub_tree_merkle_proof(&self, guta_realm_id: u64) -> anyhow::Result<MerkleProofCore<QHashOut<F>>>;
}
pub trait CoordinatorBlockAPINodeImm<F: RichField> {
    fn report_guta_update(&self, request_id: QEDAPIWriteRequestId, proof_id: QProvingJobDataID, proof_to_sub_root: MerkleProofCore<QHashOut<F>>, header: GlobalUserTreeAggregatorHeader<F>) -> anyhow::Result<()>;

}