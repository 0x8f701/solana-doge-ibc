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

use qed_dsol_bridge_core::data::{scrypt_proof::DogeBlockScryptProofOutput, state::{IBCBlockState, IBCBlockSubmitterAsync}};
use async_trait::async_trait;
use crate::sol_submitter::SolSubmitterClient;

pub struct SimpleBlockSubmitter {
    pub sol_submitter_client: SolSubmitterClient,
    
}
impl SimpleBlockSubmitter {
    pub fn new(sol_submitter_client: SolSubmitterClient) -> Self {
        Self {
            sol_submitter_client,
        }
    }
}

#[async_trait]
impl IBCBlockSubmitterAsync for SimpleBlockSubmitter{
    async fn submit_block_result(&self, block_height: u64, _state: &IBCBlockState, header_bytes: &[u8], proof_result: &DogeBlockScryptProofOutput) -> anyhow::Result<()>{
        self.sol_submitter_client.append_block_zkp(block_height as u32, header_bytes, proof_result).await?;
        Ok(())
    }

}