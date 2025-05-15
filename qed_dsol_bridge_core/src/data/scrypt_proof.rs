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


use kvq::traits::KVQSerializable;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use doge_light_client::hash::{sha256::{QBTCHash256Hasher, QSha256Hasher}, traits::BytesHasher};

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DogeBlockScryptProofInput {
    #[serde_as(as = "[_; 80]")]
    pub block_header: [u8; 80],
}

impl DogeBlockScryptProofInput {
    pub fn new(block_header: [u8; 80]) -> Self {
        Self { block_header }
    }
    pub fn to_fixed_bytes(&self) -> [u8; 80] {
        self.block_header
    }
}

impl KVQSerializable for DogeBlockScryptProofInput {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(self.block_header.to_vec())
    }

    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        if bytes.len() != 80 {
            anyhow::bail!(
                "expected 80 bytes for deserializing DogeBlockScryptProofInput, got {} bytes",
                bytes.len()
            );
        }
        let mut inner_data = [0u8; 80];
        inner_data.copy_from_slice(bytes);
        Ok(DogeBlockScryptProofInput { block_header: inner_data })
    }
}


#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DogeBlockScryptProofOutput {
    #[serde_as(as = "[_; 80]")]
    pub block_header: [u8; 80],
    pub scrypt_hash: [u8; 32],
    #[serde_as(as = "[_; 260]")]
    pub groth16_proof: [u8; 260],
}
// 
impl DogeBlockScryptProofOutput {
    pub fn get_block_hash(&self) -> [u8; 32] {
        QBTCHash256Hasher::hash_bytes(&self.block_header)
    }
}
impl KVQSerializable for DogeBlockScryptProofOutput {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let mut result = Vec::with_capacity(80 + 32 + 260);
        result.extend_from_slice(&self.block_header);
        result.extend_from_slice(&self.scrypt_hash);
        result.extend_from_slice(&self.groth16_proof);
        Ok(result)
    }

    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        if bytes.len() != 80 + 32 + 260 {
            anyhow::bail!(
                "expected 372 bytes for deserializing DogeBlockScryptProofOutput, got {} bytes",
                bytes.len()
            );
        }
        let block_header = bytes[0..80].try_into().unwrap();
        let scrypt_hash = bytes[80..112].try_into().unwrap();
        let groth16_proof = bytes[112..372].try_into().unwrap();
        Ok(DogeBlockScryptProofOutput {
            block_header,
            scrypt_hash,
            groth16_proof,
        })
        
    }
}