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

use qed_dsol_bridge_core::{config::network_constants::QEDDogeChainState, data::{scrypt_proof::DogeBlockScryptProofOutput, state::IBCBlockState}};
use serde::{Deserialize, Serialize};
use zerocopy::FromBytes;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSCInitIBCStateRequestBody {
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSCInitContractStateRequestBody {
    pub block_number: u32,
    pub block_header_bytes: String,
    pub scrypt_hash: String,
    pub proof: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SCCGetIBCStateResponse {
    pub initialized: bool,
    pub state: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolSubmitterClient {
    pub base_url: String,
    pub api_key: String,
}
impl SolSubmitterClient {
    pub fn new(url: String, api_key: String) -> Self {
        Self {
            base_url: format!("{}/api/v1", url),
            api_key,
        }
    }
    pub async fn init_ibc_program_state(&self, block_state_bytes: &[u8]) -> anyhow::Result<()> {
        let url = format!("{}/init-ibc", self.base_url);
        let client = reqwest::Client::new();
        let res = client.post(&url)
            .header("x-api-key", self.api_key.clone())
            .json(&SSCInitIBCStateRequestBody {
                data: hex::encode(block_state_bytes),
            })
            .send()
            .await?;
        let status = res.status();
        if status.is_success() {
            let resp = res.text().await?;
            println!("Init IBC block state response: {}", resp);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Failed to init IBC block state: {:?}", res.text().await?))
        }
    }
    pub async fn get_ibc_program_state_full(&self) -> anyhow::Result<Option<QEDDogeChainState>> {
        match self.get_ibc_program_state_inner().await? {
            Some(state_bytes) => {
                match QEDDogeChainState::ref_from_bytes(&state_bytes[33..]) {
                    Ok(state) => Ok(Some(state.clone())),
                    Err(e) => Err(anyhow::anyhow!("Failed to parse IBC block state: {:?}", e)),
                }
            }
            None => Ok(None),
        }
    }
    pub async fn get_ibc_program_state_inner(&self) -> anyhow::Result<Option<Vec<u8>>> {
        let url = format!("{}/get-ibc-state", self.base_url);
        let client = reqwest::Client::new();
        let res = client.get(&url)
            .header("x-api-key", self.api_key.clone())
            .send()
            .await?;
        let status = res.status();
        if status.is_success() {
            let resp: SCCGetIBCStateResponse = res.json().await?;
            if resp.initialized {
                Ok(Some(hex::decode(resp.state.unwrap())?))
            } else {
                Ok(None)
            }
        } else {
            Err(anyhow::anyhow!("Failed to get IBC block state: {:?}", res.text().await?))
        }
    }

    pub async fn append_block_zkp(&self, block_height: u32, block_header_bytes: &[u8], proof_result: &DogeBlockScryptProofOutput) -> anyhow::Result<()> {
        let url = format!("{}/append-block-zkp", self.base_url);
        let client = reqwest::Client::new();
        let res = client.post(&url)
            .header("x-api-key", self.api_key.clone())
            .json(&SSCInitContractStateRequestBody {
                block_number: block_height,
                block_header_bytes: hex::encode(block_header_bytes),
                scrypt_hash: hex::encode(&proof_result.scrypt_hash),
                proof: hex::encode(&proof_result.groth16_proof),
            })
            .send()
            .await?;
        let status = res.status();
        if status.is_success() {
            let resp = res.text().await?;
            println!("Submit block result response: {}", resp);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Failed to submit block result: {:?}", res.text().await?))
        }
    }
}