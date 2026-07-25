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

use serde::{Deserialize, Serialize};
use crate::network_retry::{
    is_retryable_http_status, is_retryable_reqwest, NetworkRetryPolicy, RetryAction,
};



#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockUpdateRequestBody {
    pub idempotency_key: String,
    pub proof_hex: String,
    pub header_hex: String,
    pub mint_buffer: String,
    pub txo_buffer: String,
    pub mint_buffer_bump: u8,
    pub txo_buffer_bump: u8,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockUpdateResponse {
    pub signature: String,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AmbiguousBlockUpdateResponse {
    status: String,
    signature: String,
    idempotency_key: String,
}

#[derive(Debug, Clone)]
pub struct SolSubmitterClient {
    pub base_url: String,
    pub api_key: String,
    client: reqwest::Client,
    retry_policy: NetworkRetryPolicy,
}
impl SolSubmitterClient {
    pub fn new_with_bearer_token(url: String, bearer_token: String) -> anyhow::Result<Self> {
        Self::build(url, bearer_token)
    }

    fn build(url: String, api_key: String) -> anyhow::Result<Self> {
        let retry_policy = NetworkRetryPolicy::default();
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(retry_policy.request_timeout())
            .build()?;
        Ok(Self {
            base_url: format!("{}/api/v1", url),
            api_key,
            client,
            retry_policy,
        })
    }

    /// Submits the current bridge block_update contract using optional Bearer auth.
    pub async fn block_update(
        &self,
        request: &BlockUpdateRequestBody,
    ) -> anyhow::Result<BlockUpdateResponse> {
        let url = format!("{}/block-update", self.base_url);
        self.retry_policy
            .run("Sender block_update request", || async {
                let mut builder = self.client.post(&url).json(request);
                if !self.api_key.is_empty() {
                    builder = builder.bearer_auth(&self.api_key);
                }
                let response = match builder.send().await {
                    Ok(response) => response,
                    Err(error) if is_retryable_reqwest(&error) => {
                        return RetryAction::Retry(anyhow::Error::new(error));
                    }
                    Err(error) => return RetryAction::Fatal(anyhow::Error::new(error)),
                };
                let status = response.status();
                if status == reqwest::StatusCode::ACCEPTED {
                    return match response.json::<AmbiguousBlockUpdateResponse>().await {
                        Ok(ambiguous) if ambiguous.status == "ambiguous" => RetryAction::Fatal(
                            anyhow::anyhow!(
                                "block-update submission outcome is ambiguous for signature {} and idempotency key {}",
                                ambiguous.signature,
                                ambiguous.idempotency_key
                            ),
                        ),
                        Ok(ambiguous) => RetryAction::Fatal(anyhow::anyhow!(
                            "unexpected accepted block-update status '{}'",
                            ambiguous.status
                        )),
                        Err(error) if is_retryable_reqwest(&error) => {
                            RetryAction::Retry(anyhow::Error::new(error))
                        }
                        Err(error) => RetryAction::Fatal(anyhow::Error::new(error)),
                    };
                }
                if status.is_success() {
                    return match response.json::<BlockUpdateResponse>().await {
                        Ok(response) => RetryAction::Success(response),
                        Err(error) if is_retryable_reqwest(&error) => {
                            RetryAction::Retry(anyhow::Error::new(error))
                        }
                        Err(error) => RetryAction::Fatal(anyhow::Error::new(error)),
                    };
                }
                let retryable = is_retryable_http_status(status.as_u16());
                let body = match response.text().await {
                    Ok(body) => body,
                    Err(error) if is_retryable_reqwest(&error) => {
                        return RetryAction::Retry(anyhow::Error::new(error));
                    }
                    Err(error) => return RetryAction::Fatal(anyhow::Error::new(error)),
                };
                let error = anyhow::anyhow!(
                    "block-update submission failed with HTTP {}: {}",
                    status,
                    body
                );
                if retryable {
                    RetryAction::Retry(error)
                } else {
                    RetryAction::Fatal(error)
                }
            })
            .await
    }
}