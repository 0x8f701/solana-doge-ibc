use std::time::{SystemTime, UNIX_EPOCH};
use bitcoin::{block::{Header, SimpleHeader}, hashes::Hash, Block};
use doge_light_client::{common_types::QHash256, core_data::{QAuxPow, QDogeBlock, QDogeBlockHeader, QMerkleBranch, QStandardBlockHeader}, doge::{coinbase_transaction::DogeAuxPowCoinbaseTransaction, transaction::BTCTransaction}};
use futures::future;
use serde::de::DeserializeOwned;

use crate::network_retry::{is_retryable_reqwest, NetworkRetryPolicy, RetryAction};
#[derive(Debug, Clone)]
pub struct DogeLinkElectrsAsyncClient {
    electrs_url: String,
    client: reqwest::Client,
    retry_policy: NetworkRetryPolicy,
}

impl DogeLinkElectrsAsyncClient {
    pub fn new(electrs_url: String) -> Self {
        let retry_policy = NetworkRetryPolicy::default();
        let client = reqwest::Client::builder()
            .timeout(retry_policy.request_timeout())
            .build()
            .expect("Electrs HTTP client configuration must be valid");
        Self {
            electrs_url,
            client,
            retry_policy,
        }
    }
    pub async fn get_json<T: DeserializeOwned>(&self, path: &str) -> anyhow::Result<T> {
        self.get_json_with_retry(path).await
    }
    pub async fn get_bytes(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        let response = self.get_response_bytes(path).await?;
        Ok(response)
    }
    pub async fn get_text(&self, path: &str) -> anyhow::Result<String> {
        let bytes = self.get_response_bytes(path).await?;
        Ok(String::from_utf8(bytes)?)
    }

    async fn get_json_with_retry<T: DeserializeOwned>(&self, path: &str) -> anyhow::Result<T> {
        let bytes = self.get_response_bytes(path).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    async fn get_response_bytes(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        let url = format!("{}/{}", self.electrs_url, path);
        self.retry_policy
            .run("Electrs request", || async {
                let response = match self.client.get(&url).send().await {
                    Ok(response) => response,
                    Err(error) if is_retryable_reqwest(&error) => {
                        return RetryAction::Retry(anyhow::Error::new(error));
                    }
                    Err(error) => return RetryAction::Fatal(anyhow::Error::new(error)),
                };
                let status = response.status();
                if !status.is_success() {
                    let error = anyhow::anyhow!("Electrs request {url} returned HTTP {status}");
                    return if crate::network_retry::is_retryable_http_status(status.as_u16()) {
                        RetryAction::Retry(error)
                    } else {
                        RetryAction::Fatal(error)
                    };
                }
                match response.bytes().await {
                    Ok(bytes) => RetryAction::Success(bytes.to_vec()),
                    Err(error) if is_retryable_reqwest(&error) => {
                        RetryAction::Retry(anyhow::Error::new(error))
                    }
                    Err(error) => RetryAction::Fatal(anyhow::Error::new(error)),
                }
            })
            .await
    }

    pub async fn get_block_height(&self) -> anyhow::Result<u32> {
        // QED's public edge can cache this dynamic endpoint independently of
        // block/status responses. A unique query forces the current tip.
        let cache_bust = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let height: u32 = self
            .get_json::<u32>(&format!("blocks/tip/height?fresh={cache_bust}"))
            .await?;
        Ok(height)
    }
    pub async fn get_block(&self, height: u32) -> anyhow::Result<Block> {
        let hash_txt = self.get_text(&format!("block-height/{}", height)).await?;
        let block_data = self.get_bytes(&format!("block/{}/raw", hash_txt)).await?;
        let btc_block: Block = bitcoin::consensus::encode::deserialize(&block_data)?;

        //let bh  = bitcoin::consensus::encode::serialize(&btc_block.header);
        //println!("bh_len: {}", bh.len());

        Ok(btc_block)

    }
    pub async fn get_qd_block(&self, height: u32) -> anyhow::Result<QDogeBlock> {
        btc_block_to_qdoge(&self.get_block(height).await?)
    }

    async fn get_qd_block_header(&self, height: u32) -> anyhow::Result<QDogeBlockHeader> {
        let hash = self.get_text(&format!("block-height/{height}")).await?;
        let header_hex = self.get_text(&format!("block/{hash}/header")).await?;
        let header: Header = bitcoin::consensus::encode::deserialize(&hex::decode(header_hex)?)?;
        btc_block_header_to_qdoge(&header)
    }

    pub async fn get_qd_block_headers_range_parallel(&self, start_height: u32, end_height: u32) -> anyhow::Result<Vec<QDogeBlockHeader>> {
        let mstr = self.electrs_url.clone();
        let results = future::join_all((start_height..=end_height).map(|block_height| {
            let k = mstr.clone();
            async move {
                let resp = DogeLinkElectrsAsyncClient::new(k.clone()).get_qd_block_header(block_height).await;
                resp
            }
        }))
        .await.into_iter().map(|x|x.map_err(|e| anyhow::anyhow!("{:?}",e))).collect::<anyhow::Result<Vec<_>>>()?;

    
        Ok(results)
    }

}




fn btc_block_to_qdoge(btc_block: &Block) -> anyhow::Result<QDogeBlock> {

    let txs = btc_block.txdata.iter().map(|x|BTCTransaction::from_bytes(&bitcoin::consensus::encode::serialize(&x))).collect::<anyhow::Result<Vec<BTCTransaction>>>()?;
    let header_bytes: Vec<u8> = bitcoin::consensus::encode::serialize::<SimpleHeader>(&btc_block.header.to_simple_header());
    let auxp = match &btc_block.header.aux_data {
        Some(ap) => {
            Some(QAuxPow {
                coinbase_transaction: DogeAuxPowCoinbaseTransaction::from_bytes(&bitcoin::consensus::encode::serialize(&ap.coinbase_tx))?,
                block_hash: ap.block_hash.to_raw_hash().to_byte_array().into(),
                coinbase_branch: QMerkleBranch {
                    side_mask: ap.coinbase_branch.side_mask,
                    hashes: ap.coinbase_branch.hashes.iter().map(|x|x.to_raw_hash().to_byte_array().into()).collect::<Vec<QHash256>>(),
                },
                blockchain_branch: QMerkleBranch {
                    side_mask: ap.blockchain_branch.side_mask,
                    hashes: ap.blockchain_branch.hashes.iter().map(|x|x.to_raw_hash().to_byte_array().into()).collect::<Vec<QHash256>>(),
                },
                parent_block: QStandardBlockHeader::from_bytes(&bitcoin::consensus::encode::serialize(&ap.parent_block))?,
            })
        },
        None => None,
    };

    let qdb = QDogeBlock {
        header: QStandardBlockHeader::from_bytes(&header_bytes)?,
        transactions: txs,
        aux_pow: auxp,
    };
    Ok(qdb)
}

fn btc_block_header_to_qdoge(btc_header: &Header) -> anyhow::Result<QDogeBlockHeader> {

    let header_bytes: Vec<u8> = bitcoin::consensus::encode::serialize::<SimpleHeader>(&btc_header.to_simple_header());
    let auxp = match &btc_header.aux_data {
        Some(ap) => {
            Some(QAuxPow {
                coinbase_transaction: DogeAuxPowCoinbaseTransaction::from_bytes(&bitcoin::consensus::encode::serialize(&ap.coinbase_tx))?,
                block_hash: ap.block_hash.to_raw_hash().to_byte_array().into(),
                coinbase_branch: QMerkleBranch {
                    side_mask: ap.coinbase_branch.side_mask,
                    hashes: ap.coinbase_branch.hashes.iter().map(|x|x.to_raw_hash().to_byte_array().into()).collect::<Vec<QHash256>>(),
                },
                blockchain_branch: QMerkleBranch {
                    side_mask: ap.blockchain_branch.side_mask,
                    hashes: ap.blockchain_branch.hashes.iter().map(|x|x.to_raw_hash().to_byte_array().into()).collect::<Vec<QHash256>>(),
                },
                parent_block: QStandardBlockHeader::from_bytes(&bitcoin::consensus::encode::serialize(&ap.parent_block))?,
            })
        },
        None => None,
    };

    let qdb = QDogeBlockHeader {
        header: QStandardBlockHeader::from_bytes(&header_bytes)?,
        aux_pow: auxp,
    };
    Ok(qdb)
}