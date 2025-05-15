use bitcoin::{block::{Header, SimpleHeader}, hashes::Hash, Block};
use doge_light_client::{core_data::{QAuxPow, QDogeBlock, QDogeBlockHeader, QHash256, QMerkleBranch, QStandardBlockHeader}, doge::transaction::BTCTransaction};
use futures::future;
use qed_dsol_bridge_core::data::base_types::hash256::Hash256;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct DogeLinkElectrsAsyncClient {
    electrs_url: String,
}
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct DogeLinkBlockStatusResponse {
    pub height: Option<u32>,
    pub in_best_chain: bool,
    pub next_best: Option<Hash256>,
}

impl DogeLinkElectrsAsyncClient {
    pub fn new(electrs_url: String) -> Self {
        DogeLinkElectrsAsyncClient {
            electrs_url,
        }
    }
    pub async fn get_json<T: DeserializeOwned>(&self, path: &str) -> anyhow::Result<T> {
        let url = format!("{}/{}", self.electrs_url, path);
        let response = reqwest::get(&url).await?.json::<T>().await?;
        Ok(response)
    }
    pub async fn get_bytes(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        let url = format!("{}/{}", self.electrs_url, path);
        let response = reqwest::get(&url).await?.bytes().await?.to_vec();
        Ok(response)
    }
    pub async fn get_text(&self, path: &str) -> anyhow::Result<String> {
        let url = format!("{}/{}", self.electrs_url, path);
        let response = reqwest::get(&url).await?.text().await?;
        Ok(response)
    }

    pub async fn get_block_height(&self) -> anyhow::Result<u32> {
        let height: u32 = self.get_json::<u32>("blocks/tip/height").await?;
        Ok(height)
    }
    pub async fn get_block_qhash(&self, height: u32) -> anyhow::Result<QHash256> {
        let hash_txt = self.get_text(&format!("block-height/{}", height)).await?;
        let mut hash = [0u8; 32];
        hex::decode_to_slice(hash_txt, &mut hash)?;
        hash.reverse();
        Ok(hash)
    }
    pub async fn get_block_hash(&self, height: u32) -> anyhow::Result<Hash256> {
        let hash_txt = self.get_text(&format!("block-height/{}", height)).await?;
        if hash_txt.len() != 64 {
            anyhow::bail!("expected hash of length 64, got '{}'",hash_txt);
        }else{
            let mut hash = [0u8; 32];
            hex::decode_to_slice(hash_txt, &mut hash)?;
            hash.reverse();
            Ok(Hash256(hash))
        }
    }
    pub async fn get_block(&self, height: u32) -> anyhow::Result<Block> {
        let hash_txt = self.get_text(&format!("block-height/{}", height)).await?;
        let block_data = self.get_bytes(&format!("block/{}/raw", hash_txt)).await?;
        let btc_block: Block = bitcoin::consensus::encode::deserialize(&block_data)?;

        //let bh  = bitcoin::consensus::encode::serialize(&btc_block.header);
        //println!("bh_len: {}", bh.len());

        Ok(btc_block)

    }
    pub async fn get_block_header_by_hash(&self, hash: Hash256) -> anyhow::Result<Header> {
        let block_header_hex = self.get_text(&format!("block/{}/header", hash.to_reversed_hex_string())).await?;
        println!("block_header_hex: {}, hash: {}", block_header_hex, hash.to_reversed_hex_string());
        let btc_block_header: Header = bitcoin::consensus::encode::deserialize(&hex::decode(block_header_hex)?)?;
        Ok(btc_block_header)
    }
    pub async fn get_block_header(&self, height: u32) -> anyhow::Result<Header> {
        self.get_block_header_by_hash(self.get_block_hash(height).await?).await
    }
    pub async fn get_blocks(&self, heights: &[u32]) -> anyhow::Result<Vec<Block>> {
        let mut blocks = Vec::with_capacity(heights.len());
        for h in heights.iter(){
            blocks.push(self.get_block(*h).await?);
        }
        Ok(blocks)
    }
    pub async fn get_qd_block(&self, height: u32) -> anyhow::Result<QDogeBlock> {
        btc_block_to_qdoge(&self.get_block(height).await?)
    }
    pub async fn get_qd_blocks(&self, heights: &[u32]) -> anyhow::Result<Vec<QDogeBlock>> {
        let mut blocks = Vec::with_capacity(heights.len());
        for h in heights.iter(){
            blocks.push(self.get_qd_block(*h).await?);
        }
        Ok(blocks)
    }
    pub async fn get_qd_block_header(&self, height: u32) -> anyhow::Result<QDogeBlockHeader> {
        let btc_header = self.get_block_header(height).await?;
        Ok(btc_block_header_to_qdoge(&btc_header)?)
    }
    pub async fn get_qd_block_header_by_hash(&self, hash: Hash256) -> anyhow::Result<QDogeBlockHeader> {
        let btc_header = self.get_block_header_by_hash(hash).await?;
        Ok(btc_block_header_to_qdoge(&btc_header)?)
    }
    pub async fn get_qd_block_headers(&self, heights: &[u32]) -> anyhow::Result<Vec<QDogeBlockHeader>> {
        let mut headers = Vec::with_capacity(heights.len());
        for h in heights.iter(){
            headers.push(self.get_qd_block_header(*h).await?);
        }
        Ok(headers)
    }
    pub async fn get_qd_block_headers_range(&self, start_height: u32, end_height: u32) -> anyhow::Result<Vec<QDogeBlockHeader>> {
        let mut headers = Vec::with_capacity((end_height - start_height + 1) as usize);
        for h in start_height..=end_height{
            //println!("get header: {}",h);
            headers.push(self.get_qd_block_header(h).await?);
        }
        Ok(headers)
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

    pub async fn get_block_status_by_hash(&self, block_hash: Hash256) -> anyhow::Result<DogeLinkBlockStatusResponse> {
        let mut base: DogeLinkBlockStatusResponse = self.get_json(&format!("block/{}/status", block_hash.to_reversed_hex_string())).await?;
        if base.next_best.is_some() {
            let mut d = base.next_best.unwrap().0;
            d.reverse();
            base.next_best = Some(Hash256(d));
        }
        Ok(base)
    }
}




fn btc_block_to_qdoge(btc_block: &Block) -> anyhow::Result<QDogeBlock> {

    let txs = btc_block.txdata.iter().map(|x|BTCTransaction::from_bytes(&bitcoin::consensus::encode::serialize(&x))).collect::<anyhow::Result<Vec<BTCTransaction>>>()?;
    let header_bytes: Vec<u8> = bitcoin::consensus::encode::serialize::<SimpleHeader>(&btc_block.header.to_simple_header());
    let auxp = match &btc_block.header.aux_data {
        Some(ap) => {
            Some(QAuxPow {
                coinbase_transaction: BTCTransaction::from_bytes(&bitcoin::consensus::encode::serialize(&ap.coinbase_tx))?,
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
                coinbase_transaction: BTCTransaction::from_bytes(&bitcoin::consensus::encode::serialize(&ap.coinbase_tx))?,
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