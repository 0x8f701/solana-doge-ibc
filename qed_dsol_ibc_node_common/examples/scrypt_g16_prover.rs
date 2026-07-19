use std::time::Duration;

use doge_light_client::constants::DogeTestNetConfig;
use fred::{prelude::{ClientLike, Config, ReconnectPolicy}, types::Builder};
use kvq::memory::arc_imm::KVQArcImmutableStoreWrapper;
use qed_dsol_bridge_core::utils::debug_timer::DebugTimer;
use qed_dsol_ibc_node_common::{doge_link_rpc_async::DogeLinkElectrsAsyncClient, nimpl::{proof_store_fred::ProofStoreFred, simple_submitter::SimpleBlockSubmitter}, sol_submitter::SolSubmitterClient, worker::{simple_async_block_processor::SimpleAsyncBlockProcessor, simple_async_dummy_prover::SimpleAsyncDummyProver}};



async fn run_dummy_g16_worker() -> anyhow::Result<()> {

    let mut timer = DebugTimer::new("scrypt_g16_worker_ex1");
    timer.lap("start");
    


    type DNConfig = DogeTestNetConfig;
    let rpc_client = DogeLinkElectrsAsyncClient::new("https://doge-electrs-testnet-demo.qed.me".to_string());
    let config = Config::from_url("redis://127.0.0.1:6379")?;
    let sol_submitter_client = SolSubmitterClient::new("http://localhost:3000".to_string(), "doge-test-api-key".to_string())?;
    let suffix_seed = 1337u64;



    let pool_size = 8;
    let pool = Builder::from_config(config)
        .with_connection_config(|config| {
            config.connection_timeout = Duration::from_secs(10);
        })
        // use exponential backoff, starting at 100 ms and doubling on each failed attempt up to 30 sec
        .set_policy(ReconnectPolicy::new_exponential(0, 100, 30_000, 2))
        .build_pool(pool_size)?;
    
    pool.init().await?;
    timer.lap("connected to redis");
    
    let q = ProofStoreFred::new_with_seed(pool.clone(), suffix_seed);

    let mut prover = SimpleAsyncDummyProver::new();

    prover.run_worker(&q, &q).await?;


    

    Ok(())

}


#[tokio::main]
async fn main() {
    run_dummy_g16_worker().await.unwrap();
}