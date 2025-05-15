use std::time::Duration;

use doge_light_client::constants::DogeTestNetConfig;
use fred::{prelude::{ClientLike, Config, ReconnectPolicy}, types::Builder};
use kvq::memory::arc_imm::KVQArcImmutableStoreWrapper;
use qed_dsol_bridge_core::{data::state::IBCBlockStateStoreReaderAsync, utils::debug_timer::DebugTimer};
use qed_dsol_ibc_node_common::{doge_link_rpc_async::DogeLinkElectrsAsyncClient, nimpl::{proof_store_fred::ProofStoreFred, simple_submitter::SimpleBlockSubmitter}, sol_submitter::SolSubmitterClient, worker::{simple_async_block_processor::SimpleAsyncBlockProcessor, simple_rpc_block_notifier::SimpleRPCBlockNotifier}};



async fn run_block_notifier() -> anyhow::Result<()> {

    let mut timer = DebugTimer::new("block_notifier_ex1");
    timer.lap("start");
    


    type DNConfig = DogeTestNetConfig;
    let rpc_client = DogeLinkElectrsAsyncClient::new("https://doge-electrs-testnet-demo.qed.me".to_string());
    let config = Config::from_url("redis://127.0.0.1:6379")?;
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
    
    //let st = q.get_state_for_block(7664698).await?;
    //println!("state for block 7664687: {:?}", st);


    let mut bp = SimpleRPCBlockNotifier::init_block_notifier(&q, rpc_client).await?;

    bp.run_worker::<DNConfig, _, _>(&q, &q).await?;


    

    Ok(())

}


#[tokio::main]
async fn main() {
    run_block_notifier().await.unwrap();
}