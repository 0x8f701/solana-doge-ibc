# Doge-Solana IBC Core

The core server for the Doge -> Solana route

Made by Psy Protocol, Psy.xyz


Highlights
* Fully Trustless
* Bulletproof, self-healing (kill -9 all the services, then delete the database, and start it up and it will still work like normal =D)


## Usage

Before starting, startup a redis node:
```bash
docker run -it --rm -p 6379:6379 redis                
```

### Block Notifier (Only One Needed)

A block notifier connects to the Psy/QED electrs API to watch for block updates. If a new block is encountered, it is pushed to a queue for the block processor.

```bash
cargo run --release --package qed_dsol_ibc_node_common --example rpc_block_notifier
```


### Block Processor (Only One Needed)
A Block Processor waits for incoming blocks in the queue, schedules proving for the scrypt zero knowledge prover and submits the block headers to solana.

```bash
cargo build --release --package qed_dsol_ibc_node_common --example block_processor
```



cargo run --release --package qed_dsol_ibc_node_common --example rpc_block_notifier


### Scrypt Prover (~4 Needed)
A proving worker which proves that a header has a given scrypt_1024_1_1_256 hash.

```bash

cargo run --release --package qed_dsol_ibc_node_common --example scrypt_g16_prover
```


## Note:

You will also need to run:
https://github.com/PsyProtocol/solana-doge-bridge-block-sender to send the blocks (this is separated to isolate the gas fee payer from the rest of the infra)


## License
See [License](./LICENSE)