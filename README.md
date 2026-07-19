# Doge–Solana IBC Services

This repository owns the current single-block update service (`e2e_block_pipeline`) and a historical Redis-backed Dogecoin block-header relay prototype. Cross-repository local tooling and the complete Dogecoin → Solana → Dogecoin smoke flow live in the sibling `psy-doge-solana-cli` repository.

The current `e2e_block_pipeline` polls finalized Dogecoin blocks, builds real deposit witnesses, invokes the SP1 block-transition prover, submits the unique `block_update` through the isolated block sender, and processes finalized mint buffers.

The workspace also retains a historical Redis-backed Dogecoin block-header relay prototype: an Electrs client, work queues and state snapshots, a header notifier, a block processor, dummy proof workers, and an HTTP client for a separate legacy submitter.

Despite the historical `ibc` names, this is not a general IBC implementation. It does not implement IBC clients, connections, channels, or packets.

## Status and relationship to the current bridge

The authoritative on-chain bridge is [`psy-doge-solana-bridge`](https://github.com/PsyProtocol/psy-doge-solana-bridge). Production operator commands and local Bun tooling are owned by `psy-doge-solana-cli`. The SP1 workspace is `psy-bridge-sp1`. This repository owns block detection/proving/submission services around those components.

The legacy relay described after the current integration section is not compatible with the current bridge proof or account interfaces. In particular, its 260-byte `DogeBlockScryptProofOutput`, dummy workers, Redis schema, and historical submitter API are separate from the current SP1 block-transition pipeline.

## Current integration ownership

Local Bun tooling is owned by the sibling CLI:

| Path | Role |
| --- | --- |
| `psy-doge-solana-cli/tools/local/launcher.ts` | Local environment launcher |
| `psy-doge-solana-cli/tools/local/runner.ts` | Internal implementation behind `local-e2e` |
| `psy-doge-solana-cli/tools/deploy/devnet.ts` | Devnet program deployment (separate) |

Preferred complete local smoke entry:

```bash
doge-solana-cli --network localhost local-e2e
```

Global CLI syntax is always `doge-solana-cli --network localhost|devnet …`. This repository no longer ships Bun launcher/smoke entry points; run them from the CLI checkout to avoid dual-source drift.

This repository continues to own the Rust `e2e_block_pipeline`. It polls finalized Dogecoin blocks, builds and verifies the real deposit witness, invokes the single block-transition SP1 `gen-proof`, uploads finalized mint/TXO buffers, submits the unique `block_update` through the isolated block sender, processes finalized mints, persists a Redis checkpoint, and writes content-addressed proof evidence.

### Focused local commands

Local tools (from a sibling checkout layout with `psy-doge-solana-cli` next to this repo):

```bash
cd ../psy-doge-solana-cli

# Compile-check internal Bun tools.
bun build tools/local/runner.ts --target=bun --outfile=/tmp/psy-doge-solana-cli-local-runner
bun build tools/local/launcher.ts --target=bun --outfile=/tmp/psy-doge-solana-cli-local-launcher

# Read-only launcher validation; no services are spawned.
bun tools/local/launcher.ts --network localhost --preflight --no-build

# Explicit complete local validation (may start dogecoind, Electrs, Solana validator,
# IBC pipeline, SP1, Sender, and local Manager service).
doge/target/release/doge-solana-cli --network localhost local-e2e
```

Devnet operator commands use remote public endpoints and start **no** local
processes (no dogecoind, Electrs, Solana validator, Manager service, Sender,
IBC, Redis, or SP1). External Manager service availability is an operational
prerequisite. Program deployment is separate:

```bash
cd ../psy-doge-solana-cli
bun tools/deploy/devnet.ts --network devnet \
  --payer /secure/payer.json \
  --program-key-dir /secure/program-keys \
  --preflight
```

Official Wormhole / Manager IDs on Solana devnet:

| Component | ID |
| --- | --- |
| Wormhole Core | `3u8hJUVTA4jH1wYAyUur7FFZVQ8H635K3tSHHF4ssjQ5` |
| Wormhole Shim | `EtZMZM22ViKMo4r5y4Anovs3wKQ2owUmDpjygnMMcdEX` |
| Delegated Manager Set program | `wdmsTJP6YnsfeQjPuuEzGCrHmZvTmNy8VkxMCK8JkBX` |
| Manager set index | `1` (official Wormhole set 1) |

Testnet/QED block ingestion uses the explicit profile and profile-specific embedded SP1 guest. Set the local bridge header/config/keypair values separately; `DOGE_START_HEIGHT` is the already-initialized checkpoint height, so the pipeline ingests `height+1` onward:

```bash
export DOGE_NETWORK=testnet
export DOGE_ELECTRS_URL=https://doge-electrs-testnet-demo.qed.me
export SP1_GEN_PROOF_PATH=../psy-bridge-sp1/target/release/gen-proof
export SP1_BLOCK_ELF_PATH=../psy-bridge-sp1/target/elf-compilation/riscv64im-succinct-zkvm-elf/release/block-transition-testnet
export SP1_BLOCK_VK_HASH=00b25e2fe5866751a38e5ca4d975b30b4187f3e0528a06dc86edc6e9a8b9cc02
export DOGE_START_HEIGHT=<bridge-checkpoint-height>
cargo run --release -p qed_dsol_ibc_node_common --example e2e_block_pipeline -- \
  --network testnet \
  --electrs-url https://doge-electrs-testnet-demo.qed.me \
  --start-height "$DOGE_START_HEIGHT" \
  # append the required sender/Solana/bridge arguments shown by --help
```

The pipeline passes `--network testnet` to `gen-proof`, rejects any VK other than the testnet profile key, and checks both the canonical embedded ELF path and SHA-256 against `SP1_BLOCK_ELF_PATH` before accepting prover output.

Local CLI smoke requires prebuilt `doge-solana-cli`, Dogecoin Core, electrs-doge, and block-sender `dist/` artifacts. The launcher auto-builds a missing bridge CLI and SP1 `gen-proof`/guest ELF, and always rebuilds local Solana SBF programs with the exact `solprogram,noopshim,regtest-vk` profile before starting the isolated validator. Install the corresponding Rust/SP1 toolchains and `cargo-build-sbf`.

## Legacy relay workspace and package map

The Rust workspace has three members; the table below describes their legacy relay surface plus the current pipeline hosted in `qed_dsol_ibc_node_common`:

| Package | Role |
| --- | --- |
| `qed_dsol_bridge_core` | Relay data and interfaces: the configured Dogecoin chain-state type, block queue items, scrypt proof input/output types, queue/store traits, state-store traits, and the asynchronous submitter trait. |
| `qed_dsol_ibc_node_common` | Current `e2e_block_pipeline` plus legacy operational components: the Dogecoin Electrs-compatible HTTP client, Redis implementation, notifier/processor workers, dummy/fake proof workers, and submitter clients. |
| `kvq` | Generic typed key/value serialization, binary-store traits, adapters, caches, and in-memory stores. It is a library used by the relay types; it is not a standalone relay service. |

Important examples under `qed_dsol_ibc_node_common/examples/`:

| Example | Purpose |
| --- | --- |
| `block_processor` | Reconciles/initializes state, consumes header work, requests scrypt proof results, submits blocks over HTTP, and persists successful state. |
| `rpc_block_notifier` | Follows the next-best Dogecoin header from the saved relay tip and enqueues it for the processor. |
| `scrypt_g16_prover` | **Dummy worker despite its name.** It instantiates `SimpleAsyncDummyProver`. |
| `dummy_g16_worker` | The same dummy signature-based worker under a less misleading example name. |
| `fake_g16_worker` | Writes an all-`0xff` 260-byte proof field; useful only for queue plumbing experiments. |
| `g16_dummy_check` | Locally exercises recovery of the public key from the dummy worker's signature-shaped data. It does not verify Groth16. |
| `read_chain_acc`, `dec1` | Hardcoded Redis state inspection experiments for block `7665804`; they are not general CLI tools. |

There are no command-line flags or environment-variable configuration paths in these examples. Operational values are compiled into the example source.

## Legacy service architecture

```text
                         Redis (shared namespace/seed)
                      +--------------------------------+
Dogecoin Electrs      | block-processor list           |
compatible HTTP API --+--> notifier enqueues header ---+----+
                      |                                 |    |
                      | scrypt job list                 |    v
                      | proof-result hash               | processor
                      | per-block completion lists      |    |
                      | state-by-height hash             |    +--> submitter HTTP API
                      | latest-state hash                |             |
                      +--------------------------------+              v
                                  ^                         historical Solana
                                  |                         program/operator
                           dummy prover worker
```

The actual sequence is:

1. **Processor startup owns initialization and reconciliation.** It reads the latest Redis state and calls the submitter's `GET /api/v1/get-ibc-state`. If neither side has state, it fetches the current Dogecoin tip plus the 31 preceding headers (32 cached hashes total), builds an empty-tree initial chain state, writes it to Redis, and posts it to `POST /api/v1/init-ibc`. Because Redis is written before the HTTP initialization call, an `init-ibc` failure can leave Redis initialized while the submitter remains uninitialized; the next startup can rebuild from the then-current Dogecoin tip.
2. **Notifier startup requires Redis state.** It waits until the processor or another operator has written a latest-state record. It then tracks that saved tip through the Electrs block-status API.
3. **Notifier enqueues a header.** When the current tip has `next_best`, it fetches that header and pushes a serialized `BlockHeaderQueueItem::BlockHeader` into the Redis processor queue. Its in-memory state advances before enqueueing, but it does not write that speculative state to Redis.
4. **Processor validates and applies the next header.** It requires the queued height to be exactly local tip + 1 and the previous hash to equal the local tip, then updates its in-memory `QEDDogeChainState`.
5. **Processor obtains a proof result.** For the Dogecoin header, or the AuxPoW parent header when present, it first checks the Redis proof-result hash using the Dogecoin block hash. On a cache miss—which is the expected checked-in behavior because the writer keys by a different header-hash derivation—it pushes an 80-byte header into the scrypt worker queue and polls a per-block notification list.
6. **Proof worker returns development data.** The included runnable worker computes Dogecoin scrypt, stores a 372-byte logical result (`80-byte header + 32-byte scrypt hash + 260-byte proof field`), and publishes it to the per-block notification list.
7. **Processor submits, then commits local state.** It posts the height, Borsh-serialized `QDogeBlockHeader`, scrypt hash, and 260-byte proof field to `POST /api/v1/append-block-zkp`. Only after a successful HTTP status does it write the updated chain state to Redis.

All three long-running examples must use the same Redis instance and the same suffix seed. The checked-in examples use seed `1337`.

## Prerequisites and hidden services

### Build prerequisites

- Rust and Cargo with network access for crates and Git dependencies.
- The sibling source tree `../dlc-lib/doge-light-client-workspace` with `doge-light-client`, `psy-doge-data-link`, and `psy-doge-bridge-helper`; these are path dependencies in the workspace manifests.
- Native build tools may be required by transitive Rust dependencies; consult the failing dependency if Cargo reports a missing compiler or system library.

The repository currently cannot be treated as a self-contained checkout: Cargo must be able to parse and build those sibling path dependencies.

### Runtime prerequisites

1. **Redis** reachable at `redis://127.0.0.1:6379` for the checked-in examples. The examples create a pool of eight connections, use a 10-second connection timeout, and configure exponential reconnect backoff starting at 100 ms and capped at 30 seconds; the `ReconnectPolicy` constructor receives `0` as its attempts argument, whose exact retry interpretation is defined by the pinned `fred` version.
2. **Dogecoin Electrs-compatible HTTP service.** The examples hardcode `https://doge-electrs-testnet-demo.qed.me` and use `DogeTestNetConfig`. Required paths are listed below.
3. **A separate submitter HTTP service** at `http://localhost:3000` implementing the exact `/api/v1` endpoints below and accepting the hardcoded development API key. The server is not implemented in this repository.
4. **The historical Solana program/operator behind that submitter.** This repository only sends HTTP requests; it does not configure a Solana RPC URL, signer, program ID, or deployment.
5. **A proof worker.** The included workers are dummy/fake only. A real producer would also need a matching legacy verifier and must emit exactly the format expected by this topology.

The separately maintained `solana-doge-bridge-block-sender` belongs to the historical submitter topology. This code does not prove that an arbitrary/current revision of that service is compatible, so verify its request bodies, state prefix, program build, and deployment before pairing them.

## Build and run

Run commands from the repository root.

### 1. Confirm the workspace can resolve

```bash
cargo metadata --no-deps --format-version 1
```

If this fails while loading `../dlc-lib/doge-light-client-workspace/...`, `../psy-doge-solana-bridge/clients/rust`, or another sibling path dependency, restore/fix the sibling checkout before attempting the relay. See [Troubleshooting](#troubleshooting).

### 2. Build the relay examples

```bash
cargo build --release --package qed_dsol_ibc_node_common --examples
```

### 3. Start Redis

For a disposable local experiment:

```bash
docker run --rm --name solana-doge-ibc-redis -p 6379:6379 redis
```

Removing that container also removes all queues, proof cache entries, and relay snapshots. Use a persistent Redis configuration if you need restart testing.

### 4. Start the external submitter

Before starting the processor, supply a service at `http://localhost:3000` that implements the API contract in [Submitter HTTP API contract](#submitter-http-api-contract), including `x-api-key: doge-test-api-key`.

The sibling CLI localhost launcher can start the block sender for local smoke; the legacy manual sequence in this section still assumes that service is started separately.

### 5. Start the processor first

```bash
cargo run --release --package qed_dsol_ibc_node_common --example block_processor
```

Start this before the notifier on a new Redis namespace. The processor is the component that can initialize Redis and submitter state; the notifier otherwise waits for a latest Redis snapshot indefinitely.

### 6. Start a dummy proof worker

```bash
cargo run --release --package qed_dsol_ibc_node_common --example scrypt_g16_prover
```

Equivalently, the explicitly named duplicate example is:

```bash
cargo run --release --package qed_dsol_ibc_node_common --example dummy_g16_worker
```

Do **not** run both merely to obtain different proof behavior: they both instantiate `SimpleAsyncDummyProver`. Multiple instances compete for the same Redis list and may be used to explore worker concurrency, but the code establishes no production worker count or delivery guarantee.

### 7. Start the notifier

```bash
cargo run --release --package qed_dsol_ibc_node_common --example rpc_block_notifier
```

For useful logs, set a `tracing-subscriber` filter only after adding corresponding initialization to the example; the checked-in examples do not currently initialize a tracing subscriber or expose a logging CLI.

### Focused development checks

Compile the libraries and examples:

```bash
cargo check --workspace --all-targets
```

Run the dummy signature recovery example:

```bash
cargo run --release --package qed_dsol_ibc_node_common --example g16_dummy_check
```

The current pipeline has focused Rust tests. Complete local validation is owned by `psy-doge-solana-cli` via the sole public entry `doge-solana-cli --network localhost local-e2e`. The legacy state-reader examples contain a fixed historical height and should only be used after editing them for the namespace/height being investigated.

## Hardcoded configuration

The runnable notifier, processor, and prover examples use these values:

| Setting | Checked-in value | Where/impact |
| --- | --- | --- |
| Dogecoin network type | `DogeTestNetConfig` | Consensus/header processing in notifier and processor. |
| Electrs base URL | `https://doge-electrs-testnet-demo.qed.me` | All Dogecoin HTTP reads. Availability is external and not guaranteed. |
| Redis URL | `redis://127.0.0.1:6379` | Queues, proof cache, and chain state. |
| Redis pool size | `8` | Per-process `fred` pool. |
| Queue/state seed | `1337` | Derives all shared queue/state suffixes. Different seeds create isolated topologies. |
| Submitter base URL | `http://localhost:3000` | The client appends `/api/v1`. |
| Submitter API key | `doge-test-api-key` | Sent as `x-api-key`; it is an example credential, not a secret suitable for deployment. |
| Required confirmations | `1` | `QDOGE_BRIDGE_REQUIRED_CONFIRMATIONS`; affects chain-state finalization/reorg assumptions. |
| Cached block hashes | `32` | Used for initial header fetch and the chain-state block hash cache. |
| Block tree height | `32` | Compile-time chain-state tree parameter. |

There is no `.env` loader or CLI parser in the examples. To change endpoints, credentials, namespaces, or network type, edit the example source or write an operator binary around the library components. Never commit real API credentials or private keys.

## Redis queues, keys, and state ownership

`ProofStoreFred::new_with_seed(pool, 1337)` derives the following active names:

| Purpose | Redis type | Key for seed `1337` | Producer / consumer |
| --- | --- | --- | --- |
| Block processor work | List | `PBPSCWQV1-$bpq1337d` | Notifier `LPUSH`; processor `RPOP`. |
| Scrypt proof work | List | `PSSCWQV1-$sq1337b` | Processor `LPUSH`; prover `RPOP`. Each entry is exactly 80 header bytes. |
| Proof completion | List per block hash | `PSNQV1-$nq1337c#scrypt#<block-hash-hex>` | Prover `LPUSH`; waiting processor `RPOP`. |
| Proof-result cache | Hash | `PSV1` | Prover writes the bincode-serialized result under both the single-SHA-256 header hash and the scrypt hash fields. The processor looks up by the Dogecoin double-SHA-256 block hash, so the checked-in cache writer and reader use different block-hash derivations. In the normal path the processor therefore waits for the completion list; do not rely on the cache-hit branch without fixing/verifying this mismatch. This key is **global**, not seed-suffixed. |
| State by height | Hash | `PSIBCV1-$iss1337e` | Processor writes field `u64 height` in big-endian bytes to serialized `IBCBlockState`. |
| Latest state | Hash | `LPSIBCV1-$iss1337e` | Processor writes field eight zero bytes to the latest serialized state. Notifier reads it. |

Other generic proof/checkpoint prefixes exist in `ProofStoreFred` (`PSWQV1`, `PSDQV1_`, `PSHQV1`, and the unsuffixed generic notification list), but the three-service header path above does not use them.

### Queue semantics that matter operationally

- Lists use `LPUSH` plus `RPOP`, producing FIFO behavior for a single queue.
- Consumption is destructive. There is no Redis transaction, visibility timeout, acknowledgement list, or dead-letter queue.
- Polling is implemented by repeated nonblocking `RPOP` calls and sleeps (100 ms for proof work, 500 ms for proof completion, 1 second for header work).
- A worker crash after popping but before re-enqueueing/committing can lose that queue item.
- A proof result is cached before the completion notification is pushed. However, the cache writer's header-hash key and the processor's Dogecoin block-hash lookup differ, so the checked-in implementation does not establish successful cache reuse after a lost notification.
- State history is retained by height, while a separate hash holds the latest snapshot. There is no pruning or migration logic.
- All cooperating processes must use identical suffixes. Changing the seed makes old queue/state data invisible, except for the global `PSV1` proof cache.

Treat Redis as relay-owned durable state. Do not flush it or delete individual keys while workers are running. The code has no administrative repair command and does not prove that Redis loss can be recovered without duplicate or divergent external submissions.

## Dogecoin Electrs API expectations

`DogeLinkElectrsAsyncClient` constructs unauthenticated HTTP GET requests as `<base-url>/<path>`. The relay needs these Electrs-style responses:

| Request | Expected response/use |
| --- | --- |
| `GET /blocks/tip/height` | JSON integer `u32`; used to select the initial 32-header window. |
| `GET /block-height/{height}` | 64-character block-hash hex text. |
| `GET /block/{hash}/header` | Raw 80-byte block header encoded as hex text. |
| `GET /block/{hash}/status` | JSON object with `height: number|null`, `in_best_chain: boolean`, and `next_best: string|null`. `next_best` is used by the notifier to follow the chain. |

The client also implements `GET /block/{hash}/raw` for full block reads, although the three-service header path initializes and advances using headers/status calls.

The wrapper does not call `error_for_status`; malformed bodies and HTTP error pages usually surface as decode/deserialization errors rather than a purpose-built status error.

## Submitter HTTP API contract

`SolSubmitterClient::new("http://localhost:3000", key)` sets the API base to `http://localhost:3000/api/v1`. Every call sends `x-api-key: <key>`. Any 2xx status is accepted; non-2xx response text is returned as an error.

### `GET /api/v1/get-ibc-state`

Expected JSON:

```json
{
  "initialized": false,
  "state": null
}
```

When `initialized` is `true`, `state` must be a hex string. The client hex-decodes it and then parses `QEDDogeChainState` starting at byte offset **33**. Therefore the external service's returned account/state layout must include the exact prefix expected by this legacy client. A short or differently laid-out response can panic or fail parsing; the layout is not negotiated.

### `POST /api/v1/init-ibc`

JSON body:

```json
{
  "data": "<hex-encoded raw QEDDogeChainState bytes>"
}
```

The processor calls this only when neither usable Redis nor submitter state is found after the startup checks.

### `POST /api/v1/append-block-zkp`

JSON body:

```json
{
  "block_number": 7654501,
  "block_header_bytes": "<hex-encoded Borsh QDogeBlockHeader>",
  "scrypt_hash": "<64 hex characters>",
  "proof": "<520 hex characters representing 260 bytes>"
}
```

`block_header_bytes` is **not always an 80-byte standard header**: it is the Borsh serialization of `QDogeBlockHeader` and may contain AuxPoW data. The proof job itself uses the 80-byte proof-of-work header (the AuxPoW parent header when present).

The client expects only success/failure status and treats a success body as printable text. It does not parse a transaction signature, commitment, resulting height, or idempotency token. The operator must verify those properties in the external service.

## Dummy prover: exact behavior

The runnable `scrypt_g16_prover` is **not SP1 and not Groth16**. The name and `groth16_proof` field are legacy labels.

`SimpleAsyncDummyProver` does the following for an 80-byte header:

1. Computes Dogecoin `scrypt_1024_1_1_256(header)`.
2. Computes `sha256(header)`.
3. Computes `sha256(sha256(header) || scrypt_hash)`.
4. Signs that 32-byte digest with a hardcoded development secp256k1 private key.
5. Places signature `r` in bytes `0..32`, `s` in `32..64`, and the recovery ID in byte `64` of a zero-filled 260-byte array.
6. Returns that array in the field named `groth16_proof`.

The public inputs used by `g16_dummy_check` are 112 bytes (`80-byte header || 32-byte scrypt hash`). That example recovers a hardcoded public key from the signature. It does not execute a zkVM, construct an arithmetic circuit, produce a Groth16 proof, or verify one.

`SimpleAsyncFakeProver` is even less meaningful: it computes the scrypt hash and returns `[0xff; 260]` as the proof. Never expose either worker to production value or describe their output as a cryptographic proof of correct scrypt execution.

The hardcoded signing key is repository test material. Do not reuse it for authentication, custody, fees, or any environment in which it could control value.

## Restart, reconciliation, and reorganization behavior

### What processor startup actually does

The processor reads Redis latest state and the external submitter state:

- **Redis absent, submitter present:** use submitter state and then save it as latest Redis state.
- **Both present at the same height:** keep the Redis state; the code compares heights but does not compare hashes or full state equality.
- **Redis behind submitter:** replace local latest state with submitter state.
- **Redis ahead of submitter:** poll submitter state every 5 seconds, up to 10 times. Regardless of whether it catches up, the code then chooses the last submitter state it read, potentially moving the processor behind Redis.
- **Submitter initially uninitialized:** wait 5 seconds and check once more. If still uninitialized, create state from the current Dogecoin tip, persist Redis state, and call `init-ibc`.

Important limitations:

- A submitter response that changes from initialized to uninitialized during the Redis-ahead polling path is unwrapped and can panic.
- Reconciliation is height-based; equal-height divergent hashes are not detected.
- There is no scan of pending Redis lists and no replay ledger for HTTP submissions.
- In-memory retry counts reset on process restart.

### Runtime retry behavior

- The notifier catches an error, logs it, waits one second, and retries.
- The processor's outer loop propagates errors, but expected per-header failures are usually caught internally. Previous-hash mismatches are requeued up to 3 times in memory; other processing/submission failures are requeued up to 10 times, with state rolled back to the prior snapshot. After the limit the item is dropped.
- Proof workers propagate an error out of their loop, so the example process exits because `main` unwraps the result.
- Redis queue pops are destructive, so process supervision alone does not guarantee replay of an item already popped.
- HTTP append has no client-side idempotency key. A timeout after the server commits but before the client observes success can cause a retry and duplicate submission unless the external service enforces idempotency.

### Notifier restart behavior

The notifier starts from the latest **committed Redis** state, not from its previous in-memory speculative state. If latest state already existed when it starts, it repeatedly samples that state one second apart until it observes no change, then begins following the chain. The log says “2 minutes,” but the implemented delay is one second.

This is only a quiet-period heuristic. It does not inspect queue length and can race with a processor or queued work.

### Reorganizations are not implemented safely

When the notifier discovers that its current tip is no longer in the best chain, the revert path reaches `todo!("handle revert")` if it finds an alternative cached block. Processor support for `RevertFork` also ends in `todo!("implement revert")`. With the configured confirmation count of 1, deep-finalized reorgs are logged and ignored.

Do not operate this topology with an assumption of automatic reorg recovery.

## Troubleshooting

### Cargo cannot load `doge-light-client`

Symptom:

```text
failed to load manifest for dependency `doge-light-client`
```

The root manifest uses sibling path dependencies under `../dlc-lib`. Ensure those paths contain complete packages that can resolve their own workspace-inherited dependencies. A directory containing only a partial package manifest is insufficient.

### Notifier waits forever at startup

The notifier loops until `LPSIBCV1-$iss1337e` contains a valid latest state. Start the processor first and make sure it can reach both Electrs and the submitter. Also confirm every process uses the same Redis URL and seed.

### Processor fails before consuming headers

Check `GET http://localhost:3000/api/v1/get-ibc-state` with the required API key. The service must return the documented JSON and, when initialized, the exact legacy hex/account layout. A current bridge API is not a drop-in replacement.

### Processor waits forever for a proof

Confirm a dummy prover is running and sharing Redis seed `1337`. Inspect whether work exists in `PSSCWQV1-$sq1337b` and whether the per-hash completion key is being written. A worker may have exited on a malformed item or Redis error.

### Header repeatedly requeues or is dropped

Likely causes include a stale/competing notifier, a queue from a previous state history, an Electrs fork, divergent Redis/submitter states, or unsupported reorganization. Stop all workers before investigating; do not blindly replay or reorder serialized list entries.

### `append-block-zkp` returns an error

Verify all of the following against the historical submitter implementation:

- `x-api-key` acceptance;
- Borsh `QDogeBlockHeader` serialization expected by the server;
- 260-byte legacy proof field rather than a current SP1 proof;
- initialized on-chain state layout and height;
- external signer funding, Solana RPC, program ID, and deployed verifier configuration.

Those Solana-side settings are not configured in this repository.

### Restart produced duplicate or missing work

This is consistent with the implemented delivery model: destructive Redis pops, no acknowledgements, no append idempotency token, and state persistence only after successful append response. Recover by comparing Dogecoin best-chain data, Redis latest/history states, Redis queues/proof cache, and the authoritative external program state before deciding what to replay. The repository provides no automated reconciliation command.

## Legacy relay security and operational limitations

The following limitations apply to the historical notifier/processor/dummy-prover topology, not to the CLI-owned local tools (`tools/local/`) or the Rust `e2e_block_pipeline` documented above:

- No production proof system is included in the legacy topology.
- The legacy example API key and dummy signing key are public development constants.
- Legacy Redis has no authentication or TLS in the checked-in URL.
- Legacy Electrs requests have no authentication, explicit HTTP status validation, explicit timeout configuration, or response pinning.
- Legacy submitter requests construct a new HTTP client per call and have no explicit timeout or retry policy.
- Legacy Redis state serialization and submitter account parsing are version-sensitive and lack migration/version negotiation beyond key-name prefixes.
- Equal-height legacy chain-state divergence is not checked.
- Legacy reorganization handling is unfinished and can panic.
- The legacy topology provides no metrics, health endpoints, graceful shutdown, queue administration, dead-letter handling, or packaged service units.
- No audit or production-readiness claim is made for the legacy topology.

## What the historical relay does not provide

- A complete Dogecoin/Solana asset bridge by itself.
- Standard IBC light-client, relayer, handshake, channel, or packet semantics.
- A real proof system in the dummy `scrypt_g16_prover`/`dummy_g16_worker` examples.
- The historical submitter server or a verified compatible Solana program deployment.
- Proven exactly-once processing, crash recovery, Redis-loss recovery, or reorg recovery for the legacy queues.
- Compatibility between the legacy relay interfaces and the current `psy-doge-solana-bridge` proof/account interfaces.

## License

This project is licensed under the GNU Affero General Public License, version 3 or later, with the additional attribution terms stated in [LICENSE](./LICENSE). Copyright © 2025 Zero Knowledge Labs Limited and Psy Protocol.