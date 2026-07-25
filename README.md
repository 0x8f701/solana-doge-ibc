# Doge–Solana IBC Services

This repository owns the production `doge_block_pipeline` service. Cross-repository local tooling and the complete Dogecoin → Solana → Dogecoin smoke flow live in the sibling `psy-doge-solana-cli` repository.

The pipeline polls finalized Dogecoin blocks, builds real deposit witnesses, invokes the SP1 block-transition prover, submits the unique `block_update` through the isolated block sender, processes finalized mint buffers, persists Redis checkpoints, and archives content-addressed proof evidence.

Despite the historical `ibc` names, this is not a general IBC implementation. It does not implement IBC clients, connections, channels, or packets.

## Status and relationship to the current bridge

The authoritative on-chain bridge is [`psy-doge-solana-bridge`](https://github.com/PsyProtocol/psy-doge-solana-bridge). Production operator commands and local Bun tooling are owned by `psy-doge-solana-cli`; the SP1 workspace is `psy-bridge-sp1`. This repository owns only block detection, proving orchestration, submission, checkpointing, and evidence around those components.

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

This repository continues to own the Rust `doge_block_pipeline` binary. It polls finalized Dogecoin blocks, builds and verifies the real deposit witness, invokes the single block-transition SP1 `gen-proof`, uploads finalized mint/TXO buffers, submits the unique `block_update` through the isolated block sender, processes finalized mints, persists a Redis checkpoint, and writes content-addressed proof evidence.

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
cargo run --release -p qed_dsol_ibc_node_common --bin doge_block_pipeline -- \
  --network testnet \
  --electrs-url https://doge-electrs-testnet-demo.qed.me \
  --start-height "$DOGE_START_HEIGHT" \
  # append the required sender/Solana/bridge arguments shown by --help
```

The pipeline passes `--network testnet` to `gen-proof`, rejects any verifying key other than the testnet profile key, and validates the daemon identity by the path-independent guest id plus the embedded block-transition ELF SHA-256 and verifying key hash (never by the daemon's local path) before accepting prover output.

Local CLI smoke requires prebuilt `doge-solana-cli`, Dogecoin Core, electrs-doge, and block-sender `dist/` artifacts. The launcher auto-builds a missing bridge CLI and SP1 `gen-proof`/guest ELF, and always rebuilds local Solana SBF programs with the exact `solprogram,noopshim,regtest-vk` profile before starting the isolated validator. Install the corresponding Rust/SP1 toolchains and `cargo-build-sbf`.

## Production package map

| Package | Role |
| --- | --- |
| `qed_dsol_ibc_node_common` | Production Electrs client, block pipeline, retry policy, Redis checkpoint/evidence handling, Sender client, and `doge_block_pipeline` binary. |
| `qed_dsol_bridge_core` | Shared historical data types retained by other workspace members; it is not linked into the production pipeline crate. |
| `kvq` | Generic typed key/value serialization library used by historical workspace components; it is not a standalone service. |

The old notifier/processor/fake/dummy worker topology and its examples were removed from the production crate. Redis checkpoint keys, checkpoint journals, proof archive recovery, operator-store schema migrations, and historical withdrawal/noop payload compatibility remain intentionally because they protect persisted production state.

## License

This project is licensed under the GNU Affero General Public License, version 3 or later, with the additional attribution terms stated in [LICENSE](./LICENSE). Copyright © 2025 Zero Knowledge Labs Limited and Psy Protocol.