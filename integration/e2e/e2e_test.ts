#!/usr/bin/env bun

import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { parseArgs } from "node:util";
import {
    Connection,
    Keypair,
    PublicKey,
    Transaction,
    TransactionInstruction,
} from "../../../psy-doge-solana-bridge/clients/js/node_modules/@solana/web3.js";
import { TOKEN_PROGRAM_ID } from "../../../psy-doge-solana-bridge/clients/js/node_modules/@solana/spl-token";
import bs58 from "../../../psy-doge-solana-bridge/clients/js/node_modules/bs58";

type Profile = "real-noop" | "initialized-noop";
type JsonObject = Record<string, unknown>;
type CommandResult = { command: string[]; exitCode: number; stdout: string; stderr: string; durationMs: number };
type ElectrsUtxo = { txid: string; vout: number; value: number; status: { confirmed: boolean; block_height?: number } };
type FundingArtifact = {
    address: string;
    wif: string;
    txid: string;
    vout: number;
    value: number;
    blockHeight: number;
    confirmations: number;
    minedBlocks: number;
    dogecoinTipHeight: number;
    electrsTipHeight: number;
};
type BridgeOutput = {
    bridgeStatePda: string;
    dogeMint: string;
    operatorPubkey: string;
    payerPubkey: string;
    operatorKeypair: string;
    payerKeypair: string;
    operatorStore: string;
    userKeypair: string;
    userPubkey: string;
    userTokenAccount: string;
};
type BridgeProgress = {
    accountDataLength: number;
    tipHeight: number;
    finalizedHeight: number;
    autoClaimedDepositsNextIndex: number;
};
type TokenBalance = { amount: bigint; decimals: number; uiAmountString: string };
type Options = {
    profile: Profile;
    dryRun: boolean;
    fullLiveRegtest: boolean;
    keepServices: boolean;
    selfTest: boolean;
};
type Evidence = {
    schema: string;
    startedAt: string;
    finishedAt?: string;
    completed: boolean;
    profile: Profile;
    mode: "dry-plan" | "full-live-regtest";
    withdrawalMode: {
        managerSigningEnabled: boolean;
        broadcastEnabled: boolean;
        dryRunDefault: boolean;
        fullLiveRegtest: boolean;
    };
    paths: JsonObject;
    phases: Record<string, JsonObject>;
    completion?: JsonObject;
    failure?: { message: string; stack?: string };
};

const IBC_REPO = path.resolve(import.meta.dir, "../..");
const PROJECTS_DIR = path.resolve(IBC_REPO, "..");
const BRIDGE_REPO = path.join(PROJECTS_DIR, "psy-doge-solana-bridge");
const CLI_REPO = path.join(PROJECTS_DIR, "psy-doge-solana-cli");
const LOCAL_OPS_ROOT = path.join(CLI_REPO, "doge");
const DOGE_RPC_URL = "http://127.0.0.1:22555";
const ELECTRS_URL = "http://127.0.0.1:3002";
const SOLANA_RPC = "http://127.0.0.1:8899";
const MANAGER_SERVICE_URL = "http://127.0.0.1:7071";
const DEPOSIT_BIN = path.join(LOCAL_OPS_ROOT, "target/release/deposit_to_solana");
const PROCESS_WITHDRAWAL_BIN = path.join(LOCAL_OPS_ROOT, "target/release/process_withdrawal");
const LOC_SETUP = path.join(import.meta.dir, "locSetupDoge.ts");
const BRIDGE_OUTPUT_PATH = path.join(BRIDGE_REPO, "bridge-config/bridge-output.json");
const USER_OUTPUT_PATH = path.join(BRIDGE_REPO, "bridge-config/users/user1.json");
const KEYS_DIR = path.join(BRIDGE_REPO, "bridge-config/keys");
const DOGECOIN_REPO = path.join(PROJECTS_DIR, "dogecoin");
const ELECTRS_REPO = path.join(PROJECTS_DIR, "electrs-doge");
const EVIDENCE_PATH = "/tmp/e2e-test-evidence.json";
const DEPOSIT_EVIDENCE_PATH = "/tmp/e2e-test-deposit-evidence.json";
const WITHDRAWAL_EVIDENCE_PATH = "/tmp/e2e-test-withdrawal-evidence.json";
const FUNDING_ARTIFACT_PATH = "/tmp/e2e-test-funding.json";
const BLOCK_PROOF_EVIDENCE_ROOT = "/tmp/psy-doge-block-proof-evidence";
const BLOCK_PROOF_LATEST_PATH = path.join(BLOCK_PROOF_EVIDENCE_ROOT, "latest.json");
const DOGE_BRIDGE_PROGRAM = new PublicKey("DBjo5tqf2uwt4sg9JznSk9SBbEvsLixknN58y3trwCxJ");
const LOCAL_NOOP_SHIM_PROGRAM = "FwDChsHWLwbhTiYQ4Sum5mjVWswECi9cmrA11GUFUuxi";
const DEPOSIT_AMOUNT_SATS = 100_000_000;
// Active bridge config: flat 10 DOGE + 1% on the deposit; 100 DOGE -> 89 DOGE net mint.
const DEPOSIT_FLAT_FEE_SATS = 10_000_000;
const DEPOSIT_FEE_NUM = 1;
const DEPOSIT_FEE_DEN = 100;
const EXPECTED_NET_MINT_SATS =
    DEPOSIT_AMOUNT_SATS - DEPOSIT_FLAT_FEE_SATS - Math.floor((DEPOSIT_AMOUNT_SATS * DEPOSIT_FEE_NUM) / DEPOSIT_FEE_DEN);
const BURN_AMOUNT_SATS = 50_000_000;
// The launcher initializes this E2E from bridge-config/doge_config.json, whose
// withdrawal fee is 10,000,000 sats (0.1 DOGE) plus 1%. Keep these explicit so the request
// records the fee result from the configuration that created it, rather than
// reconstructing historical net amounts from mutable on-chain configuration.
const WITHDRAWAL_FLAT_FEE_SATS = 10_000_000;
const WITHDRAWAL_FEE_NUM = 1;
const WITHDRAWAL_FEE_DEN = 100;
const EXPECTED_NET_WITHDRAWAL_SATS = calculateWithdrawalNetAmountSats(
    BURN_AMOUNT_SATS,
    WITHDRAWAL_FLAT_FEE_SATS,
    WITHDRAWAL_FEE_NUM,
    WITHDRAWAL_FEE_DEN,
);
const FUNDING_FEE_RESERVE_SATS = 1_000_000;
const FUNDING_ARTIFACT_SCHEMA = "psy-doge-full-e2e-funding-v1";
const FUNDING_BLOCKS = 110;
const FUNDING_MIN_CONFIRMATIONS = 100;
const PIPELINE_REQUIRED_CONFIRMATIONS = 1;
// From tip H-1: mine H with the deposit, process proof H+C to finalize H, then
// mine C more blocks so the pipeline's finalized tip reaches H+C.
const DEPOSIT_PIPELINE_BLOCKS_TO_MINE = 1 + (2 * PIPELINE_REQUIRED_CONFIRMATIONS);
const INSTRUCTION_REQUEST_WITHDRAWAL = 2;
const REQUEST_WITHDRAWAL_INSTRUCTION_DATA_SIZE = 48;
const WITHDRAWAL_ADDRESS_TYPE_P2SH = 1;
const BRIDGE_TIP_HEIGHT_OFFSET = 68;
const BRIDGE_FINALIZED_HEIGHT_OFFSET = 268;
const BRIDGE_FINALIZED_AUTO_CLAIM_INDEX_OFFSET = 264;
const MAX_CAPTURE_CHARS = 16_384;
const GROTH16_PROOF_SIZE = 356;
const PUBLIC_VALUES_SIZE = 32;
const EXPECTED_BLOCK_VK = "0x00a46ec348b525eea327ac89a090b17c44dab7e399a1d9fa4668c52cba1ba672";
const MANAGER_QUORUM_M = 5;
const MANAGER_QUORUM_N = 7;
const DISC_17_FINALIZE = 17;
const PENDING_WITHDRAWAL_STATUS_FINALIZED = 2;

function usage(): string {
    return `Dogecoin -> Solana -> Dogecoin local E2E orchestrator (Bun)

Usage:
  bun integration/e2e/e2e_test.ts --dry-run [--profile real-noop]
  bun integration/e2e/e2e_test.ts --full-live-regtest [--profile real-noop] [--keep-services]
  bun integration/e2e/e2e_test.ts --self-test

Options:
  --profile <name>          real-noop (default) or initialized-noop
  --dry-run                 Non-destructive dry-plan only; zero chain side effects; completed=false
  --full-live-regtest       Explicit gate for full live regtest: manager signing + broadcast +
                            Dogecoin confirmation + disc-17 confirmed finalize. Required for completed=true.
  --keep-services           Leave locSetupDoge-owned daemon services running after the test
  --self-test               Run source-level block-proof / confirmed-finalize validation tests and exit
  -h, --help                Show this help

Release-only contracts:
  * The single block-transition SP1 Groth16 proof must be exactly ${GROTH16_PROOF_SIZE} non-zero bytes with PV ${PUBLIC_VALUES_SIZE}B.
  * Block pipeline evidence is read from ${BLOCK_PROOF_LATEST_PATH} (content-addressed per-height dir).
  * Withdrawal evidence is non-ZK and must include atomic authorize/VAA, 5-of-7 manager quorum,
    signed transaction, live broadcast/confirmation, and permissionless disc-17 finalize.
  * Deposit txid is taken from deposit evidence — never mempool[0].
  * Funding is prepared before the dynamic bridge checkpoint and read from ${FUNDING_ARTIFACT_PATH}.
  * --legacy-ibc dummy Redis sandbox is not used.
  * completed=true only after the block proof, on-chain mint/burn, live Dogecoin confirmation, and disc-17 finalize succeed.
  * Failure paths always write completed=false.

Local preflight for --full-live-regtest requires built dogecoind, dogecoin-cli, and electrs-doge
because locSetupDoge is invoked with --no-build. Evidence is always written to ${EVIDENCE_PATH}.`;
}

function parseOptions(): Options | null {
    const { values } = parseArgs({
        args: Bun.argv.slice(2),
        strict: true,
        allowPositionals: false,
        options: {
            help: { type: "boolean", short: "h" },
            profile: { type: "string", default: "real-noop" },
            "dry-run": { type: "boolean" },
            "full-live-regtest": { type: "boolean" },
            "keep-services": { type: "boolean" },
            "self-test": { type: "boolean" },
        },
    });
    if (values.help) {
        console.log(usage());
        return null;
    }
    if (values.profile !== "real-noop" && values.profile !== "initialized-noop") {
        throw new Error(`--profile must be real-noop or initialized-noop, got '${values.profile}'`);
    }
    const dryRun = Boolean(values["dry-run"]);
    const fullLiveRegtest = Boolean(values["full-live-regtest"]);
    const selfTest = Boolean(values["self-test"]);
    if (selfTest && (dryRun || fullLiveRegtest)) {
        throw new Error("--self-test cannot be combined with --dry-run or --full-live-regtest");
    }
    if (dryRun && fullLiveRegtest) {
        throw new Error("--dry-run and --full-live-regtest are mutually exclusive");
    }
    if (!selfTest && !dryRun && !fullLiveRegtest) {
        throw new Error("Choose exactly one of --dry-run, --full-live-regtest, or --self-test");
    }
    return {
        profile: values.profile,
        dryRun,
        fullLiveRegtest,
        keepServices: Boolean(values["keep-services"]),
        selfTest,
    };
}

function isObject(value: unknown): value is JsonObject {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}

function executable(candidates: Array<string | undefined>): string | null {
    for (const candidate of candidates) {
        if (!candidate) continue;
        const resolved = candidate.includes(path.sep) ? path.resolve(candidate) : Bun.which(candidate);
        if (!resolved) continue;
        try {
            fs.accessSync(resolved, fs.constants.X_OK);
            return resolved;
        } catch {
            // Try the next candidate.
        }
    }
    return null;
}

function preflightLocalBinaries(): JsonObject {
    const dogecoind = executable([
        process.env.DOGECOIND,
        "dogecoind",
        path.join(DOGECOIN_REPO, "src/dogecoind"),
        path.join(DOGECOIN_REPO, "build/src/dogecoind"),
    ]);
    const dogecoinCli = executable([
        process.env.DOGECOIN_CLI,
        "dogecoin-cli",
        path.join(DOGECOIN_REPO, "src/dogecoin-cli"),
        path.join(DOGECOIN_REPO, "build/src/dogecoin-cli"),
    ]);
    const electrs = executable([
        process.env.ELECTRS_DOGE,
        "electrs-doge",
        path.join(ELECTRS_REPO, "target/release/electrs"),
    ]);
    const missing = [
        dogecoind ? null : "dogecoind",
        dogecoinCli ? null : "dogecoin-cli",
        electrs ? null : "electrs-doge",
    ].filter((name): name is string => name !== null);
    if (missing.length > 0) {
        throw new Error(
            `Local regtest requires built ${missing.join(", ")} binaries because the launcher is invoked with --no-build.\n` +
            `Build Dogecoin Core:\n  cd ${DOGECOIN_REPO}\n  ./autogen.sh\n  ./configure --without-gui --disable-tests --disable-bench\n  make src/dogecoind src/dogecoin-cli\n` +
            `Build electrs-doge:\n  cd ${ELECTRS_REPO}\n  cargo build --release --bin electrs\n` +
            "The current deposit/withdraw Rust CLIs are regtest-only; --profile testnet cannot run this complete flow until their network/WIF handling is retargeted.",
        );
    }
    return { dogecoind, dogecoinCli, electrs, docker: null, legacyIbc: false };
}

function requiredString(object: JsonObject, key: string, source: string): string {
    const value = object[key];
    if (typeof value !== "string" || value.length === 0) {
        throw new Error(`${source} is missing non-empty string '${key}'`);
    }
    return value;
}

function requiredNumber(object: JsonObject, key: string, source: string): number {
    const value = object[key];
    if (typeof value !== "number" || !Number.isFinite(value)) {
        throw new Error(`${source} is missing finite number '${key}'`);
    }
    return value;
}

function optionalObject(object: JsonObject, key: string): JsonObject | null {
    const value = object[key];
    return isObject(value) ? value : null;
}

function readJsonObject(filePath: string): JsonObject {
    let parsed: unknown;
    try {
        parsed = JSON.parse(fs.readFileSync(filePath, "utf8"));
    } catch (error) {
        throw new Error(`Cannot read JSON ${filePath}: ${error instanceof Error ? error.message : String(error)}`);
    }
    if (!isObject(parsed)) throw new Error(`${filePath} must contain a JSON object`);
    return parsed;
}

function readFundingArtifact(filePath: string): FundingArtifact {
    const mode = fs.statSync(filePath).mode & 0o777;
    assertCondition(mode === 0o600, `${filePath} must have mode 600, got ${mode.toString(8)}`);
    const artifact = readJsonObject(filePath);
    assertCondition(requiredString(artifact, "schema", filePath) === FUNDING_ARTIFACT_SCHEMA, `${filePath} has an unsupported schema`);
    assertCondition(requiredString(artifact, "network", filePath) === "regtest", `${filePath} must target regtest`);
    const address = requiredString(artifact, "address", filePath);
    const wif = requiredString(artifact, "wif", filePath);
    const txid = requiredString(artifact, "txid", filePath);
    assertCondition(/^[0-9a-f]{64}$/i.test(txid), `${filePath}.txid must be 64 hex characters`);
    const vout = requiredNumber(artifact, "vout", filePath);
    const value = requiredNumber(artifact, "value", filePath);
    const blockHeight = requiredNumber(artifact, "blockHeight", filePath);
    const confirmations = requiredNumber(artifact, "confirmations", filePath);
    const minedBlocks = requiredNumber(artifact, "minedBlocks", filePath);
    const dogecoinTipHeight = requiredNumber(artifact, "dogecoinTipHeight", filePath);
    const electrsTipHeight = requiredNumber(artifact, "electrsTipHeight", filePath);
    for (const [name, number] of Object.entries({ vout, value, blockHeight, confirmations, minedBlocks, dogecoinTipHeight, electrsTipHeight })) {
        assertCondition(Number.isSafeInteger(number) && number >= 0, `${filePath}.${name} must be a non-negative safe integer`);
    }
    assertCondition(value >= DEPOSIT_AMOUNT_SATS + FUNDING_FEE_RESERVE_SATS, `${filePath}.value is insufficient for deposit plus fee reserve`);
    assertCondition(confirmations >= FUNDING_MIN_CONFIRMATIONS, `${filePath}.confirmations must be at least ${FUNDING_MIN_CONFIRMATIONS}`);
    assertCondition(minedBlocks === FUNDING_BLOCKS, `${filePath}.minedBlocks must equal ${FUNDING_BLOCKS}`);
    assertCondition(dogecoinTipHeight === electrsTipHeight, `${filePath} Dogecoin/Electrs tips differ`);
    assertCondition(confirmations === electrsTipHeight - blockHeight + 1, `${filePath}.confirmations is inconsistent with its indexed tip and block height`);
    return { address, wif, txid, vout, value, blockHeight, confirmations, minedBlocks, dogecoinTipHeight, electrsTipHeight };
}

function logPhase(number: number, name: string): void {
    console.log(`\n=== Phase ${number}: ${name} ===`);
}

function assertCondition(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(message);
}

function calculateWithdrawalNetAmountSats(
    grossAmountSats: number,
    flatFeeSats: number,
    feeRateNumerator: number,
    feeRateDenominator: number,
): number {
    assertCondition(Number.isSafeInteger(grossAmountSats) && grossAmountSats > 0, `Invalid gross withdrawal amount ${grossAmountSats}`);
    assertCondition(Number.isSafeInteger(flatFeeSats) && flatFeeSats >= 0, `Invalid withdrawal flat fee ${flatFeeSats}`);
    assertCondition(Number.isSafeInteger(feeRateNumerator) && feeRateNumerator >= 0, `Invalid withdrawal fee numerator ${feeRateNumerator}`);
    assertCondition(Number.isSafeInteger(feeRateDenominator) && feeRateDenominator > 0, `Invalid withdrawal fee denominator ${feeRateDenominator}`);

    const gross = BigInt(grossAmountSats);
    const proportionalFee = (gross * BigInt(feeRateNumerator)) / BigInt(feeRateDenominator);
    const totalFee = BigInt(flatFeeSats) + proportionalFee;
    assertCondition(totalFee > 0n, "Withdrawal fee must be nonzero");
    assertCondition(totalFee < gross, `Withdrawal fee ${totalFee} must leave a positive net amount from gross ${gross}`);
    const net = gross - totalFee;
    assertCondition(net > 0n && net <= BigInt(Number.MAX_SAFE_INTEGER), `Invalid net withdrawal amount ${net}`);
    return Number(net);
}

function depositPipelineHeights(tipBeforeDeposit: number): {
    depositHeight: number;
    finalizationProofHeight: number;
    requiredTipHeight: number;
} {
    assertCondition(Number.isSafeInteger(tipBeforeDeposit) && tipBeforeDeposit >= 0, `Invalid pre-deposit tip height ${tipBeforeDeposit}`);
    const depositHeight = tipBeforeDeposit + 1;
    const finalizationProofHeight = depositHeight + PIPELINE_REQUIRED_CONFIRMATIONS;
    const requiredTipHeight = finalizationProofHeight + PIPELINE_REQUIRED_CONFIRMATIONS;
    assertCondition(
        requiredTipHeight - tipBeforeDeposit === DEPOSIT_PIPELINE_BLOCKS_TO_MINE,
        "Deposit pipeline mining formula is inconsistent",
    );
    return { depositHeight, finalizationProofHeight, requiredTipHeight };
}

function compactOutput(value: string): string {
    if (value.length <= MAX_CAPTURE_CHARS) return value;
    return `${value.slice(0, MAX_CAPTURE_CHARS)}\n...[truncated ${value.length - MAX_CAPTURE_CHARS} chars]`;
}

function writeEvidence(evidence: Evidence): void {
    const temporary = `${EVIDENCE_PATH}.${process.pid}.tmp`;
    fs.writeFileSync(temporary, `${JSON.stringify(evidence, null, 2)}\n`, { mode: 0o600 });
    fs.renameSync(temporary, EVIDENCE_PATH);
}

function sha256Hex(bytes: Uint8Array | Buffer | string): string {
    return createHash("sha256").update(bytes).digest("hex");
}

function fileSha256Hex(filePath: string): string {
    return sha256Hex(fs.readFileSync(filePath));
}

function bytesNonZero(bytes: Uint8Array): boolean {
    return bytes.some((byte) => byte !== 0);
}

function normalizeVk(value: string): string {
    const trimmed = value.trim().toLowerCase();
    return trimmed.startsWith("0x") ? trimmed : `0x${trimmed}`;
}

function launcherCommand(profile: Profile): string[] {
    return [
        "bun",
        LOC_SETUP,
        "--profile",
        profile,
        "--initialize",
        "--create-users",
        "--dogecoind",
        "--prepare-full-e2e-funding",
        FUNDING_ARTIFACT_PATH,
        "--block-sender",
        "--ibc-pipeline",
        "--manager-service",
        "--no-build",
    ];
}

function withdrawalCliArgs(bridge: BridgeOutput, fullLive: boolean): string[] {
    const args = [
        "--request-index", "0",
        "--solana-rpc-url", SOLANA_RPC,
        "--operator-keypair", bridge.operatorKeypair,
        "--payer-keypair", bridge.payerKeypair,
        "--operator-store", bridge.operatorStore,
        "--manager-service-url", MANAGER_SERVICE_URL,
        "--electrs-url", ELECTRS_URL,
        "--doge-rpc-url", DOGE_RPC_URL,
        "--doge-rpc-user", "doge",
        "--doge-rpc-password", "doge",
        "--wormhole-shim-program", LOCAL_NOOP_SHIM_PROGRAM,
        "--evidence-path", WITHDRAWAL_EVIDENCE_PATH,
        "--manager-signing-enabled",
    ];
    if (fullLive) args.push("--broadcast-enabled");
    return args;
}

export type CompletionValidation = {
    completedEligible: boolean;
    block: JsonObject;
    withdrawal: JsonObject;
    evidence: JsonObject;
    reasons: string[];
};

/** Single-ZK evidence bundle: content-addressed block proof plus non-ZK withdrawal lifecycle. */
export function buildCompletionEvidence(block: JsonObject, withdrawal: JsonObject): JsonObject {
    const blockVerified = block.verified === true;
    const withdrawalVerified = withdrawal.verified === true;
    return {
        schema: "doge-single-block-zk-completion-v1",
        blockProof: blockVerified ? {
            kind: "block-groth16",
            contentAddressed: true,
            height: block.height ?? null,
            contentSha256: block.contentSha256 ?? null,
            evidenceDir: block.evidenceDir ?? null,
            manifestPath: block.manifestPath ?? null,
            manifestSha256: block.manifestSha256 ?? null,
            status: block.status ?? null,
            proofPath: block.proofPath ?? null,
            proofBytes: block.proofBytes ?? null,
            proofSha256Hex: block.proofSha256Hex ?? null,
            publicValuesPath: block.publicValuesPath ?? null,
            publicValuesBytes: block.publicValuesBytes ?? null,
            publicValuesSha256Hex: block.publicValuesSha256Hex ?? null,
            vkPath: block.vkPath ?? null,
            vkSha256Hex: block.vkSha256Hex ?? null,
            vkHash: block.vkHash ?? null,
            elfPath: block.elfPath ?? null,
            elfSha256Hex: block.elfSha256Hex ?? null,
            inputsPath: block.inputsPath ?? null,
            inputsSha256Hex: block.inputsSha256Hex ?? null,
            verified: true,
        } : { kind: "block-groth16", verified: false, contentAddressed: false },
        withdrawal: withdrawalVerified ? {
            kind: "atomic-output-only",
            authorizeSignature: withdrawal.authorizeSignature ?? null,
            vaaHashHex: withdrawal.vaaHashHex ?? null,
            vaaSequence: withdrawal.vaaSequence ?? null,
            managerQuorum: withdrawal.managerQuorum ?? null,
            signatureCount: withdrawal.signatureCount ?? null,
            signedRawBytes: withdrawal.signedRawBytes ?? null,
            signedRawSha256Hex: withdrawal.signedRawSha256Hex ?? null,
            finalTxidInternalHex: withdrawal.finalTxidInternalHex ?? null,
            confirmationOk: withdrawal.confirmationOk ?? false,
            confirmationBlockHeight: withdrawal.confirmationBlockHeight ?? null,
            confirmationTxIndexInBlock: withdrawal.confirmationTxIndexInBlock ?? null,
            confirmationConfirmations: withdrawal.confirmationConfirmations ?? null,
            transactionMerkleBranchHex: withdrawal.transactionMerkleBranchHex ?? null,
            finalizeOk: withdrawal.finalizeOk ?? false,
            finalizeDiscriminator: withdrawal.finalizeDiscriminator ?? null,
            finalizeSignature: withdrawal.finalizeSignature ?? null,
            stage: withdrawal.stage ?? null,
            completed: withdrawal.completed ?? false,
            verified: true,
        } : { kind: "atomic-output-only", verified: false },
        singleZk: blockVerified,
        noWithdrawalZk: true,
    };
}

export function bundleCompletionEvidence(validation: CompletionValidation, extra: JsonObject = {}): JsonObject {
    return {
        completedEligible: validation.completedEligible,
        reasons: validation.reasons,
        evidence: validation.evidence,
        block: validation.block,
        withdrawal: validation.withdrawal,
        ...extra,
    };
}

function readArtifactEntry(artifacts: JsonObject, key: string, source: string): JsonObject {
    const entry = artifacts[key];
    if (!isObject(entry)) throw new Error(`${source}.artifacts.${key} must be an object`);
    return entry;
}

function validateBinaryArtifact(
    entry: JsonObject,
    source: string,
    expectedSize: number,
    requireNonZero: boolean,
): { path: string; sha256: string; size: number; bytes: Uint8Array } {
    const artifactPath = requiredString(entry, "path", source);
    const declaredSha = requiredString(entry, "sha256", source).toLowerCase();
    const declaredSize = requiredNumber(entry, "size", source);
    assertCondition(fs.existsSync(artifactPath), `${source} path missing: ${artifactPath}`);
    const bytes = new Uint8Array(fs.readFileSync(artifactPath));
    assertCondition(bytes.length === expectedSize, `${source} must be ${expectedSize} bytes, got ${bytes.length}`);
    assertCondition(declaredSize === expectedSize, `${source}.size must be ${expectedSize}, got ${declaredSize}`);
    const actualSha = sha256Hex(bytes);
    assertCondition(actualSha === declaredSha, `${source} sha256 mismatch: manifest ${declaredSha} vs file ${actualSha}`);
    if (requireNonZero) {
        assertCondition(bytesNonZero(bytes), `${source} bytes are all zeros`);
    }
    return { path: artifactPath, sha256: actualSha, size: bytes.length, bytes };
}

/** Validate block pipeline latest.json + content-addressed artifacts. */
export function validateBlockProofEvidence(
    manifest: JsonObject,
    options: { requireDeposit?: boolean; requireMinted?: boolean } = {},
): JsonObject {
    const requireDeposit = options.requireDeposit ?? true;
    const requireMinted = options.requireMinted ?? false;
    const source = "blockProof.latest";
    const height = requiredNumber(manifest, "height", source);
    const contentSha = requiredString(manifest, "content_sha256", source).toLowerCase();
    const evidenceDir = requiredString(manifest, "evidence_dir", source);
    assertCondition(fs.existsSync(evidenceDir), `${source}.evidence_dir missing: ${evidenceDir}`);
    assertCondition(
        evidenceDir.includes(`height-${height}`) && evidenceDir.toLowerCase().includes(contentSha),
        `${source}.evidence_dir is not content-addressed for height ${height}: ${evidenceDir}`,
    );
    const artifacts = optionalObject(manifest, "artifacts");
    assertCondition(artifacts, `${source}.artifacts is required`);

    const proof = validateBinaryArtifact(readArtifactEntry(artifacts, "proof", source), `${source}.proof`, GROTH16_PROOF_SIZE, true);
    const publicValues = validateBinaryArtifact(
        readArtifactEntry(artifacts, "public_values", source),
        `${source}.public_values`,
        PUBLIC_VALUES_SIZE,
        true,
    );
    const vk = validateBinaryArtifact(readArtifactEntry(artifacts, "vk", source), `${source}.vk`, 32, true);
    const elf = readArtifactEntry(artifacts, "elf", source);
    const elfPath = requiredString(elf, "path", `${source}.elf`);
    const elfShaFromArtifact = requiredString(elf, "sha256", `${source}.elf`).toLowerCase();
    assertCondition(fs.existsSync(elfPath), `${source}.elf path missing: ${elfPath}`);
    const elfFileSha = fileSha256Hex(elfPath);
    assertCondition(elfFileSha === elfShaFromArtifact, `${source}.elf sha256 mismatch`);
    const elfShaTop = typeof manifest.elf_sha256 === "string" ? manifest.elf_sha256.toLowerCase() : elfShaFromArtifact;
    assertCondition(elfShaTop === elfFileSha, `${source}.elf_sha256 mismatch`);
    const inputs = readArtifactEntry(artifacts, "inputs", source);
    const inputsPath = requiredString(inputs, "path", `${source}.inputs`);
    const inputsSha = requiredString(inputs, "sha256", `${source}.inputs`).toLowerCase();
    assertCondition(fs.existsSync(inputsPath), `${source}.inputs path missing: ${inputsPath}`);
    assertCondition(fileSha256Hex(inputsPath) === inputsSha, `${source}.inputs sha256 mismatch`);

    const vkHexFromFile = `0x${Buffer.from(vk.bytes).toString("hex")}`;
    const manifestVk = typeof manifest.vk_hash === "string"
        ? normalizeVk(manifest.vk_hash)
        : typeof manifest.blockVk === "string"
            ? normalizeVk(manifest.blockVk)
            : vkHexFromFile;
    assertCondition(
        normalizeVk(manifestVk) === normalizeVk(EXPECTED_BLOCK_VK) || normalizeVk(vkHexFromFile) === normalizeVk(EXPECTED_BLOCK_VK),
        `${source} VK mismatch: expected ${EXPECTED_BLOCK_VK}, got manifest=${manifestVk} file=${vkHexFromFile}`,
    );

    const depositCount = typeof manifest.deposit_count === "number" ? manifest.deposit_count : 0;
    const mintedAmountSats = typeof manifest.minted_amount_sats === "number" ? manifest.minted_amount_sats : null;
    const witnessDepositCount = typeof manifest.witness_deposit_count === "number" ? manifest.witness_deposit_count : 0;
    const witnessMintedAmountSats = typeof manifest.witness_minted_amount_sats === "number" ? manifest.witness_minted_amount_sats : null;
    const sourceWitnessHeight = typeof manifest.source_witness_height === "number" ? manifest.source_witness_height : null;
    const finalizedSourceHeight = typeof manifest.finalized_source_height === "number" ? manifest.finalized_source_height : null;
    const autoClaimStart = typeof manifest.auto_claim_start_index === "number" ? manifest.auto_claim_start_index : null;
    const autoClaimEnd = typeof manifest.auto_claim_end_index === "number" ? manifest.auto_claim_end_index : null;
    if (requireDeposit) {
        // confirmations=1: deposit is witnessed at height H, but deposit_count/minted_amount_sats
        // become nonzero on proof H+1 when H finalizes and mint/TXO buffers commit.
        // Accept either finalized-buffer fields or current-witness fields.
        const finalizedDeposit = depositCount > 0 && (mintedAmountSats === null || mintedAmountSats > 0);
        const witnessDeposit = witnessDepositCount > 0 && (witnessMintedAmountSats === null || witnessMintedAmountSats > 0);
        assertCondition(
            finalizedDeposit || witnessDeposit,
            `${source} missing deposit claim: deposit_count=${depositCount} minted_amount_sats=${mintedAmountSats} witness_deposit_count=${witnessDepositCount} witness_minted_amount_sats=${witnessMintedAmountSats}`,
        );
        if (finalizedDeposit && autoClaimStart !== null && autoClaimEnd !== null) {
            assertCondition(autoClaimEnd > autoClaimStart, `${source} auto_claim_end_index must exceed auto_claim_start_index`);
        }
    }

    const submissionSignature = typeof manifest.submission_signature === "string" ? manifest.submission_signature : null;
    assertCondition(Boolean(submissionSignature), `${source}.submission_signature is required`);
    const status = typeof manifest.status === "string" ? manifest.status : null;
    assertCondition(
        status === null || status === "submitted" || status === "proof_generated" || status === "minted",
        `${source}.status unexpected: ${status}`,
    );
    const bufferUploadCompleted = manifest.buffer_upload_completed === true;
    const mintBuffer = typeof manifest.mint_buffer === "string" ? manifest.mint_buffer : null;
    const txoBuffer = typeof manifest.txo_buffer === "string" ? manifest.txo_buffer : null;
    const mintBufferBump = typeof manifest.mint_buffer_bump === "number" ? manifest.mint_buffer_bump : null;
    const txoBufferBump = typeof manifest.txo_buffer_bump === "number" ? manifest.txo_buffer_bump : null;
    const mintGroupSignatures = Array.isArray(manifest.mint_group_signatures)
        ? manifest.mint_group_signatures.filter((value): value is string => typeof value === "string" && value.length > 0)
        : [];
    const mintGroupsProcessed = typeof manifest.mint_groups_processed === "number" ? manifest.mint_groups_processed : 0;
    const totalMintsProcessed = typeof manifest.total_mints_processed === "number" ? manifest.total_mints_processed : 0;
    if (requireMinted) {
        assertCondition(status === "minted", `${source}.status must be minted for live gate, got ${status}`);
        assertCondition(bufferUploadCompleted, `${source}.buffer_upload_completed must be true for live mint gate`);
        assertCondition(typeof mintBuffer === "string" && mintBuffer.length > 0, `${source}.mint_buffer required for live mint gate`);
        assertCondition(typeof txoBuffer === "string" && txoBuffer.length > 0, `${source}.txo_buffer required for live mint gate`);
        assertCondition(mintBufferBump !== null, `${source}.mint_buffer_bump required for live mint gate`);
        assertCondition(txoBufferBump !== null, `${source}.txo_buffer_bump required for live mint gate`);
        assertCondition(depositCount > 0, `${source}.deposit_count must be > 0 for live mint gate`);
        assertCondition(mintedAmountSats !== null && mintedAmountSats > 0, `${source}.minted_amount_sats must be > 0 for live mint gate`);
        assertCondition(mintGroupSignatures.length > 0, `${source}.mint_group_signatures required for live mint gate`);
        assertCondition(mintGroupsProcessed > 0, `${source}.mint_groups_processed must be > 0 for live mint gate`);
        assertCondition(
            totalMintsProcessed === depositCount,
            `${source}.total_mints_processed (${totalMintsProcessed}) must equal deposit_count (${depositCount})`,
        );
        // Content-hashed mint artifacts (fixed keys after collision fix).
        for (const key of ["pending_mints", "pending_mints_json", "txo_indices", "txo_indices_json"]) {
            if (artifacts[key] === undefined) continue;
            const entry = readArtifactEntry(artifacts, key, source);
            const artPath = requiredString(entry, "path", `${source}.${key}`);
            const artSha = requiredString(entry, "sha256", `${source}.${key}`).toLowerCase();
            assertCondition(fs.existsSync(artPath), `${source}.${key} path missing: ${artPath}`);
            assertCondition(fileSha256Hex(artPath) === artSha, `${source}.${key} sha256 mismatch`);
        }
    }
    const manifestSha = typeof manifest.manifest_sha256 === "string" ? manifest.manifest_sha256.toLowerCase() : null;
    const manifestPath = typeof manifest.manifest_path === "string" ? manifest.manifest_path : null;

    return {
        height,
        status,
        contentSha256: contentSha,
        manifestSha256: manifestSha,
        evidenceDir,
        manifestPath,
        depositCount,
        mintedAmountSats,
        witnessDepositCount,
        witnessMintedAmountSats,
        sourceWitnessHeight,
        finalizedSourceHeight,
        bufferUploadCompleted,
        mintBuffer,
        txoBuffer,
        mintBufferBump,
        txoBufferBump,
        mintGroupSignatures,
        mintGroupsProcessed,
        totalMintsProcessed,
        autoClaimStartIndex: autoClaimStart,
        autoClaimEndIndex: autoClaimEnd,
        proofPath: proof.path,
        proofBytes: proof.size,
        proofSha256Hex: proof.sha256,
        publicValuesPath: publicValues.path,
        publicValuesBytes: publicValues.size,
        publicValuesSha256Hex: publicValues.sha256,
        publicValuesHex: Buffer.from(publicValues.bytes).toString("hex"),
        vkPath: vk.path,
        vkSha256Hex: vk.sha256,
        vkHash: normalizeVk(manifestVk),
        elfPath,
        elfSha256Hex: elfFileSha,
        inputsPath,
        inputsSha256Hex: inputsSha,
        submissionSignature,
        idempotencyKey: typeof manifest.idempotency_key === "string" ? manifest.idempotency_key : null,
        verified: true,
    };
}

/** Validate non-ZK atomic withdrawal evidence for full-live success. */
export function validateWithdrawalEvidence(evidence: JsonObject, options: { requireFinalize: boolean }): JsonObject {
    const source = "withdrawalEvidence";
    const schema = requiredString(evidence, "schema", source);
    assertCondition(schema.startsWith("doge-local-process-withdrawal-"), `${source}.schema unexpected: ${schema}`);
    const completed = evidence.completed === true;
    const stage = typeof evidence.stage === "string" ? evidence.stage : null;

    const request = optionalObject(evidence, "request");
    const authorize = optionalObject(evidence, "authorize");
    const relay = optionalObject(evidence, "relay");
    const confirmation = optionalObject(evidence, "confirmation");
    const finalize = optionalObject(evidence, "finalize");
    assertCondition(request && authorize && relay, `${source} missing request/authorize/relay sections`);

    const authorizeSignature = requiredString(authorize, "signature", `${source}.authorize`);
    requiredString(authorize, "pendingPda", `${source}.authorize`);
    requiredString(authorize, "intentHashHex", `${source}.authorize`);
    requiredString(authorize, "utx0HashHex", `${source}.authorize`);
    requiredString(authorize, "unsignedTxHashHex", `${source}.authorize`);
    const requestStart = requiredNumber(authorize, "requestStart", `${source}.authorize`);
    const requestEnd = requiredNumber(authorize, "requestEnd", `${source}.authorize`);
    assertCondition(requestEnd === requestStart + 1, `${source}.authorize must cover exactly one request`);
    requiredNumber(authorize, "managerSetIndex", `${source}.authorize`);
    requiredNumber(authorize, "inputCount", `${source}.authorize`);
    assertCondition(authorize.noopVerified === true, `${source}.authorize.noopVerified must be true`);
    const sequence = requiredNumber(authorize, "sequence", `${source}.authorize`);

    const signatureCount = requiredNumber(relay, "signatureCount", `${source}.relay`);
    const quorum = requiredNumber(relay, "quorum", `${source}.relay`);
    const managerTotal = requiredNumber(relay, "managerTotal", `${source}.relay`);
    assertCondition(quorum === MANAGER_QUORUM_M, `${source}.relay.quorum must be ${MANAGER_QUORUM_M}`);
    assertCondition(managerTotal === MANAGER_QUORUM_N, `${source}.relay.managerTotal must be ${MANAGER_QUORUM_N}`);
    assertCondition(signatureCount >= MANAGER_QUORUM_M, `${source}.relay.signatureCount ${signatureCount} < ${MANAGER_QUORUM_M}`);
    const signerIndices = relay.signerIndices;
    assertCondition(Array.isArray(signerIndices), `${source}.relay.signerIndices must be an array`);
    assertCondition(signerIndices.length === signatureCount, `${source}.relay.signerIndices length must match signatureCount`);
    const uniqueSignerIndices = new Set<number>();
    for (const signerIndex of signerIndices) {
        assertCondition(Number.isInteger(signerIndex), `${source}.relay.signerIndices must contain integers`);
        assertCondition(signerIndex >= 0 && signerIndex < MANAGER_QUORUM_N, `${source}.relay signer index ${signerIndex} outside 0..${MANAGER_QUORUM_N - 1}`);
        uniqueSignerIndices.add(signerIndex);
    }
    assertCondition(uniqueSignerIndices.size === signatureCount, `${source}.relay.signerIndices must be unique`);
    requiredString(relay, "vaaHashHex", `${source}.relay`);
    const vaaSequence = requiredNumber(relay, "vaaSequence", `${source}.relay`);
    assertCondition(vaaSequence === sequence, `${source}.relay.vaaSequence does not match authorize.sequence`);
    const signedRawBytes = requiredNumber(relay, "signedRawBytes", `${source}.relay`);
    assertCondition(signedRawBytes > 0, `${source}.relay.signedRawBytes must be > 0`);
    requiredString(relay, "signedRawSha256Hex", `${source}.relay`);
    requiredString(relay, "finalTxidInternalHex", `${source}.relay`);
    if (options.requireFinalize) assertCondition(relay.broadcast === true, `${source}.relay.broadcast must be true for completed flow`);

    const recipientAmountSats = requiredNumber(request, "recipientAmountSats", `${source}.request`);
    assertCondition(recipientAmountSats > 0, `${source}.request.recipientAmountSats must be positive`);

    let confirmationOk = false;
    let finalizeOk = false;
    if (options.requireFinalize) {
        assertCondition(confirmation, `${source}.confirmation required for full-live finalize`);
        assertCondition(finalize, `${source}.finalize required for full-live disc-17 path`);
        assertCondition(confirmation.status === "CONFIRMED", `${source}.confirmation.status must be CONFIRMED`);
        requiredNumber(confirmation, "blockHeight", `${source}.confirmation`);
        requiredNumber(confirmation, "txIndexInBlock", `${source}.confirmation`);
        const confirmations = requiredNumber(confirmation, "confirmations", `${source}.confirmation`);
        assertCondition(confirmations >= 1, `${source}.confirmation.confirmations must be >= 1`);
        assertCondition(Array.isArray(confirmation.transactionMerkleBranchHex), `${source}.confirmation.transactionMerkleBranchHex must be an array`);
        confirmationOk = true;

        const discriminator = requiredNumber(finalize, "discriminator", `${source}.finalize`);
        assertCondition(discriminator === DISC_17_FINALIZE, `${source}.finalize.discriminator must be disc-17 (${DISC_17_FINALIZE})`);
        assertCondition(finalize.instruction === "finalize_confirmed_withdrawal", `${source}.finalize.instruction must be finalize_confirmed_withdrawal`);
        assertCondition(finalize.permissionless === true, `${source}.finalize.permissionless must be true`);
        assertCondition(finalize.finalizeConfirmed === true, `${source}.finalize.finalizeConfirmed must be true`);
        requiredString(finalize, "signature", `${source}.finalize`);
        requiredNumber(finalize, "slot", `${source}.finalize`);
        finalizeOk = true;
        assertCondition(completed, `${source}.completed must be true after confirmed finalize`);
        assertCondition(stage === "CONFIRMED_FINALIZED", `${source}.stage must be CONFIRMED_FINALIZED, got ${stage}`);
    } else {
        assertCondition(!completed, `${source}.completed must be false without full finalize`);
    }

    return {
        schema,
        completed,
        stage,
        requestIndex: typeof request.index === "number" ? request.index : 0,
        recipientAmountSats,
        grossAmountSats: typeof request.grossAmountSats === "number" ? request.grossAmountSats : null,
        authorizeSignature,
        sequence,
        vaaHashHex: relay.vaaHashHex,
        vaaSequence,
        managerQuorum: `${MANAGER_QUORUM_M}-of-${MANAGER_QUORUM_N}`,
        signatureCount,
        signedRawBytes,
        signedRawSha256Hex: relay.signedRawSha256Hex,
        finalTxidInternalHex: relay.finalTxidInternalHex,
        confirmationOk,
        confirmationBlockHeight: confirmation && typeof confirmation.blockHeight === "number" ? confirmation.blockHeight : null,
        confirmationTxIndexInBlock: confirmation && typeof confirmation.txIndexInBlock === "number" ? confirmation.txIndexInBlock : null,
        confirmationConfirmations: confirmation && typeof confirmation.confirmations === "number" ? confirmation.confirmations : null,
        transactionMerkleBranchHex: confirmation?.transactionMerkleBranchHex ?? null,
        finalizeOk,
        finalizeDiscriminator: finalize && typeof finalize.discriminator === "number" ? finalize.discriminator : null,
        finalizeSignature: finalize && typeof finalize.signature === "string" ? finalize.signature : null,
        verified: true,
    };
}

/** completed=true requires the block proof and the full non-ZK withdrawal lifecycle. */
export function evaluateCompletion(input: {
    blockManifest: JsonObject | null;
    withdrawalEvidence: JsonObject | null;
    mintSats: number | null;
    burnSats: number | null;
    requireFullLive: boolean;
}): CompletionValidation {
    const reasons: string[] = [];
    let block: JsonObject = { verified: false };
    let withdrawal: JsonObject = { verified: false };

    if (!input.blockManifest) {
        reasons.push("missing block proof pipeline artifact (latest.json)");
    } else {
        try {
            block = validateBlockProofEvidence(input.blockManifest, { requireDeposit: true, requireMinted: input.requireFullLive });
        } catch (error) {
            reasons.push(`block proof validation failed: ${error instanceof Error ? error.message : String(error)}`);
        }
    }

    if (!input.withdrawalEvidence) {
        reasons.push("missing withdrawal evidence");
    } else {
        try {
            withdrawal = validateWithdrawalEvidence(input.withdrawalEvidence, { requireFinalize: input.requireFullLive });
        } catch (error) {
            reasons.push(`withdrawal evidence validation failed: ${error instanceof Error ? error.message : String(error)}`);
        }
    }

    if (input.requireFullLive) {
        if (input.mintSats !== EXPECTED_NET_MINT_SATS) reasons.push(`mint amount ${input.mintSats} != expected ${EXPECTED_NET_MINT_SATS}`);
        if (input.burnSats !== BURN_AMOUNT_SATS) reasons.push(`burn amount ${input.burnSats} != expected ${BURN_AMOUNT_SATS}`);
        if (block.verified !== true) reasons.push("block proof not verified");
        if (withdrawal.verified !== true) reasons.push("atomic withdrawal evidence not verified");
        if (withdrawal.confirmationOk !== true) reasons.push("live Dogecoin confirmation missing");
        if (withdrawal.finalizeOk !== true) reasons.push("permissionless disc-17 finalize missing");
        if (withdrawal.completed !== true) reasons.push("withdrawal evidence completed!=true");
        if (block.verified === true) {
            if (block.proofBytes !== GROTH16_PROOF_SIZE || typeof block.proofSha256Hex !== "string") reasons.push("block proof identity missing exact size/hash");
            if (typeof block.contentSha256 !== "string" || typeof block.evidenceDir !== "string") reasons.push("block proof content-addressed identity missing");
            if (typeof block.elfSha256Hex !== "string" || typeof block.inputsSha256Hex !== "string") reasons.push("block proof ELF/input identity missing");
            if (typeof block.vkHash !== "string") reasons.push("block proof VK identity missing");
        }
    } else {
        reasons.push("full-live-regtest gate not enabled; completed remains false");
    }

    const evidence = buildCompletionEvidence(block, withdrawal);
    return { completedEligible: reasons.length === 0, block, withdrawal, evidence, reasons };
}

async function readStream(stream: ReadableStream<Uint8Array> | null): Promise<string> {
    return stream ? await new Response(stream).text() : "";
}

async function runCommand(bin: string, args: string[], options: { cwd?: string; env?: Record<string, string> } = {}): Promise<CommandResult> {
    const command = [bin, ...args];
    console.log(`$ ${command.map(shellQuote).join(" ")}`);
    const started = Date.now();
    const child = Bun.spawn(command, {
        cwd: options.cwd ?? IBC_REPO,
        env: { ...process.env, NO_PROXY: "localhost,127.0.0.1", no_proxy: "localhost,127.0.0.1", ...options.env },
        stdout: "pipe",
        stderr: "pipe",
    });
    const [stdout, stderr, exitCode] = await Promise.all([
        readStream(child.stdout),
        readStream(child.stderr),
        child.exited,
    ]);
    const result = { command, exitCode, stdout, stderr, durationMs: Date.now() - started };
    if (stdout.trim()) console.log(stdout.trim());
    if (stderr.trim()) console.error(stderr.trim());
    if (exitCode !== 0) {
        throw new Error(`Command failed with exit code ${exitCode}: ${command.map(shellQuote).join(" ")}\n${stderr || stdout}`);
    }
    return result;
}

function shellQuote(value: string): string {
    return /^[A-Za-z0-9_./:=,@+-]+$/.test(value) ? value : JSON.stringify(value);
}

async function waitFor<T>(description: string, timeoutMs: number, condition: () => Promise<T | null | undefined | false>, intervalMs = 1_000): Promise<T> {
    const started = Date.now();
    let lastError: unknown;
    while (Date.now() - started < timeoutMs) {
        try {
            const value = await condition();
            if (value !== null && value !== undefined && value !== false) return value;
        } catch (error) {
            lastError = error;
        }
        await Bun.sleep(intervalMs);
    }
    const suffix = lastError instanceof Error ? ` Last error: ${lastError.message}` : "";
    throw new Error(`Timed out after ${timeoutMs} ms waiting for ${description}.${suffix}`);
}

async function waitForLauncherReady<T>(
    launcher: Bun.Subprocess,
    description: string,
    timeoutMs: number,
    condition: () => Promise<T | null | undefined | false>,
): Promise<T> {
    const readiness = waitFor(description, timeoutMs, condition);
    const exited = launcher.exited.then((exitCode) => {
        throw new Error(`locSetupDoge exited before readiness with code ${exitCode}`);
    });
    return await Promise.race([readiness, exited]);
}

async function jsonRpc(url: string, method: string, params: unknown[], auth?: { user: string; password: string }): Promise<unknown> {
    const headers: Record<string, string> = { "content-type": "application/json" };
    if (auth) headers.authorization = `Basic ${Buffer.from(`${auth.user}:${auth.password}`).toString("base64")}`;
    const response = await fetch(url, {
        method: "POST",
        headers,
        body: JSON.stringify({ jsonrpc: "2.0", id: `${method}-${Date.now()}`, method, params }),
    });
    const text = await response.text();
    let body: unknown;
    try {
        body = JSON.parse(text);
    } catch {
        throw new Error(`${method} returned HTTP ${response.status} with invalid JSON: ${text}`);
    }
    if (!response.ok) throw new Error(`${method} returned HTTP ${response.status}: ${text}`);
    if (!isObject(body)) throw new Error(`${method} returned a non-object JSON-RPC response`);
    if (body.error !== null && body.error !== undefined) throw new Error(`${method} JSON-RPC error: ${JSON.stringify(body.error)}`);
    if (!("result" in body)) throw new Error(`${method} JSON-RPC response has no result`);
    return body.result;
}

async function dogeRpc(method: string, params: unknown[] = []): Promise<unknown> {
    return jsonRpc(DOGE_RPC_URL, method, params, { user: "doge", password: "doge" });
}

async function solanaRpc(method: string, params: unknown[] = []): Promise<unknown> {
    return jsonRpc(SOLANA_RPC, method, params);
}

async function electrsGet(route: string): Promise<unknown> {
    const url = `${ELECTRS_URL}${route.startsWith("/") ? route : `/${route}`}`;
    const response = await fetch(url);
    const text = await response.text();
    if (!response.ok) throw new Error(`Electrs GET ${url} returned ${response.status}: ${text}`);
    try {
        return JSON.parse(text);
    } catch {
        return text;
    }
}



function loadBridgeOutput(): BridgeOutput {
    const bridge = readJsonObject(BRIDGE_OUTPUT_PATH);
    const user = readJsonObject(USER_OUTPUT_PATH);
    const privateKey = user.private_key;
    if (!Array.isArray(privateKey) || privateKey.length !== 64 || privateKey.some((byte) => !Number.isInteger(byte) || byte < 0 || byte > 255)) {
        throw new Error(`${USER_OUTPUT_PATH} private_key must contain 64 bytes`);
    }
    const output: BridgeOutput = {
        bridgeStatePda: requiredString(bridge, "bridge_state_pda", BRIDGE_OUTPUT_PATH),
        dogeMint: requiredString(bridge, "doge_mint", BRIDGE_OUTPUT_PATH),
        operatorPubkey: requiredString(bridge, "operator_pubkey", BRIDGE_OUTPUT_PATH),
        payerPubkey: requiredString(bridge, "payer_pubkey", BRIDGE_OUTPUT_PATH),
        operatorKeypair: path.join(KEYS_DIR, "operator.json"),
        payerKeypair: path.join(KEYS_DIR, "payer.json"),
        operatorStore: path.join(KEYS_DIR, "operator-store.sqlite"),
        userKeypair: USER_OUTPUT_PATH,
        userPubkey: requiredString(user, "pubkey", USER_OUTPUT_PATH),
        userTokenAccount: requiredString(user, "doge_ata", USER_OUTPUT_PATH),
    };
    for (const filePath of [output.operatorKeypair, output.payerKeypair]) {
        assertCondition(fs.existsSync(filePath), `Required keypair is missing: ${filePath}`);
    }
    assertCondition(output.bridgeStatePda === "9vzbk8X27e6VRcCPWCyxZsa2DV6GLQ3y9e1mXzfAgUdX", `Unexpected bridge state PDA ${output.bridgeStatePda}`);
    return output;
}

function loadSolanaKeypair(filePath: string, field = "private_key"): Keypair {
    const parsed = readJsonObject(filePath);
    const value = parsed[field];
    if (!Array.isArray(value) || value.length !== 64 || value.some((byte) => !Number.isInteger(byte) || byte < 0 || byte > 255)) {
        throw new Error(`${filePath} ${field} must contain 64 bytes`);
    }
    return Keypair.fromSecretKey(Uint8Array.from(value));
}

function loadUserKeypair(): Keypair {
    return loadSolanaKeypair(USER_OUTPUT_PATH);
}

function loadFileKeypair(filePath: string): Keypair {
    const value: unknown = JSON.parse(fs.readFileSync(filePath, "utf8"));
    if (!Array.isArray(value) || value.length !== 64 || value.some((byte) => !Number.isInteger(byte) || byte < 0 || byte > 255)) {
        throw new Error(`${filePath} must contain a 64-byte Solana keypair array`);
    }
    return Keypair.fromSecretKey(Uint8Array.from(value));
}

function getAccountBytes(result: unknown, address: string): Uint8Array {
    if (!isObject(result) || !isObject(result.value)) throw new Error(`Solana account ${address} is absent`);
    const data = result.value.data;
    if (!Array.isArray(data) || typeof data[0] !== "string" || data[1] !== "base64") {
        throw new Error(`Solana account ${address} did not return base64 data`);
    }
    return Uint8Array.from(Buffer.from(data[0], "base64"));
}

async function readBridgeProgress(bridgeStatePda: string): Promise<BridgeProgress> {
    const bytes = getAccountBytes(
        await solanaRpc("getAccountInfo", [bridgeStatePda, { encoding: "base64", commitment: "confirmed" }]),
        bridgeStatePda,
    );
    assertCondition(bytes.length > BRIDGE_FINALIZED_HEIGHT_OFFSET + 4, `Bridge state account is too short: ${bytes.length} bytes`);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    return {
        accountDataLength: bytes.length,
        tipHeight: view.getUint32(BRIDGE_TIP_HEIGHT_OFFSET, true),
        autoClaimedDepositsNextIndex: view.getUint32(BRIDGE_FINALIZED_AUTO_CLAIM_INDEX_OFFSET, true),
        finalizedHeight: view.getUint32(BRIDGE_FINALIZED_HEIGHT_OFFSET, true),
    };
}

async function tokenBalance(tokenAccount: string): Promise<TokenBalance> {
    const result = await solanaRpc("getTokenAccountBalance", [tokenAccount, { commitment: "confirmed" }]);
    if (!isObject(result) || !isObject(result.value)) throw new Error(`Invalid getTokenAccountBalance response for ${tokenAccount}`);
    const amount = requiredString(result.value, "amount", "getTokenAccountBalance.value");
    const decimals = requiredNumber(result.value, "decimals", "getTokenAccountBalance.value");
    const uiAmountString = requiredString(result.value, "uiAmountString", "getTokenAccountBalance.value");
    if (!/^\d+$/.test(amount)) throw new Error(`Token balance amount is not an integer: ${amount}`);
    return { amount: BigInt(amount), decimals, uiAmountString };
}

function p2shPayload(address: string): Uint8Array {
    const decoded = Uint8Array.from(bs58.decode(address));
    assertCondition(decoded.length === 25, `Dogecoin address ${address} must decode to 25 bytes`);
    assertCondition(decoded[0] === 0xc4, `Dogecoin address ${address} is not regtest P2SH (version ${decoded[0]})`);
    const body = decoded.slice(0, 21);
    const checksum = decoded.slice(21);
    const first = createHash("sha256").update(body).digest();
    const expected = createHash("sha256").update(first).digest().subarray(0, 4);
    assertCondition(Buffer.from(checksum).equals(expected), `Dogecoin address ${address} has invalid Base58Check checksum`);
    return decoded.slice(1, 21);
}

function regtestP2shAddress(payload: Uint8Array): string {
    assertCondition(payload.length === 20, `P2SH payload must be 20 bytes, got ${payload.length}`);
    const body = Buffer.concat([Buffer.from([0xc4]), Buffer.from(payload)]);
    const first = createHash("sha256").update(body).digest();
    const checksum = createHash("sha256").update(first).digest().subarray(0, 4);
    return bs58.encode(Buffer.concat([body, checksum]));
}

function buildRequestWithdrawalInstructionData(recipientPayload: Uint8Array, grossAmountSats: bigint, netAmountSats: bigint): Buffer {
    assertCondition(recipientPayload.length === 20, `P2SH payload must be 20 bytes, got ${recipientPayload.length}`);
    const maxU64 = (1n << 64n) - 1n;
    assertCondition(grossAmountSats > 0n && grossAmountSats <= maxU64, `Gross withdrawal amount is outside u64: ${grossAmountSats}`);
    assertCondition(netAmountSats > 0n && netAmountSats < grossAmountSats, `Net withdrawal amount ${netAmountSats} must be positive and below gross ${grossAmountSats}`);

    const instructionData = Buffer.alloc(REQUEST_WITHDRAWAL_INSTRUCTION_DATA_SIZE);
    instructionData.fill(INSTRUCTION_REQUEST_WITHDRAWAL, 0, 8);
    instructionData.writeBigUInt64LE(grossAmountSats, 8);
    instructionData.writeUInt32LE(WITHDRAWAL_ADDRESS_TYPE_P2SH, 16);
    instructionData.set(recipientPayload, 20);
    instructionData.writeBigUInt64LE(netAmountSats, 40);
    return instructionData;
}

async function requestWithdrawal(
    payerKeypair: Keypair,
    userKeypair: Keypair,
    userTokenAccount: string,
    dogeMint: string,
    recipientAddress: string,
    grossAmountSats: bigint,
    netAmountSats: bigint,
): Promise<string> {
    const instructionData = buildRequestWithdrawalInstructionData(p2shPayload(recipientAddress), grossAmountSats, netAmountSats);
    const bridgeState = PublicKey.findProgramAddressSync([Buffer.from("bridge_state")], DOGE_BRIDGE_PROGRAM)[0];
    const instruction = new TransactionInstruction({
        programId: DOGE_BRIDGE_PROGRAM,
        keys: [
            { pubkey: bridgeState, isSigner: false, isWritable: true },
            { pubkey: new PublicKey(userTokenAccount), isSigner: false, isWritable: true },
            { pubkey: new PublicKey(dogeMint), isSigner: false, isWritable: true },
            { pubkey: userKeypair.publicKey, isSigner: true, isWritable: false },
            { pubkey: TOKEN_PROGRAM_ID, isSigner: false, isWritable: false },
        ],
        data: instructionData,
    });
    const connection = new Connection(SOLANA_RPC, "confirmed");
    const latest = await connection.getLatestBlockhash("confirmed");
    const transaction = new Transaction({
        feePayer: payerKeypair.publicKey,
        recentBlockhash: latest.blockhash,
    }).add(instruction);
    transaction.sign(payerKeypair, userKeypair);
    const signature = await connection.sendRawTransaction(transaction.serialize(), { skipPreflight: false, maxRetries: 5 });
    await connection.confirmTransaction({ signature, ...latest }, "confirmed");
    return signature;
}

function commandEvidence(result: CommandResult): JsonObject {
    const command = [...result.command];
    for (let index = 0; index < command.length - 1; index += 1) {
        if (command[index] === "--funding-wif") command[index + 1] = "[REDACTED]";
    }
    return {
        command,
        exitCode: result.exitCode,
        durationMs: result.durationMs,
        stdout: compactOutput(result.stdout),
        stderr: compactOutput(result.stderr),
    };
}

async function startLauncher(profile: Profile): Promise<{ process: Bun.Subprocess; stdoutLog: string; stderrLog: string }> {
    const command = launcherCommand(profile);
    console.log(`$ ${command.map(shellQuote).join(" ")}`);
    const stdoutLog = "/tmp/e2e-test-launcher.stdout.log";
    const stderrLog = "/tmp/e2e-test-launcher.stderr.log";
    fs.writeFileSync(stdoutLog, "");
    fs.writeFileSync(stderrLog, "");
    const process = Bun.spawn(command, {
        cwd: PROJECTS_DIR,
        env: { ...globalThis.process.env, NO_PROXY: "localhost,127.0.0.1", no_proxy: "localhost,127.0.0.1" },
        stdout: Bun.file(stdoutLog),
        stderr: Bun.file(stderrLog),
    });
    return { process, stdoutLog, stderrLog };
}

async function stopLauncher(launcher: Bun.Subprocess, keepServices: boolean): Promise<JsonObject> {
    if (keepServices) return { keptRunning: true, pid: launcher.pid };
    if (launcher.exitCode === null) {
        console.log(`\n[cleanup] Sending SIGTERM to locSetupDoge PID ${launcher.pid}`);
        launcher.kill("SIGTERM");
    }
    const exitCode = await Promise.race([
        launcher.exited,
        Bun.sleep(15_000).then(() => null),
    ]);
    if (exitCode === null) {
        console.warn(`[cleanup] Launcher PID ${launcher.pid} did not exit in 15s; sending SIGKILL`);
        launcher.kill("SIGKILL");
        return { keptRunning: false, pid: launcher.pid, exitCode: await launcher.exited, forced: true };
    }
    return { keptRunning: false, pid: launcher.pid, exitCode, forced: false };
}

async function runDryPlan(options: Options, evidence: Evidence): Promise<void> {
    logPhase(1, "Infrastructure setup plan (non-destructive)");
    const command = launcherCommand(options.profile);
    assertCondition(!command.includes("--legacy-ibc"), "dry-plan launcher must not depend on --legacy-ibc");
    console.log(command.map(shellQuote).join(" "));
    logPhase(2, "Funding artifact plan");
    console.log(`Launcher prepares 110 mined blocks before checkpoint/init and atomically publishes mode-600 ${FUNDING_ARTIFACT_PATH}`);
    logPhase(3, "Deposit plan");
    console.log(`${DEPOSIT_BIN} --amount-sats ${DEPOSIT_AMOUNT_SATS} --evidence-path ${DEPOSIT_EVIDENCE_PATH} ...`);
    console.log("Deposit txid is taken exclusively from deposit evidence (never mempool[0])");
    logPhase(4, "Mine and wait for block-proof pipeline + mint plan");
    console.log(
        `Mine ${DEPOSIT_PIPELINE_BLOCKS_TO_MINE} blocks: deposit H, proof H+${PIPELINE_REQUIRED_CONFIRMATIONS} finalizing H, and a confirmation block so that proof is processable`,
    );
    console.log(`Wait for ${BLOCK_PROOF_LATEST_PATH} with finalized deposit_count>0 and 356B proof`);
    console.log("Wait for IBC block_update mint of pDOGE");
    logPhase(5, "Verify mint plan");
    console.log(`Solana getTokenAccountBalance; expected net mint ${EXPECTED_NET_MINT_SATS} after flat+percent fees on gross ${DEPOSIT_AMOUNT_SATS}`);
    logPhase(6, "Burn pDOGE plan");
    console.log(`Submit request_withdrawal burning gross ${BURN_AMOUNT_SATS} sats with net ${EXPECTED_NET_WITHDRAWAL_SATS} sats after the explicit flat+percent withdrawal fee`);
    logPhase(7, "Full-live withdrawal plan (gated; not executed in dry-run)");
    console.log(
        `${PROCESS_WITHDRAWAL_BIN} --manager-signing-enabled --broadcast-enabled --evidence-path ${WITHDRAWAL_EVIDENCE_PATH} ...`,
    );
    console.log("Assert atomic authorize/VAA, 5-of-7 manager quorum, signed tx, live confirmation, and disc-17 finalize");
    logPhase(8, "Single-ZK completion evidence plan");
    console.log(`Parse content-addressed block proof ${BLOCK_PROOF_LATEST_PATH} + non-ZK withdrawal ${WITHDRAWAL_EVIDENCE_PATH}`);
    console.log(`Require one ${GROTH16_PROOF_SIZE}B block proof with ${PUBLIC_VALUES_SIZE}B public values and VK/ELF/input hashes`);
    console.log(`Write ${EVIDENCE_PATH} with completed=false under dry-plan`);

    evidence.completed = false;
    evidence.finishedAt = new Date().toISOString();
    evidence.phases.plan = {
        launcherCommand: command,
        fundingArtifact: FUNDING_ARTIFACT_PATH,
        depositAmountSats: DEPOSIT_AMOUNT_SATS,
        expectedNetMintSats: EXPECTED_NET_MINT_SATS,
        depositFlatFeeSats: DEPOSIT_FLAT_FEE_SATS,
        depositFeeRate: `${DEPOSIT_FEE_NUM}/${DEPOSIT_FEE_DEN}`,
        pipelineRequiredConfirmations: PIPELINE_REQUIRED_CONFIRMATIONS,
        depositPipelineBlocksToMine: DEPOSIT_PIPELINE_BLOCKS_TO_MINE,
        burnAmountSats: BURN_AMOUNT_SATS,
        expectedNetWithdrawalSats: EXPECTED_NET_WITHDRAWAL_SATS,
        withdrawalFlatFeeSats: WITHDRAWAL_FLAT_FEE_SATS,
        withdrawalFeeRate: `${WITHDRAWAL_FEE_NUM}/${WITHDRAWAL_FEE_DEN}`,
        blockProofLatest: BLOCK_PROOF_LATEST_PATH,
        withdrawalEvidence: WITHDRAWAL_EVIDENCE_PATH,
        fullLiveRegtestRequiredForCompletion: true,
        legacyIbc: false,
        managerSigningEnabled: true,
        broadcastEnabled: true,
        dryPlanCompletedFalse: true,
        chainSideEffects: false,
    };
    const dryEval = evaluateCompletion({
        blockManifest: null,
        withdrawalEvidence: null,
        mintSats: null,
        burnSats: null,
        requireFullLive: false,
    });
    evidence.completion = bundleCompletionEvidence(dryEval, { dryPlan: true });
    assertCondition(evidence.completed === false, "dry-plan must leave completed=false");
    assertCondition(dryEval.completedEligible === false, "dry-plan must not be completion-eligible");
    writeEvidence(evidence);
    console.log(`\nDRY-PLAN complete: zero chain side effects; completed=false`);
    console.log(`Evidence: ${EVIDENCE_PATH}`);
}

function fixtureBytes(size: number, fill: number): Buffer {
    return Buffer.alloc(size, fill);
}

function writeFixtureFile(filePath: string, bytes: Buffer): { path: string; sha256: string; size: number } {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, bytes);
    return { path: filePath, sha256: sha256Hex(bytes), size: bytes.length };
}

function runSelfTests(): void {
    const root = `/tmp/e2e-self-test-${process.pid}`;
    fs.rmSync(root, { recursive: true, force: true });
    fs.mkdirSync(root, { recursive: true });


    const withdrawalPayload = Buffer.from("000102030405060708090a0b0c0d0e0f10111213", "hex");
    const withdrawalInstructionData = buildRequestWithdrawalInstructionData(
        withdrawalPayload,
        BigInt(BURN_AMOUNT_SATS),
        BigInt(EXPECTED_NET_WITHDRAWAL_SATS),
    );
    const expectedWithdrawalInstructionHex = "020202020202020280f0fa020000000001000000000102030405060708090a0b0c0d0e0f10111213e0b85a0200000000";
    const withdrawalFeeSats = BURN_AMOUNT_SATS - EXPECTED_NET_WITHDRAWAL_SATS;
    assertCondition(EXPECTED_NET_WITHDRAWAL_SATS === 39_500_000, "50,000,000 gross must produce 39,500,000 net under the initialized withdrawal fees");
    assertCondition(withdrawalFeeSats === 10_500_000 && withdrawalFeeSats > 0, "withdrawal net must include the nonzero flat+percent fee");
    assertCondition(withdrawalInstructionData.length === REQUEST_WITHDRAWAL_INSTRUCTION_DATA_SIZE, "request_withdrawal instruction data must be exactly 48 bytes");
    assertCondition(withdrawalInstructionData.subarray(0, 8).equals(Buffer.alloc(8, INSTRUCTION_REQUEST_WITHDRAWAL)), "request_withdrawal discriminator mismatch");
    assertCondition(withdrawalInstructionData.readBigUInt64LE(8) === BigInt(BURN_AMOUNT_SATS), "request_withdrawal gross amount field mismatch");
    assertCondition(withdrawalInstructionData.readUInt32LE(16) === WITHDRAWAL_ADDRESS_TYPE_P2SH, "request_withdrawal address type field mismatch");
    assertCondition(withdrawalInstructionData.subarray(20, 40).equals(withdrawalPayload), "request_withdrawal recipient payload field mismatch");
    assertCondition(withdrawalInstructionData.readBigUInt64LE(40) === BigInt(EXPECTED_NET_WITHDRAWAL_SATS), "request_withdrawal net amount field mismatch");
    assertCondition(withdrawalInstructionData.toString("hex") === expectedWithdrawalInstructionHex, "request_withdrawal exact ABI bytes mismatch");
    let rejectedNonpositiveNet = false;
    try {
        calculateWithdrawalNetAmountSats(WITHDRAWAL_FLAT_FEE_SATS, WITHDRAWAL_FLAT_FEE_SATS, 0, WITHDRAWAL_FEE_DEN);
    } catch {
        rejectedNonpositiveNet = true;
    }
    assertCondition(rejectedNonpositiveNet, "withdrawal fee calculation must reject a nonpositive net amount");
    const pipelineHeights = depositPipelineHeights(122);
    assertCondition(pipelineHeights.depositHeight === 123, "deposit must be included at H");
    assertCondition(pipelineHeights.finalizationProofHeight === 124, "required-confirmations=1 must finalize H in proof H+1");
    assertCondition(pipelineHeights.requiredTipHeight === 125, "proof H+1 must itself be confirmed by tip H+2");
    assertCondition(DEPOSIT_PIPELINE_BLOCKS_TO_MINE === 3, "deposit finalization requires exactly three post-broadcast blocks");
    const height = pipelineHeights.finalizationProofHeight;
    const contentSha = "ab".repeat(32);
    const evidenceDir = path.join(root, `height-${height}`, contentSha);
    const proofArt = writeFixtureFile(path.join(evidenceDir, "proof.bin"), fixtureBytes(GROTH16_PROOF_SIZE, 7));
    const pvArt = writeFixtureFile(path.join(evidenceDir, "public_values.bin"), fixtureBytes(PUBLIC_VALUES_SIZE, 9));
    const vkArt = writeFixtureFile(path.join(evidenceDir, "vk.bin"), Buffer.from(EXPECTED_BLOCK_VK.slice(2), "hex"));
    const elfArt = writeFixtureFile(path.join(evidenceDir, "elf.bin"), fixtureBytes(64, 3));
    const inputsArt = writeFixtureFile(path.join(evidenceDir, "inputs.json"), Buffer.from("{}"));
    const block: JsonObject = {
        height,
        status: "minted",
        content_sha256: contentSha,
        evidence_dir: evidenceDir,
        manifest_path: path.join(evidenceDir, "manifest.json"),
        manifest_sha256: "ef".repeat(32),
        deposit_count: 1,
        minted_amount_sats: EXPECTED_NET_MINT_SATS,
        finalized_source_height: pipelineHeights.depositHeight,
        source_witness_height: pipelineHeights.finalizationProofHeight,
        witness_deposit_count: 0,
        witness_minted_amount_sats: 0,
        submission_signature: "sig",
        vk_hash: EXPECTED_BLOCK_VK,
        elf_sha256: elfArt.sha256,
        buffer_upload_completed: true,
        mint_buffer: "mint-buffer",
        mint_buffer_bump: 1,
        txo_buffer: "txo-buffer",
        txo_buffer_bump: 2,
        mint_group_signatures: ["mint-sig"],
        mint_groups_processed: 1,
        total_mints_processed: 1,
        artifacts: { proof: proofArt, public_values: pvArt, vk: vkArt, elf: elfArt, inputs: inputsArt },
    };
    const withdrawal: JsonObject = {
        schema: "doge-local-process-withdrawal-v3-output-only",
        stage: "CONFIRMED_FINALIZED",
        completed: true,
        request: { index: 0, grossAmountSats: BURN_AMOUNT_SATS, recipientAmountSats: EXPECTED_NET_WITHDRAWAL_SATS },
        authorize: {
            signature: "authorize-sig", pendingPda: "pending", intentHashHex: "aa".repeat(32),
            utx0HashHex: "bb".repeat(32), unsignedTxHashHex: "cc".repeat(32),
            requestStart: 0, requestEnd: 1, managerSetIndex: 0, inputCount: 1,
            noopVerified: true, sequence: 7,
        },
        relay: {
            vaaHashHex: "dd".repeat(32), vaaSequence: 7, signatureCount: 5, quorum: 5,
            managerTotal: 7, signerIndices: [0, 1, 2, 3, 4], signedRawBytes: 220,
            signedRawSha256Hex: "ee".repeat(32), finalTxidInternalHex: "ff".repeat(32), broadcast: true,
        },
        confirmation: { status: "CONFIRMED", blockHeight: 100, txIndexInBlock: 1, confirmations: 1, transactionMerkleBranchHex: [] },
        finalize: { status: "FINALIZED", instruction: "finalize_confirmed_withdrawal", discriminator: 17, permissionless: true, finalizeConfirmed: true, signature: "fin", slot: 2 },
    };

    const success = evaluateCompletion({ blockManifest: block, withdrawalEvidence: withdrawal, mintSats: EXPECTED_NET_MINT_SATS, burnSats: BURN_AMOUNT_SATS, requireFullLive: true });
    assertCondition(success.completedEligible, success.reasons.join("; "));
    assertCondition(success.evidence.singleZk === true, "single block ZK identity missing");
    assertCondition(success.evidence.noWithdrawalZk === true, "withdrawal must be explicitly non-ZK");

    const witnessEvidenceDir = path.join(root, `height-${pipelineHeights.depositHeight}`, contentSha);
    fs.mkdirSync(witnessEvidenceDir, { recursive: true });
    const witnessOnlyBlock: JsonObject = {
        ...block,
        height: pipelineHeights.depositHeight,
        evidence_dir: witnessEvidenceDir,
        manifest_path: path.join(witnessEvidenceDir, "manifest.json"),
        finalized_source_height: pipelineHeights.depositHeight - PIPELINE_REQUIRED_CONFIRMATIONS,
        source_witness_height: pipelineHeights.depositHeight,
        witness_deposit_count: 1,
        witness_minted_amount_sats: EXPECTED_NET_MINT_SATS,
        deposit_count: 0,
        minted_amount_sats: 0,
        mint_group_signatures: [],
        mint_groups_processed: 0,
        total_mints_processed: 0,
    };
    const witnessOnly = validateBlockProofEvidence(witnessOnlyBlock, { requireDeposit: true, requireMinted: false });
    assertCondition(witnessOnly.witnessDepositCount === 1, "proof H must expose the deposit witness");
    let witnessOnlyPassedLiveMintGate = false;
    try {
        validateBlockProofEvidence(witnessOnlyBlock, { requireDeposit: true, requireMinted: true });
        witnessOnlyPassedLiveMintGate = true;
    } catch {
        // Expected: proof H witnesses the deposit but cannot mint until proof H+1 finalizes its buffer.
    }
    assertCondition(!witnessOnlyPassedLiveMintGate, "proof H witness must not satisfy the live finalized-mint gate");

    const duplicateSigner = evaluateCompletion({
        blockManifest: block,
        withdrawalEvidence: {
            ...withdrawal,
            relay: { ...(withdrawal.relay as JsonObject), signerIndices: [0, 1, 2, 3, 3] },
        },
        mintSats: EXPECTED_NET_MINT_SATS,
        burnSats: BURN_AMOUNT_SATS,
        requireFullLive: true,
    });
    assertCondition(!duplicateSigner.completedEligible, "completion must reject duplicate manager signer evidence");

    const noFinalize = evaluateCompletion({
        blockManifest: block,
        withdrawalEvidence: { ...withdrawal, completed: false, stage: "SIGNED_NOT_BROADCAST", confirmation: { status: "NOT_ATTEMPTED" }, finalize: { status: "NOT_ATTEMPTED" } },
        mintSats: EXPECTED_NET_MINT_SATS,
        burnSats: BURN_AMOUNT_SATS,
        requireFullLive: true,
    });
    assertCondition(!noFinalize.completedEligible, "completion must require live confirmation/finalize");

    const noBlock = evaluateCompletion({ blockManifest: null, withdrawalEvidence: withdrawal, mintSats: EXPECTED_NET_MINT_SATS, burnSats: BURN_AMOUNT_SATS, requireFullLive: true });
    assertCondition(!noBlock.completedEligible, "completion must require block proof");

    fs.rmSync(root, { recursive: true, force: true });
    console.log(`SELF-TEST PASS: request_withdrawal 48-byte ABI (${BURN_AMOUNT_SATS} gross -> ${EXPECTED_NET_WITHDRAWAL_SATS} net) + deposit H -> proof H+1 finalized buffer + single block ZK + atomic confirmed withdrawal gates`);
}

async function main(): Promise<void> {
    const options = parseOptions();
    if (!options) return;

    if (options.selfTest) {
        runSelfTests();
        return;
    }

    const evidence: Evidence = {
        schema: "doge-bun-e2e-v2",
        startedAt: new Date().toISOString(),
        completed: false,
        profile: options.profile,
        mode: options.fullLiveRegtest ? "full-live-regtest" : "dry-plan",
        withdrawalMode: {
            managerSigningEnabled: true,
            broadcastEnabled: options.fullLiveRegtest,
            dryRunDefault: !options.fullLiveRegtest,
            fullLiveRegtest: options.fullLiveRegtest,
        },
        paths: {
            ibcRepo: IBC_REPO,
            projectsDir: PROJECTS_DIR,
            bridgeRepo: BRIDGE_REPO,
            bridgeOutput: BRIDGE_OUTPUT_PATH,
            userOutput: USER_OUTPUT_PATH,
            depositEvidence: DEPOSIT_EVIDENCE_PATH,
            fundingArtifact: FUNDING_ARTIFACT_PATH,
            withdrawalEvidence: WITHDRAWAL_EVIDENCE_PATH,
            blockProofLatest: BLOCK_PROOF_LATEST_PATH,
            blockProofRoot: BLOCK_PROOF_EVIDENCE_ROOT,
            finalEvidence: EVIDENCE_PATH,
        },
        phases: {},
    };
    writeEvidence(evidence);

    if (options.dryRun) {
        await runDryPlan(options, evidence);
        return;
    }

    assertCondition(options.fullLiveRegtest, "live execution requires --full-live-regtest");

    try {
        for (const binary of [DEPOSIT_BIN, PROCESS_WITHDRAWAL_BIN]) {
            assertCondition(fs.existsSync(binary), `Required release binary is missing: ${binary}`);
        }
        evidence.phases.preflight = preflightLocalBinaries();
        writeEvidence(evidence);
    } catch (error) {
        evidence.failure = {
            message: error instanceof Error ? error.message : String(error),
            stack: error instanceof Error ? error.stack : undefined,
        };
        evidence.finishedAt = new Date().toISOString();
        evidence.completed = false;
        writeEvidence(evidence);
        throw error;
    }

    for (const stale of [FUNDING_ARTIFACT_PATH, DEPOSIT_EVIDENCE_PATH, WITHDRAWAL_EVIDENCE_PATH]) {
        fs.rmSync(stale, { force: true });
    }
    fs.rmSync(BLOCK_PROOF_EVIDENCE_ROOT, { recursive: true, force: true });

    let launcher: Bun.Subprocess | null = null;
    let launcherLogs: { stdoutLog: string; stderrLog: string } | null = null;
    try {
        logPhase(1, "Infrastructure Setup (no legacy-ibc)");
        const startedLauncher = await startLauncher(options.profile);
        launcher = startedLauncher.process;
        launcherLogs = { stdoutLog: startedLauncher.stdoutLog, stderrLog: startedLauncher.stderrLog };
        const bridge = await waitForLauncherReady(launcher, "bridge initialization, users, Electrs, block sender, IBC pipeline, and manager service", 180_000, async () => {
            if (!fs.existsSync(BRIDGE_OUTPUT_PATH) || !fs.existsSync(USER_OUTPUT_PATH)) return false;
            try {
                const [health, electrsHeight, managerHealth] = await Promise.all([
                    solanaRpc("getHealth"),
                    electrsGet("/blocks/tip/height"),
                    fetch(`${MANAGER_SERVICE_URL}/v1/signed_vaa/1/${"00".repeat(32)}/0`),
                ]);
                if (health !== "ok" || (typeof electrsHeight !== "string" && typeof electrsHeight !== "number")) return false;
                if (managerHealth.status !== 404) return false;
                return loadBridgeOutput();
            } catch {
                return false;
            }
        });
        const bridgeBefore = await readBridgeProgress(bridge.bridgeStatePda);
        evidence.phases.infrastructure = { ready: true, launcherPid: launcher.pid, bridge, bridgeBefore, legacyIbc: false };
        writeEvidence(evidence);

        logPhase(2, "Validate Launcher Funding Artifact");
        assertCondition(fs.existsSync(FUNDING_ARTIFACT_PATH), `Launcher readiness reached without funding artifact ${FUNDING_ARTIFACT_PATH}`);
        const funding = readFundingArtifact(FUNDING_ARTIFACT_PATH);
        const fundingAddress = funding.address;
        const wifValue = funding.wif;
        const fundingUtxo: ElectrsUtxo = {
            txid: funding.txid,
            vout: funding.vout,
            value: funding.value,
            status: { confirmed: true, block_height: funding.blockHeight },
        };
        evidence.phases.funding = {
            source: "launcher-funding-artifact",
            artifactPath: FUNDING_ARTIFACT_PATH,
            address: funding.address,
            minedBlocks: funding.minedBlocks,
            confirmations: funding.confirmations,
            dogecoinTipHeight: funding.dogecoinTipHeight,
            electrsTipHeight: funding.electrsTipHeight,
            selectedUtxo: fundingUtxo,
            wifRecorded: false,
        };
        writeEvidence(evidence);

        logPhase(3, "Deposit");
        const balanceBefore = await tokenBalance(bridge.userTokenAccount);
        const depositPromise = runCommand(DEPOSIT_BIN, [
            "--solana-rpc-url", SOLANA_RPC,
            "--operator-keypair", bridge.operatorKeypair,
            "--payer-keypair", bridge.payerKeypair,
            "--recipient-token-account", bridge.userTokenAccount,
            "--operator-store", bridge.operatorStore,
            "--electrs-url", ELECTRS_URL,
            "--funding-wif", wifValue,
            "--funding-txid", fundingUtxo.txid,
            "--funding-vout", String(fundingUtxo.vout),
            "--funding-amount", String(fundingUtxo.value),
            "--amount-sats", String(DEPOSIT_AMOUNT_SATS),
            "--confirmation-timeout-secs", "180",
            "--evidence-path", DEPOSIT_EVIDENCE_PATH,
        ], { cwd: LOCAL_OPS_ROOT });

        // Authoritative deposit identity comes from deposit evidence, never mempool[0].
        const depositTxid = await waitFor("deposit evidence txid", 120_000, async () => {
            if (!fs.existsSync(DEPOSIT_EVIDENCE_PATH)) return false;
            try {
                const depositEvidencePartial = readJsonObject(DEPOSIT_EVIDENCE_PATH);
                const depositObject = depositEvidencePartial.deposit;
                if (!isObject(depositObject)) return false;
                const txid = depositObject.txid;
                return typeof txid === "string" && txid.length > 0 ? txid : false;
            } catch {
                return false;
            }
        }, 500);
        evidence.phases.depositBroadcast = {
            txid: depositTxid,
            balanceBefore: balanceBefore.amount.toString(),
            source: "deposit-evidence",
            mempoolIndexForbidden: true,
        };
        writeEvidence(evidence);

        logPhase(4, "Mine + Wait for block proof pipeline + mint");
        const depositBlockHeightBeforeRaw = await dogeRpc("getblockcount", []);
        assertCondition(typeof depositBlockHeightBeforeRaw === "number", `getblockcount returned ${String(depositBlockHeightBeforeRaw)}`);
        const depositMiningPlan = depositPipelineHeights(depositBlockHeightBeforeRaw);
        const minedBlockHashes = await dogeRpc("generatetoaddress", [DEPOSIT_PIPELINE_BLOCKS_TO_MINE, fundingAddress]);
        assertCondition(
            Array.isArray(minedBlockHashes) && minedBlockHashes.length === DEPOSIT_PIPELINE_BLOCKS_TO_MINE,
            `Expected ${DEPOSIT_PIPELINE_BLOCKS_TO_MINE} mined block hashes, got ${JSON.stringify(minedBlockHashes)}`,
        );
        const dogeHeightAfterMining = await dogeRpc("getblockcount", []);
        assertCondition(
            dogeHeightAfterMining === depositMiningPlan.requiredTipHeight,
            `Expected Dogecoin tip ${depositMiningPlan.requiredTipHeight} after deposit mining, got ${String(dogeHeightAfterMining)}`,
        );
        const electrsHeightAfterMining = await waitFor("Electrs to index the deposit finalization sequence", 120_000, async () => {
            const height = Number(await electrsGet("/blocks/tip/height"));
            return Number.isSafeInteger(height) && height >= depositMiningPlan.requiredTipHeight ? height : false;
        });
        const depositResult = await depositPromise;
        const depositEvidence = readJsonObject(DEPOSIT_EVIDENCE_PATH);
        const depositObject = depositEvidence.deposit;
        assertCondition(isObject(depositObject), `${DEPOSIT_EVIDENCE_PATH} is missing deposit object`);
        const confirmedDepositTxid = requiredString(depositObject, "txid", `${DEPOSIT_EVIDENCE_PATH}.deposit`);
        assertCondition(confirmedDepositTxid === depositTxid, `Deposit evidence txid changed from ${depositTxid} to ${confirmedDepositTxid}`);
        const confirmedDepositHeight = requiredNumber(depositObject, "confirmation_height", `${DEPOSIT_EVIDENCE_PATH}.deposit`);
        assertCondition(
            confirmedDepositHeight === depositMiningPlan.depositHeight,
            `Expected deposit in first mined block H=${depositMiningPlan.depositHeight}, got H=${confirmedDepositHeight}`,
        );

        const balanceAfter = await waitFor("IBC block_update and pDOGE mint", 15 * 60_000, async () => {
            const [progress, balance] = await Promise.all([
                readBridgeProgress(bridge.bridgeStatePda),
                tokenBalance(bridge.userTokenAccount),
            ]);
            const expected = balanceBefore.amount + BigInt(EXPECTED_NET_MINT_SATS);
            if (balance.amount < expected) return false;
            if (progress.finalizedHeight < depositMiningPlan.depositHeight) return false;
            return { progress, balance };
        }, 2_000);

        const blockManifest = await waitFor("block proof pipeline latest.json with deposit claim", 15 * 60_000, async () => {
            if (!fs.existsSync(BLOCK_PROOF_LATEST_PATH)) return false;
            try {
                const manifest = readJsonObject(BLOCK_PROOF_LATEST_PATH);
                // Live mint evidence must come from proof H+C, whose finalized buffer contains deposit H.
                const validated = validateBlockProofEvidence(manifest, { requireDeposit: true, requireMinted: true });
                if (validated.height !== depositMiningPlan.finalizationProofHeight) return false;
                if (validated.finalizedSourceHeight !== depositMiningPlan.depositHeight) return false;
                return { manifest, validated };
            } catch {
                return false;
            }
        }, 2_000);

        evidence.phases.deposit = {
            command: commandEvidence(depositResult),
            transaction: depositObject,
            custody: depositEvidence.custody,
            dogeHeightBeforeMining: depositBlockHeightBeforeRaw,
            dogeHeightAfterMining,
            electrsHeightAfterMining,
            minedBlockCount: DEPOSIT_PIPELINE_BLOCKS_TO_MINE,
            miningPlan: depositMiningPlan,
            bridgeBefore,
            bridgeAfter: balanceAfter.progress,
            depositTxidSource: "deposit-evidence",
        };
        evidence.phases.blockProof = blockManifest.validated;
        writeEvidence(evidence);

        logPhase(5, "Verify Mint");
        const mintedDelta = balanceAfter.balance.amount - balanceBefore.amount;
        const manifestMinted = typeof blockManifest.validated.mintedAmountSats === "number"
            ? blockManifest.validated.mintedAmountSats
            : EXPECTED_NET_MINT_SATS;
        assertCondition(
            mintedDelta === BigInt(EXPECTED_NET_MINT_SATS),
            `Expected net mint ${EXPECTED_NET_MINT_SATS} pDOGE sats (gross deposit ${DEPOSIT_AMOUNT_SATS} minus fees), got ${mintedDelta}`,
        );
        assertCondition(
            BigInt(manifestMinted) === mintedDelta,
            `Token balance delta ${mintedDelta} != block manifest minted_amount_sats ${manifestMinted}`,
        );
        assertCondition(balanceAfter.balance.amount >= BigInt(BURN_AMOUNT_SATS), `Minted balance ${balanceAfter.balance.amount} is below burn amount ${BURN_AMOUNT_SATS}`);
        evidence.phases.mint = {
            tokenAccount: bridge.userTokenAccount,
            dogeMint: bridge.dogeMint,
            beforeSats: balanceBefore.amount.toString(),
            afterSats: balanceAfter.balance.amount.toString(),
            grossDepositSats: DEPOSIT_AMOUNT_SATS,
            flatFeeSats: DEPOSIT_FLAT_FEE_SATS,
            feeRate: `${DEPOSIT_FEE_NUM}/${DEPOSIT_FEE_DEN}`,
            expectedNetMintSats: EXPECTED_NET_MINT_SATS,
            mintedSats: mintedDelta.toString(),
            manifestMintedAmountSats: manifestMinted,
            verified: true,
        };
        writeEvidence(evidence);

        logPhase(6, "Burn pDOGE / request_withdrawal");
        const recipientPayload = createHash("sha256").update(`doge-e2e-withdrawal-${Date.now()}`).digest().subarray(0, 20);
        const recipientAddressValue = regtestP2shAddress(recipientPayload);
        const userKeypair = loadUserKeypair();
        const payerKeypair = loadFileKeypair(bridge.payerKeypair);
        assertCondition(userKeypair.publicKey.toBase58() === bridge.userPubkey, `User keypair pubkey ${userKeypair.publicKey} does not match ${bridge.userPubkey}`);
        assertCondition(payerKeypair.publicKey.toBase58() === bridge.payerPubkey, `Payer keypair pubkey ${payerKeypair.publicKey} does not match ${bridge.payerPubkey}`);
        const burnSignature = await requestWithdrawal(
            payerKeypair,
            userKeypair,
            bridge.userTokenAccount,
            bridge.dogeMint,
            recipientAddressValue,
            BigInt(BURN_AMOUNT_SATS),
            BigInt(EXPECTED_NET_WITHDRAWAL_SATS),
        );
        const postBurnBalance = await waitFor("pDOGE burn balance", 60_000, async () => {
            const balance = await tokenBalance(bridge.userTokenAccount);
            return balance.amount === balanceAfter.balance.amount - BigInt(BURN_AMOUNT_SATS) ? balance : false;
        }, 500);
        evidence.phases.burn = {
            signature: burnSignature,
            amountSats: BURN_AMOUNT_SATS,
            addressType: 1,
            recipientAddress: recipientAddressValue,
            recipientPayloadHex: Buffer.from(p2shPayload(recipientAddressValue)).toString("hex"),
            netAmountSats: EXPECTED_NET_WITHDRAWAL_SATS,
            withdrawalFlatFeeSats: WITHDRAWAL_FLAT_FEE_SATS,
            withdrawalFeeRate: `${WITHDRAWAL_FEE_NUM}/${WITHDRAWAL_FEE_DEN}`,
            balanceBeforeSats: balanceAfter.balance.amount.toString(),
            balanceAfterSats: postBurnBalance.amount.toString(),
            verified: true,
        };
        writeEvidence(evidence);

        logPhase(7, "Withdrawal full-live: manager signing + broadcast + disc-17 finalize");
        const withdrawalArgs = withdrawalCliArgs(bridge, true);
        assertCondition(withdrawalArgs.includes("--broadcast-enabled"), "full-live withdrawal must enable broadcast");
        assertCondition(withdrawalArgs.includes("--manager-signing-enabled"), "full-live withdrawal must enable manager signing");
        const withdrawalResult = await runCommand(PROCESS_WITHDRAWAL_BIN, withdrawalArgs, { cwd: LOCAL_OPS_ROOT });
        assertCondition(fs.existsSync(WITHDRAWAL_EVIDENCE_PATH), `Withdrawal evidence missing at ${WITHDRAWAL_EVIDENCE_PATH}`);
        const withdrawalEvidence = readJsonObject(WITHDRAWAL_EVIDENCE_PATH);
        const withdrawalValidated = validateWithdrawalEvidence(withdrawalEvidence, { requireFinalize: true });
        assertCondition(withdrawalValidated.finalizeDiscriminator === DISC_17_FINALIZE, "disc-17 finalize discriminator missing");
        assertCondition(withdrawalValidated.finalizeOk === true, "disc-17 finalize was not confirmed");
        assertCondition(withdrawalEvidence.completed === true, "withdrawal evidence completed must be true after finalize");

        evidence.phases.withdrawal = {
            command: commandEvidence(withdrawalResult),
            requestIndex: 0,
            managerSigningEnabled: true,
            broadcastEnabled: true,
            evidence: withdrawalValidated,
            evidenceFile: WITHDRAWAL_EVIDENCE_PATH,
        };
        writeEvidence(evidence);

        logPhase(8, "Single block-ZK top-level verification");
        const completion = evaluateCompletion({
            blockManifest: blockManifest.manifest,
            withdrawalEvidence,
            mintSats: Number(mintedDelta),
            burnSats: BURN_AMOUNT_SATS,
            requireFullLive: true,
        });
        evidence.completion = bundleCompletionEvidence(completion, {
            pendingFinalizedStatus: PENDING_WITHDRAWAL_STATUS_FINALIZED,
        });
        assertCondition(completion.completedEligible, `Completion gate failed: ${completion.reasons.join("; ")}`);
        assertCondition(completion.evidence.singleZk === true, "content-addressed block proof identity missing");
        assertCondition(completion.evidence.noWithdrawalZk === true, "withdrawal must remain non-ZK");
        assertCondition(isObject(depositEvidence.custody) && depositEvidence.custody.registered === true, "Deposit evidence does not confirm custody registration");
        assertCondition(
            balanceAfter.progress.tipHeight > bridgeBefore.tipHeight || balanceAfter.progress.finalizedHeight > bridgeBefore.finalizedHeight,
            "Bridge state height did not advance",
        );
        evidence.phases.verification = {
            depositCustodyRegistered: true,
            bridgeStateAdvanced: true,
            mintVerified: true,
            burnVerified: true,
            blockProofVerified: true,
            withdrawalZk: false,
            managerQuorum5of7: true,
            dogeConfirmationVerified: true,
            disc17FinalizeVerified: true,
            singleBlockProofNonZero356: true,
            legacyIbc: false,
            operatorStoreExists: fs.existsSync(bridge.operatorStore),
            operatorStoreBytes: fs.existsSync(bridge.operatorStore) ? fs.statSync(bridge.operatorStore).size : 0,
        };
        evidence.completed = true;
        evidence.finishedAt = new Date().toISOString();
        writeEvidence(evidence);
        console.log(`\nPASS: full-live-regtest deposit -> block proof mint -> burn -> 5/7 signed broadcast -> disc-17 finalize completed.`);
        console.log(`Evidence: ${EVIDENCE_PATH}`);
    } catch (error) {
        evidence.completed = false;
        evidence.failure = {
            message: error instanceof Error ? error.message : String(error),
            stack: error instanceof Error ? error.stack : undefined,
        };
        evidence.finishedAt = new Date().toISOString();
        writeEvidence(evidence);
        throw error;
    } finally {
        if (launcher) {
            evidence.phases.cleanup = await stopLauncher(launcher, options.keepServices);
            if (launcherLogs) {
                const stdout = fs.existsSync(launcherLogs.stdoutLog) ? fs.readFileSync(launcherLogs.stdoutLog, "utf8") : "";
                const stderr = fs.existsSync(launcherLogs.stderrLog) ? fs.readFileSync(launcherLogs.stderrLog, "utf8") : "";
                evidence.phases.launcherOutput = {
                    stdoutLog: launcherLogs.stdoutLog,
                    stderrLog: launcherLogs.stderrLog,
                    stdout: compactOutput(stdout),
                    stderr: compactOutput(stderr),
                };
            }
            // Never flip completed=true in cleanup after failure.
            if (evidence.failure) evidence.completed = false;
            writeEvidence(evidence);
        }
    }
}

main().catch((error) => {
    console.error(`\nE2E FAILED: ${error instanceof Error ? error.message : String(error)}`);
    console.error(`Evidence: ${EVIDENCE_PATH}`);
    process.exitCode = 1;
});
