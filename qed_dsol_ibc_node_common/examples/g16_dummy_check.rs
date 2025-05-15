use doge_light_client::hash::{sha256::QSha256Hasher, traits::BytesHasher};
use qed_dsol_bridge_core::data::scrypt_proof::DogeBlockScryptProofInput;
use qed_dsol_ibc_node_common::worker::simple_async_dummy_prover::{gen_dummy_zkp, get_public_inputs_from_output};


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum Secp256k1RecoverError {
    SignatureError,
    RecoveryError,
}
fn secp256k1_recover(
    hash: &[u8; 32],
    is_odd: bool,
    signature: &[u8; 64],
) -> Result<[u8; 64], Secp256k1RecoverError> {
    use k256::ecdsa::{RecoveryId, Signature, VerifyingKey};

    // Parse the recoverable signature
    let signature: Signature = Signature::try_from(signature.as_ref())
        .map_err(|_| Secp256k1RecoverError::SignatureError)?;

    let rec_id = RecoveryId::from_byte(if is_odd { 1 } else { 0 }).unwrap();
    
    // Recover the public key
    let recovered: [u8; 64] = VerifyingKey::recover_from_prehash(hash, &signature, rec_id)
        .map_err(|_| Secp256k1RecoverError::RecoveryError)?
        .to_encoded_point(false)
        .as_bytes()[1..]
        .try_into()
        .map_err(|_| Secp256k1RecoverError::RecoveryError)?;

    Ok(recovered)
}

const FAKE_ZKP_SECP256K1_PUBLIC_KEY: [u8; 64] = [
    42, 226, 164, 253, 134, 160, 144, 70, 218, 238, 32, 82, 219, 9, 246, 74, 56, 240, 99, 112, 107,
    164, 4, 62, 203, 190, 172, 11, 92, 167, 93, 60, 166, 22, 174, 95, 242, 154, 59, 169, 14, 213,
    91, 29, 50, 196, 217, 111, 79, 90, 138, 60, 241, 216, 166, 63, 86, 65, 236, 179, 183, 174, 108,
    44,
];

pub fn verify_dummy_zkp(
    proof_data: &[u8],
    public_inputs: &[u8],
) -> Result<(), Secp256k1RecoverError> {
    if proof_data.len() != 260 {
        return Err(Secp256k1RecoverError::RecoveryError);
    } else if public_inputs.len() != 112 {
        return Err(Secp256k1RecoverError::RecoveryError);
    }
    let base_block_sha256 = QSha256Hasher::hash_bytes(&public_inputs[0..80]);
    let mut combo: [u8; 64] = [0; 64];

    // sha256 hash
    combo[0..32].copy_from_slice(&base_block_sha256);

    // scrypt hash
    combo[32..64].copy_from_slice(&public_inputs[80..112]);

    let msg = QSha256Hasher::hash_bytes(&combo);
    let is_odd = (proof_data[64]&1) == 1;
    let pubkey = secp256k1_recover(&msg, is_odd, &proof_data[0..64].try_into().unwrap())?;

    if pubkey.ne(&FAKE_ZKP_SECP256K1_PUBLIC_KEY) {
        Err(Secp256k1RecoverError::SignatureError)
    } else {
        Ok(())
    }
}

fn main() {

    let header = hex_literal::hex!("040062008fc0121faf1821a2cec75de4125cc38dc58b057094acab9f88b517bd4f8d42a06d6dab84e3c9daab39ce25f64f18443f73f73b7c3733fbf360ada6179e9f6fce0df3db670585021e8000581a");

    let output1 = gen_dummy_zkp(DogeBlockScryptProofInput{block_header: header}).unwrap();
    let pi1 = get_public_inputs_from_output(&output1);
    verify_dummy_zkp(&output1.groth16_proof, &pi1).unwrap();

}