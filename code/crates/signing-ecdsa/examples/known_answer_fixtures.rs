use std::fs;
use std::path::Path;

use informalsystems_malachitebft_signing_ecdsa::{
    CurveConfig, K256Config, P256Config, P384Config, PrivateKey,
};
use serde::Serialize;

const SOURCE_COMMIT: &str = "bcac2b2dbd369a6c29b9151fcd0889b133d7221a";
const MESSAGE: &[u8] = b"1money-malachite-ecdsa-known-answer-v1";

#[derive(Serialize)]
struct Fixture {
    source_commit: String,
    message: Vec<u8>,
    private_key: Vec<u8>,
    public_key: Vec<u8>,
    signature: Vec<u8>,
    private_key_json: String,
    public_key_json: String,
    signature_json: String,
}

fn fixture<C: CurveConfig>(source_commit: &str, private_key: &[u8], message: &[u8]) -> Fixture {
    let private_key =
        PrivateKey::<C>::from_slice(private_key).expect("fixture private key is valid");
    let public_key = private_key.public_key();
    let signature = private_key.sign(message);

    Fixture {
        source_commit: source_commit.to_owned(),
        message: message.to_vec(),
        private_key: private_key.to_vec(),
        public_key: public_key.to_vec(),
        signature: signature.to_vec(),
        private_key_json: serde_json::to_string(&private_key).expect("serialize private key"),
        public_key_json: serde_json::to_string(&public_key).expect("serialize public key"),
        signature_json: serde_json::to_string(&signature).expect("serialize signature"),
    }
}

fn write_fixture(path: &Path, fixture: &Fixture) -> Result<(), Box<dyn std::error::Error>> {
    let mut json = serde_json::to_string_pretty(fixture)?;
    json.push('\n');
    fs::write(path, json)?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    fs::create_dir_all(&fixtures)?;

    write_fixture(
        &fixtures.join("k256.json"),
        &fixture::<K256Config>(SOURCE_COMMIT, &[0x11; 32], MESSAGE),
    )?;
    write_fixture(
        &fixtures.join("p256.json"),
        &fixture::<P256Config>(SOURCE_COMMIT, &[0x11; 32], MESSAGE),
    )?;
    write_fixture(
        &fixtures.join("p384.json"),
        &fixture::<P384Config>(SOURCE_COMMIT, &[0x11; 48], MESSAGE),
    )?;

    Ok(())
}
