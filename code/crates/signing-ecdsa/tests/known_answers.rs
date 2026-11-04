use informalsystems_malachitebft_signing_ecdsa::{
    CurveConfig, K256Config, P256Config, P384Config, PrivateKey, PublicKey, Signature,
};
use serde::Deserialize;

const SOURCE_COMMIT: &str = "bcac2b2dbd369a6c29b9151fcd0889b133d7221a";
const MESSAGE: &[u8] = b"1money-malachite-ecdsa-known-answer-v1";

#[derive(Debug, Deserialize)]
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

fn load_fixture(contents: &str) -> Fixture {
    serde_json::from_str(contents).expect("deserialize known-answer fixture")
}

fn assert_known_answer<C: CurveConfig>(fixture: &Fixture, expected_private_key: &[u8]) {
    assert_eq!(fixture.source_commit, SOURCE_COMMIT);
    assert_eq!(fixture.message, MESSAGE);
    assert_eq!(fixture.private_key, expected_private_key);

    let private_key =
        PrivateKey::<C>::from_slice(expected_private_key).expect("construct private key");
    assert_eq!(private_key.to_vec(), fixture.private_key);
    assert_eq!(
        serde_json::to_string(&private_key).unwrap(),
        fixture.private_key_json
    );

    let public_key = private_key.public_key();
    assert_eq!(public_key.to_vec(), fixture.public_key);
    assert_eq!(
        serde_json::to_string(&public_key).unwrap(),
        fixture.public_key_json
    );

    let signature = private_key.sign(MESSAGE);
    assert_eq!(signature.to_vec(), fixture.signature);
    assert_eq!(
        serde_json::to_string(&signature).unwrap(),
        fixture.signature_json
    );

    let decoded_public =
        PublicKey::<C>::from_sec1_bytes(&fixture.public_key).expect("decode fixture public key");
    let decoded_signature =
        Signature::<C>::from_slice(&fixture.signature).expect("decode fixture signature");
    decoded_public
        .verify(MESSAGE, &decoded_signature)
        .expect("fixture signature verifies");
}

fn assert_rejects_mutations<C: CurveConfig>(fixture: &Fixture) {
    let public_key =
        PublicKey::<C>::from_sec1_bytes(&fixture.public_key).expect("decode fixture public key");
    let signature =
        Signature::<C>::from_slice(&fixture.signature).expect("decode fixture signature");

    let mut mutated_message = fixture.message.clone();
    mutated_message[0] ^= 1;
    assert!(public_key.verify(&mutated_message, &signature).is_err());

    let mut mutated_signature = fixture.signature.clone();
    let last = mutated_signature.len() - 1;
    mutated_signature[last] ^= 1;
    let mutated_signature = Signature::<C>::from_slice(&mutated_signature)
        .expect("mutated signature retains valid scalar encoding");
    assert!(public_key.verify(MESSAGE, &mutated_signature).is_err());
}

#[test]
fn k256_known_answer_matches_bcac2b2d() {
    let fixture = load_fixture(include_str!("fixtures/k256.json"));
    assert_known_answer::<K256Config>(&fixture, &[0x11; 32]);
}

#[test]
fn p256_known_answer_matches_bcac2b2d() {
    let fixture = load_fixture(include_str!("fixtures/p256.json"));
    assert_known_answer::<P256Config>(&fixture, &[0x11; 32]);
}

#[test]
fn p384_known_answer_matches_bcac2b2d() {
    let fixture = load_fixture(include_str!("fixtures/p384.json"));
    assert_known_answer::<P384Config>(&fixture, &[0x11; 48]);
}

#[test]
fn known_answers_reject_mutated_messages_and_signatures() {
    assert_rejects_mutations::<K256Config>(&load_fixture(include_str!("fixtures/k256.json")));
    assert_rejects_mutations::<P256Config>(&load_fixture(include_str!("fixtures/p256.json")));
    assert_rejects_mutations::<P384Config>(&load_fixture(include_str!("fixtures/p384.json")));
}
