use std::collections::BTreeSet;

use bytes::Bytes;
use glob::glob;
use malachitebft_engine::util::streaming::StreamId;
use malachitebft_peer::PeerId;

use crate::{streaming::State, utils::*};

pub mod runner;
pub mod utils;

const SHA2_256: u64 = 0x12;
const PART_STREAM_TESTS: &str =
    "^(MessagesInRightOrderTest|MessagesInWrongOrderTest|InitReceivedTest|FinReceivedTest)$";
const PART_STREAM_TEST_NAMES: [&str; 4] = [
    "MessagesInRightOrderTest",
    "MessagesInWrongOrderTest",
    "InitReceivedTest",
    "FinReceivedTest",
];

#[test]
fn test_mbt_part_streaming_specified_traces() {
    let temp_dir = tempfile::TempDir::with_prefix("informalsystems-malachitebft-part-streaming")
        .expect("Failed to create temp dir");
    let temp_path = temp_dir.path().to_owned();

    if std::env::var("KEEP_TEMP").is_ok() {
        std::mem::forget(temp_dir);
    }

    let quint_seed = quint_seed();

    println!("{}", temp_path.to_string_lossy());
    generate_test_traces(
        "block-streaming/part_stream.qnt",
        &temp_path.to_string_lossy(),
        quint_seed,
        PART_STREAM_TESTS,
    );

    let json_fixtures = glob(&format!("{}/*.itf.json", temp_path.display()))
        .expect("Failed to read glob pattern")
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(
        json_fixtures.len(),
        4,
        "Quint must generate one trace for each selected part-stream scenario"
    );
    let generated_tests = json_fixtures
        .iter()
        .map(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_suffix(".itf.json"))
                .and_then(|name| name.strip_prefix("test_"))
                .and_then(|name| name.rsplit_once('_'))
                .map(|(test_name, _)| test_name)
                .expect("Quint trace filename must identify its test")
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        generated_tests,
        PART_STREAM_TEST_NAMES.into_iter().collect(),
        "Quint must generate exactly the selected part-stream scenarios"
    );

    for json_fixture in json_fixtures {
        println!(
            "🚀 Running trace {:?}",
            json_fixture.file_name().unwrap().to_str().unwrap()
        );

        let json = std::fs::read_to_string(&json_fixture).unwrap();
        let trace = itf::trace_from_str::<State>(&json).unwrap();

        let hash = multihash::Multihash::<64>::wrap(SHA2_256, b"PeerId").unwrap();
        let peer_id = PeerId::from_multihash(hash).unwrap();

        let streaming_runner = runner::StreamingRunner::new(peer_id, StreamId::new(Bytes::new()));
        trace.run_on(streaming_runner).unwrap();
    }
}

#[test]
fn test_mbt_part_streaming_random_traces() {
    let temp_dir = tempfile::TempDir::with_prefix("informalsystems-malachitebft-part-streaming")
        .expect("Failed to create temp dir");
    let temp_path = temp_dir.path().to_owned();

    if std::env::var("KEEP_TEMP").is_ok() {
        std::mem::forget(temp_dir);
    }

    let quint_seed = quint_seed();

    println!("{}", temp_path.to_string_lossy());
    generate_random_traces(
        "block-streaming/part_stream.qnt",
        &temp_path.to_string_lossy(),
        quint_seed,
        // current quint spec has 4 message parts in tests so there are 24 (4!) possible traces
        // given that duplicate messages case is not covered in the spec
        24,
    );

    for json_fixture in glob(&format!("{}/*.itf.json", temp_path.display()))
        .expect("Failed to read glob pattern")
        .flatten()
    {
        println!(
            "🚀 Running trace {:?}",
            json_fixture.file_name().unwrap().to_str().unwrap()
        );

        let json = std::fs::read_to_string(&json_fixture).unwrap();
        let trace = itf::trace_from_str::<State>(&json).unwrap();

        let hash = multihash::Multihash::<64>::wrap(SHA2_256, b"PeerId").unwrap();
        let peer_id = PeerId::from_multihash(hash).unwrap();

        let streaming_runner = runner::StreamingRunner::new(peer_id, StreamId::new(Bytes::new()));
        trace.run_on(streaming_runner).unwrap();
    }
}
