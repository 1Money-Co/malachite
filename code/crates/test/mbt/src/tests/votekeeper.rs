use std::collections::BTreeSet;

use glob::glob;

use rand::rngs::StdRng;
use rand::SeedableRng;

use crate::utils::generate_test_traces;
use crate::votekeeper::State;

pub mod runner;
pub mod utils;

use runner::VoteKeeperRunner;

const RANDOM_SEED: u64 = 0x42;
const VOTEKEEPER_TRACE_TESTS: &str = "^(synchronousConsensusTest|polkaAnyTest|polkaNilTest|\
    skipSmallQuorumAllPrevotesTest|noSkipSmallQuorumMixedVotesSameValTest|\
    skipSmallQuorumMixedVotesTwoValsTest|skipQuorumSinglePrevoteTest|\
    skipQuorumSinglePrecommitTest|noSkipQuorumMixedVotesSameValTest|\
    skipQuorumMixedVotesTwoValsTest)$";
const VOTEKEEPER_TRACE_TEST_NAMES: [&str; 10] = [
    "synchronousConsensusTest",
    "polkaAnyTest",
    "polkaNilTest",
    "skipSmallQuorumAllPrevotesTest",
    "noSkipSmallQuorumMixedVotesSameValTest",
    "skipSmallQuorumMixedVotesTwoValsTest",
    "skipQuorumSinglePrevoteTest",
    "skipQuorumSinglePrecommitTest",
    "noSkipQuorumMixedVotesSameValTest",
    "skipQuorumMixedVotesTwoValsTest",
];

#[test]
fn test_itf() {
    let temp_dir = tempfile::TempDir::with_prefix("arc-malachitebft-core-votekeeperkeeper-")
        .expect("Failed to create temp dir");
    let temp_path = temp_dir.path().to_owned();

    if std::env::var("KEEP_TEMP").is_ok() {
        std::mem::forget(temp_dir);
    }

    let quint_seed = option_env!("QUINT_SEED")
        .inspect(|x| {
            println!("using QUINT_SEED={x}");
        })
        .or(Some("118"))
        .and_then(|x| x.parse::<u64>().ok())
        .filter(|&x| x != 0)
        .expect("invalid random seed for quint");

    generate_test_traces(
        "consensus/quint/tests/votekeeper/votekeeperTest.qnt",
        &temp_path.to_string_lossy(),
        quint_seed,
        Some(VOTEKEEPER_TRACE_TESTS),
    );

    let json_fixtures = glob(&format!("{}/*.itf.json", temp_path.display()))
        .expect("Failed to read glob pattern")
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(
        json_fixtures.len(),
        VOTEKEEPER_TRACE_TEST_NAMES.len(),
        "Quint must generate one trace for each selected vote-keeper scenario"
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
        VOTEKEEPER_TRACE_TEST_NAMES.into_iter().collect(),
        "Quint must generate exactly the selected vote-keeper scenarios"
    );

    for json_fixture in json_fixtures {
        println!("🚀 Running trace {json_fixture:?}");

        let json = std::fs::read_to_string(&json_fixture).unwrap();
        let trace = itf::trace_from_str::<State>(&json).unwrap();

        let rng = StdRng::seed_from_u64(RANDOM_SEED);
        let vote_keeper_runner = VoteKeeperRunner::new(rng);

        trace.run_on(vote_keeper_runner).unwrap();
    }
}
