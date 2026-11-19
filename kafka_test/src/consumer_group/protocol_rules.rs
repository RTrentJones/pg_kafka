//! JoinGroup protocol rules and offsets-only groups.

use crate::common::DynResult;
use crate::common::{create_db_client, get_bootstrap_servers, TestResult};
use crate::error_codes;
use crate::fixtures::produce_one;
use crate::setup::TestContext;
use crate::wire::requests::*;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::TopicPartitionList;
use std::time::{Duration, Instant};

/// Raw wire, Kafka 3.7's answers: INCONSISTENT_GROUP_PROTOCOL for a different
/// protocol type, no common protocol, or an empty protocol list; UNKNOWN_MEMBER_ID for a JoinGroup
/// with a member id the group does not know (no group is created for it); and a v5 error
/// response decodes (protocol_name "" rather than null before v7). The group is unaffected.
pub async fn test_join_group_protocol_and_member_id_rules() -> TestResult {
    println!("=== Test: JoinGroup protocol rules ===\n");
    let ctx = TestContext::new().await?;
    let group = ctx.unique_group("proto").await;
    let fresh = ctx.unique_group("unknown-member").await;

    let mut a = conn().await?;
    let (e, generation, member, proto) = join_now(
        &mut a,
        &join_req(&group, "", "consumer", &["range", "roundrobin"]),
    )
    .await?;
    assert_eq!((e, proto.as_deref()), (error_codes::NONE, Some("range")));
    assert_eq!(
        sync_leader(&mut a, &group, generation, &member).await?,
        error_codes::NONE
    );

    for (what, req) in [
        (
            "protocol type connect",
            join_req(&group, "", "connect", &["range"]),
        ),
        (
            "no common protocol",
            join_req(&group, "", "consumer", &["cooperative-sticky"]),
        ),
        ("empty protocol list", join_req(&group, "", "consumer", &[])),
    ] {
        let mut c = conn().await?;
        let (e, _, _, proto) = join_now(&mut c, &req).await?;
        println!("{}: error {} protocol_name {:?}", what, e, proto);
        assert_eq!(e, error_codes::INCONSISTENT_GROUP_PROTOCOL, "{}", what);
        assert_eq!(
            proto.as_deref(),
            Some(""),
            "v5 error carries an empty, non-null name"
        );
    }
    let mut c = conn().await?;
    let (e, ..) = join_now(&mut c, &join_req(&fresh, "", "consumer", &[])).await?;
    assert_eq!(
        e,
        error_codes::INCONSISTENT_GROUP_PROTOCOL,
        "empty protocols, new group"
    );

    let (e, ..) = join_now(
        &mut c,
        &join_req(&group, "no-such-member", "consumer", &["range"]),
    )
    .await?;
    assert_eq!(e, error_codes::UNKNOWN_MEMBER_ID);
    let (e, ..) = join_now(
        &mut c,
        &join_req(&fresh, "no-such-member", "consumer", &["range"]),
    )
    .await?;
    assert_eq!(e, error_codes::UNKNOWN_MEMBER_ID);
    let (_, state, _, _) = describe(&fresh).await?;
    assert_eq!(state, "Dead", "refused joins create no group");

    let (e, state, ptype, members) = describe(&group).await?;
    assert_eq!(
        (e, state.as_str(), ptype.as_str(), members),
        (error_codes::NONE, "Stable", "consumer", 1)
    );
    assert_eq!(leave(&mut a, &group, &member).await?, error_codes::NONE);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// With real clients: an rdkafka consumer using the cooperative-sticky assignor
/// joining a group whose member uses range gets INCONSISTENT_GROUP_PROTOCOL, and the existing
/// member keeps its assignment and keeps consuming (no rebalance is started by the refused join).
pub async fn test_join_with_no_common_assignor_is_refused() -> TestResult {
    println!("=== Test: a consumer with no common assignor is refused ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("assignor").await;
    let group = ctx.unique_group("assignor").await;
    produce_one(&topic).await?;

    let make = |strategy: &str| -> DynResult<BaseConsumer> {
        Ok(ClientConfig::new()
            .set("bootstrap.servers", get_bootstrap_servers())
            .set("broker.address.family", "v4")
            .set("group.id", &group)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "10000")
            .set("partition.assignment.strategy", strategy)
            .create()?)
    };
    let a = make("range")?;
    a.subscribe(&[&topic])?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut got = false;
    while !got && Instant::now() < deadline {
        got = matches!(a.poll(Duration::from_millis(300)), Some(Ok(_)));
    }
    assert!(got, "range consumer consumes");
    let (_, state, _, members) = describe(&group).await?;
    assert_eq!((state.as_str(), members), ("Stable", 1));

    let b = make("cooperative-sticky")?;
    b.subscribe(&[&topic])?;
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut refused = false;
    while !refused && Instant::now() < deadline {
        // keep A alive
        let _ = a.poll(Duration::from_millis(100));
        if let Some(Err(e)) = b.poll(Duration::from_millis(200)) {
            println!("B: {}", e);
            if let KafkaError::Global(RDKafkaErrorCode::InconsistentGroupProtocol)
            | KafkaError::MessageConsumption(RDKafkaErrorCode::InconsistentGroupProtocol) = e
            {
                refused = true;
            } else if e.to_string().contains("nconsistent") {
                refused = true;
            }
        }
    }
    assert!(refused, "B must be told INCONSISTENT_GROUP_PROTOCOL");

    // A is still the only member, Stable, with its partition, and still consumes new records.
    let (_, state, _, members) = describe(&group).await?;
    assert_eq!((state.as_str(), members), ("Stable", 1), "group unaffected");
    assert_eq!(a.assignment()?.count(), 1);
    produce_one(&topic).await?;
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut again = false;
    while !again && Instant::now() < deadline {
        again = matches!(a.poll(Duration::from_millis(300)), Some(Ok(_)));
    }
    assert!(again, "A keeps consuming");
    drop(b);
    drop(a);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A group with only committed offsets is an Empty "consumer" group for ListGroups and
/// DescribeGroups, including after a worker restart (which empties the in-memory
/// coordinator), so `kafka-consumer-groups --describe` finds it; DeleteGroups deletes it and
/// then answers GROUP_ID_NOT_FOUND. Uses a real rdkafka consumer group to commit.
/// Sequential: restarts the listener worker.
pub async fn test_offsets_only_group_is_listed_after_restart() -> TestResult {
    println!("=== Test: offsets-only groups are Empty groups after a restart ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("offsets-only").await;
    let group = ctx.unique_group("offsets-only").await;
    produce_one(&topic).await?;

    {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", get_bootstrap_servers())
            .set("broker.address.family", "v4")
            .set("group.id", &group)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "10000")
            .create()?;
        consumer.subscribe(&[&topic])?;
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut got = false;
        while !got && Instant::now() < deadline {
            if let Some(Ok(_)) = consumer.poll(Duration::from_millis(500)) {
                got = true;
            }
        }
        assert!(got, "consumer read the record");
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&topic, 0, rdkafka::Offset::Offset(1))?;
        consumer.commit(&tpl, rdkafka::consumer::CommitMode::Sync)?;
        consumer.unsubscribe();
    } // dropped: leaves the group

    let db = create_db_client().await?;
    crate::auth::restart_listener(&db, &[], &[]).await?;
    println!("✅ listener worker restarted (coordinator state gone)\n");

    let listed = list_groups().await?;
    let entry = listed.iter().find(|g| g.0 == group);
    assert_eq!(
        entry.map(|g| (g.1.as_str(), g.2.as_str())),
        Some(("consumer", "Empty")),
        "ListGroups: {:?}",
        listed
            .iter()
            .filter(|g| g.0.starts_with("offsets-only"))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        describe(&group).await?,
        (error_codes::NONE, "Empty".into(), "consumer".into(), 0)
    );
    println!("✅ ListGroups / DescribeGroups report the group as Empty \"consumer\"\n");

    // rdkafka's group listing (ListGroups + DescribeGroups) agrees.
    let (state, protocol_type) = {
        let probe: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", get_bootstrap_servers())
            .set("broker.address.family", "v4")
            .create()?;
        let groups = probe.fetch_group_list(Some(&group), Duration::from_secs(10))?;
        let g = groups
            .groups()
            .iter()
            .find(|g| g.name() == group)
            .ok_or("rdkafka group list misses the group")?;
        (g.state().to_string(), g.protocol_type().to_string())
    };
    assert_eq!(
        (state.as_str(), protocol_type.as_str()),
        ("Empty", "consumer")
    );

    assert_eq!(delete_group(&group).await?, error_codes::NONE);
    assert_eq!(delete_group(&group).await?, error_codes::GROUP_ID_NOT_FOUND);
    assert_eq!(describe(&group).await?.1, "Dead");
    let left: i64 = ctx
        .db()
        .query_one(
            "SELECT count(*) FROM kafka.consumer_offsets WHERE group_id = $1",
            &[&group],
        )
        .await?
        .get(0);
    assert_eq!(left, 0);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
