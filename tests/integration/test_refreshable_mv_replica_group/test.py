import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

# Two replicas of the same Replicated database, assigned to different replica groups
# (like two services of a ClickHouse Cloud warehouse).
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/group_rw1.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": "shard1", "replica": "1"},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/group_rw2.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": "shard1", "replica": "2"},
)
nodes = [node1, node2]

test_idx = 0


def wait_until_view_registered(node, name):
    assert_eq_with_retry(
        node,
        f"select count() from system.view_refreshes where database = 're' and view = '{name}'",
        "1\n",
        retry_count=60,
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture
def replicated_db():
    global test_idx
    for node in nodes:
        node.query(
            f"create database re engine = Replicated('/test/re_{test_idx}', 'shard1', '{{replica}}')"
        )
    yield
    for node in nodes:
        node.query("drop database if exists re sync")
    test_idx += 1


def create_pinned_view(name, group, create_node=node1):
    create_node.query(
        f"create materialized view re.{name} refresh after 1 second settings replica_group = '{group}'"
        f" append (r String) engine ReplicatedMergeTree order by r"
        f" as select getMacro('replica') as r"
    )
    for node in nodes:
        node.query("system sync database replica re")
        wait_until_view_registered(node, name)


def wait_for_refreshes(node, name, min_count):
    for _attempt in range(120):
        if int(node.query(f"select count() from re.{name}")) >= min_count:
            return
        time.sleep(0.5)
    raise Exception(f"view {name} didn't reach {min_count} refreshes in time")


def test_scheduled_refreshes_pinned_to_group(started_cluster, replicated_db):
    # Pin to node1's group; all scheduled refreshes must run on node1,
    # regardless of which node created the view.
    create_pinned_view("pin1", "rw1", create_node=node2)
    wait_for_refreshes(node1, "pin1", 3)
    node1.query("system sync replica re.pin1")
    assert node1.query("select distinct r from re.pin1") == "1\n"
    assert (
        node1.query(
            "select last_refresh_replica from system.view_refreshes where view = 'pin1'"
        )
        == "1\n"
    )
    # The excluded replica just shows the view as scheduled, not disabled.
    assert (
        node2.query(
            "select status in ('Scheduled', 'RunningOnAnotherReplica') from system.view_refreshes where view = 'pin1'"
        )
        == "1\n"
    )

    # And the other way around: pin to node2's group.
    create_pinned_view("pin2", "rw2", create_node=node1)
    wait_for_refreshes(node2, "pin2", 3)
    node2.query("system sync replica re.pin2")
    assert node2.query("select distinct r from re.pin2") == "2\n"


def test_manual_refresh_not_restricted(started_cluster, replicated_db):
    # SYSTEM REFRESH VIEW runs on the replica that receives it, even outside the pinned group.
    create_pinned_view("manual", "rw1")
    wait_for_refreshes(node1, "manual", 1)
    node2.query("system refresh view re.manual")
    node2.query("system wait view re.manual")
    node2.query("system sync replica re.manual")
    assert_eq_with_retry(
        node2, "select count() from re.manual where r = '2'", "1\n"
    )


def test_alter_replica_group(started_cluster, replicated_db):
    # Repin the view to the other group with ALTER ... MODIFY REFRESH.
    create_pinned_view("repin", "rw1")
    wait_for_refreshes(node1, "repin", 2)
    node1.query(
        "alter table re.repin modify refresh after 1 second settings replica_group = 'rw2'"
    )
    node1.query("truncate table re.repin")
    wait_for_refreshes(node2, "repin", 2)
    node2.query("system sync replica re.repin")
    assert node2.query("select distinct r from re.repin") == "2\n"


def test_no_replica_in_group(started_cluster, replicated_db):
    # Pinning to a group with no replicas: scheduled refreshes just don't happen.
    create_pinned_view("nowhere", "no_such_group")
    time.sleep(5)
    assert node1.query("select count() from re.nowhere") == "0\n"
    for node in nodes:
        assert (
            node.query(
                "select status from system.view_refreshes where view = 'nowhere'"
            )
            == "Scheduled\n"
        )
    # Manual refresh still works as an escape hatch.
    node1.query("system refresh view re.nowhere")
    node1.query("system wait view re.nowhere")
    assert node1.query("select count() from re.nowhere") == "1\n"


def test_incompatible_with_all_replicas(started_cluster, replicated_db):
    assert "INCORRECT_QUERY" in node1.query_and_get_error(
        "create materialized view re.bad refresh after 1 second settings all_replicas = 1, replica_group = 'rw1'"
        " append (r String) engine ReplicatedMergeTree order by r as select 'x' as r"
    )
    # Also can't sneak the combination in with ALTER on an existing all_replicas view.
    node1.query(
        "create materialized view re.allrep refresh after 1 second settings all_replicas = 1"
        " append (r String) engine ReplicatedMergeTree order by r as select 'x' as r"
    )
    assert "INCORRECT_QUERY" in node1.query_and_get_error(
        "alter table re.allrep modify refresh after 1 second settings all_replicas = 1, replica_group = 'rw1'"
    )
