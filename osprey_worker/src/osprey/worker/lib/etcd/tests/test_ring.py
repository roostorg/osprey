from __future__ import absolute_import

import json
from collections import Counter

import pytest
from osprey.worker.lib.etcd.ring import EtcdHashRing, EtcdHashRingMember, EtcdHashRingTopology
from six.moves import range


@pytest.fixture(scope='function')
def ring(etcd_client, etcd_key):
    return EtcdHashRing(etcd_client=etcd_client, key=etcd_key)


def test_ring_no_members(ring):
    assert ring.members == []
    assert ring.select(1) is None
    assert ring.select(1, 2) == []


def test_ring_with_legacy_data(ring, etcd_key, etcd_client, wait_for_condition):
    members = ['test-1', 'test-2', 'test-3']
    etcd_client.set(etcd_key, json.dumps(members))

    serialized_members = [{'name': name, 'num_replicas': 512} for name in members]
    wait_for_condition(
        lambda: EtcdHashRingTopology.to_json(ring.members) == EtcdHashRingTopology.to_json(serialized_members)
    )


def test_ring_with_new_format_data(ring, etcd_key, etcd_client, wait_for_condition):
    members = ['test-1', 'test-2', 'test-3']
    serialized_members = [{'name': name, 'num_replicas': 2 ** (i + 1)} for i, name in enumerate(members)]

    etcd_client.set(etcd_key, json.dumps(serialized_members))
    wait_for_condition(
        lambda: EtcdHashRingTopology.to_json(ring.members) == EtcdHashRingTopology.to_json(serialized_members)
    )


def test_ring_with_mixed_data(ring, etcd_key, etcd_client, wait_for_condition):
    members = [{'name': name, 'num_replicas': 2 ** (i + 1)} for i, name in enumerate(['test-1', 'test-2', 'test-3'])]
    serialized_members = list(members) + ['test-4']

    etcd_client.set(etcd_key, json.dumps(serialized_members))

    wait_for_condition(
        lambda: EtcdHashRingTopology.to_json(ring.members)
        == EtcdHashRingTopology.to_json(members + [{'name': 'test-4', 'num_replicas': 512}])
    )


def test_ring_with_overrides(ring, etcd_key, etcd_client, wait_for_condition):
    members = ['test-1', 'test-2', 'test-3']
    members = [{'name': name, 'num_replicas': 2 ** (i + 1)} for i, name in enumerate(members)]
    mapped_overrides = {1: ['other'], 2: ['a', 'b', 'c']}

    serialized_overrides = [[k] + v for k, v in mapped_overrides.items()]
    serialized_json = {'schema': 'v1', 'members': members, 'overrides': serialized_overrides}

    etcd_client.set(etcd_key, json.dumps(serialized_json))

    wait_for_condition(
        lambda: EtcdHashRingTopology.to_json(ring.members, ring.overrides)
        == EtcdHashRingTopology.to_json(members, mapped_overrides)
    )


def test_ring_add_member_as_string(ring, etcd_key, etcd_client, wait_for_condition):
    ring.add('test-1')
    serialized_value = json.loads(etcd_client.get(etcd_key).node.value)
    assert serialized_value == [{'num_replicas': 512, 'name': 'test-1'}]
    wait_for_condition(
        lambda: EtcdHashRingTopology.to_json(ring.members) == EtcdHashRingTopology.to_json(serialized_value)
    )


def test_ring_add_member_as_member_object(ring, etcd_key, etcd_client, wait_for_condition):
    ring.add(EtcdHashRingMember(name='test-1', num_replicas=128))
    serialized_value = json.loads(etcd_client.get(etcd_key).node.value)
    assert serialized_value == [{'num_replicas': 128, 'name': 'test-1'}]
    wait_for_condition(
        lambda: EtcdHashRingTopology.to_json(ring.members) == EtcdHashRingTopology.to_json(serialized_value)
    )


def test_ring_add_member_as_dict_object(ring, etcd_key, etcd_client, wait_for_condition):
    ring.add({'num_replicas': 128, 'name': 'test-1'})
    serialized_value = json.loads(etcd_client.get(etcd_key).node.value)
    assert serialized_value == [{'num_replicas': 128, 'name': 'test-1'}]
    wait_for_condition(
        lambda: EtcdHashRingTopology.to_json(ring.members) == EtcdHashRingTopology.to_json(serialized_value)
    )


def test_ring_add_remove_operations(ring, etcd_key, etcd_client, wait_for_condition):
    ring.add({'num_replicas': 128, 'name': 'test-1'}, {'num_replicas': 128, 'name': 'test-2'})
    serialized_value = json.loads(etcd_client.get(etcd_key).node.value)
    assert serialized_value == [{'num_replicas': 128, 'name': 'test-1'}, {'num_replicas': 128, 'name': 'test-2'}]
    wait_for_condition(
        lambda: EtcdHashRingTopology.to_json(ring.members) == EtcdHashRingTopology.to_json(serialized_value)
    )

    ring.remove('test-1')
    serialized_value = json.loads(etcd_client.get(etcd_key).node.value)
    assert serialized_value == [{'num_replicas': 128, 'name': 'test-2'}]
    wait_for_condition(
        lambda: EtcdHashRingTopology.to_json(ring.members) == EtcdHashRingTopology.to_json(serialized_value)
    )


def test_ring_update_known_member(ring, etcd_key, etcd_client, wait_for_condition):
    ring.add({'num_replicas': 128, 'name': 'test-1'})
    serialized_value = json.loads(etcd_client.get(etcd_key).node.value)
    assert serialized_value == [{'num_replicas': 128, 'name': 'test-1'}]
    wait_for_condition(
        lambda: EtcdHashRingTopology.to_json(ring.members) == EtcdHashRingTopology.to_json(serialized_value)
    )

    ring.update({'num_replicas': 256, 'name': 'test-1'})
    serialized_value = json.loads(etcd_client.get(etcd_key).node.value)
    assert serialized_value == [{'num_replicas': 256, 'name': 'test-1'}]
    wait_for_condition(
        lambda: EtcdHashRingTopology.to_json(ring.members) == EtcdHashRingTopology.to_json(serialized_value)
    )


def test_ring_update_unknown_member(ring, etcd_key, etcd_client, wait_for_condition):
    ring.add({'num_replicas': 128, 'name': 'test-1'})
    serialized_value = json.loads(etcd_client.get(etcd_key).node.value)
    assert serialized_value == [{'num_replicas': 128, 'name': 'test-1'}]
    wait_for_condition(
        lambda: EtcdHashRingTopology.to_json(ring.members) == EtcdHashRingTopology.to_json(serialized_value)
    )

    ring.update({'num_replicas': 256, 'name': 'test-2'})
    serialized_value = json.loads(etcd_client.get(etcd_key).node.value)
    assert serialized_value == [{'num_replicas': 128, 'name': 'test-1'}]
    wait_for_condition(
        lambda: EtcdHashRingTopology.to_json(ring.members) == EtcdHashRingTopology.to_json(serialized_value)
    )


def test_ring_select(ring, wait_for_condition):
    ring.add('test-1', 'test-2', 'test-3')

    wait_for_condition(
        lambda: ring.select(3) == b'test-1'
        and ring.select(6) == b'test-2'
        and ring.select(1) == b'test-3'
        and ring.select('foo') == b'test-2'
    )

    # Testing count of distribution
    node_counts = Counter(ring.select(n) for n in range(10000))
    assert node_counts[b'test-1'] == 3263
    assert node_counts[b'test-2'] == 3332
    assert node_counts[b'test-3'] == 3405


def test_ring_select_with_dynamic_node_weights(ring, wait_for_condition):
    ring.add('test-1', 'test-2', EtcdHashRingMember('test-3', num_replicas=2048))

    wait_for_condition(
        lambda: ring.select(5) == b'test-1' and ring.select(6) == b'test-2' and ring.select(3) == b'test-3'
    )

    # Testing count of distribution
    node_counts = Counter(ring.select(n) for n in range(10000))
    assert node_counts[b'test-1'] == 1654
    assert node_counts[b'test-2'] == 1632
    assert node_counts[b'test-3'] == 6714


def test_ring_update_with_adjusted_node_replicas(ring, wait_for_condition):
    ring.add('test-1', 'test-2', EtcdHashRingMember('test-3', num_replicas=2048))

    wait_for_condition(
        lambda: ring.select(5) == b'test-1' and ring.select(6) == b'test-2' and ring.select(3) == b'test-3'
    )

    # Testing count of distribution
    node_counts = Counter(ring.select(n) for n in range(10000))
    assert node_counts[b'test-1'] == 1654
    assert node_counts[b'test-2'] == 1632
    assert node_counts[b'test-3'] == 6714

    # Testing count of distribution
    topology = EtcdHashRingTopology.from_members(['test-1', 'test-2', 'test-3'], ring._default_num_replicas)
    ring._update(topology)

    wait_for_condition(lambda: ring.select(3) == b'test-1')

    node_counts = Counter(ring.select(n) for n in range(10000))
    assert node_counts[b'test-1'] == 3263
    assert node_counts[b'test-2'] == 3332
    assert node_counts[b'test-3'] == 3405


def test_ring_select_override(ring, wait_for_condition):
    ring.add('test-1', 'test-2', 'test-3')

    wait_for_condition(
        lambda: ring.select(1) == b'test-3' and ring.select(3) == b'test-1' and ring.select(6) == b'test-2'
    )

    ring.overrides = {1: [b'special-a'], 3: [b'test-2', b'test-8', b'test-12']}

    wait_for_condition(
        lambda: ring.select(1) == b'special-a' and ring.select(3) == b'test-2' and ring.select(6) == b'test-2'
    )


def test_ring_select_override_with_secondaries(ring, wait_for_condition):
    ring.add('test-1', 'test-2', 'test-3')

    wait_for_condition(
        lambda: ring.select(1, 2) == [b'test-3', b'test-2', b'test-1']
        and ring.select(3, 2) == [b'test-1', b'test-2', b'test-3']
        and ring.select(6, 2) == [b'test-2', b'test-1', b'test-3']
    )

    ring.overrides = {1: [b'special-a'], 3: [b'test-2', b'test-8']}

    wait_for_condition(
        lambda: ring.select(1, 2) == [b'special-a', b'test-3', b'test-2']
        and ring.select(3, 2) == [b'test-2', b'test-8', b'test-1']
        and ring.select(6, 2) == [b'test-2', b'test-1', b'test-3']
    )


def test_ring_member_equality():
    a = EtcdHashRingMember(name=b'test-1', num_replicas=128)
    b = EtcdHashRingMember(name=b'test-1', num_replicas=128)
    assert a == b


def test_ring_add_updates_existing_member(ring, wait_for_condition):
    a = EtcdHashRingMember(name=b'test', num_replicas=128)
    b = EtcdHashRingMember(name=b'test', num_replicas=256)
    ring.add(a)
    ring.add(b)
    wait_for_condition(lambda: ring.members == [b])


def test_ring_member_with_non_positive_replicas_fails(ring):
    with pytest.raises(ValueError):
        EtcdHashRingMember(name=b'test', num_replicas=0)

    with pytest.raises(ValueError):
        EtcdHashRingMember(name=b'test', num_replicas=-1)


def test_ring_drops_invalid_members(ring, etcd_key, etcd_client, wait_for_condition):
    """This ensures that we drop invalid members instead of hard failing or dropping everything after the member"""
    valid_members = [{'name': 'valid-%d' % i, 'num_replicas': 1} for i in range(0, 4)]
    invalid_member = {'name': 'invalid', 'num_replicas': 0}
    etcd_client.set(etcd_key, json.dumps(valid_members[:2] + [invalid_member] + valid_members[2:]))

    wait_for_condition(
        lambda: EtcdHashRingTopology.to_json(ring.members) == EtcdHashRingTopology.to_json(valid_members)
    )
