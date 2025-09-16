import json

from osprey.worker.lib.etcd.tree import ReadOnlyEtcdTree as EtcdTree


def test_set(etcd_client, etcd_key, wait_for_condition):
    """Test that setting a sub path deeper in the tree works."""
    etcd_tree = EtcdTree(etcd_key)
    etcd_tree.watch()

    wait_for_condition(lambda: etcd_tree.get('%s/a' % etcd_key) is None)
    wait_for_condition(lambda: etcd_tree.get('%s/a/b/c' % etcd_key) is None)

    node_value_dict = {'foo': 'bar', 'baz': 1}
    etcd_client.set('%s/a/b/c' % etcd_key, json.dumps(node_value_dict))

    wait_for_condition(lambda: etcd_tree.get('%s/a' % etcd_key) is None)
    wait_for_condition(lambda: json.loads(etcd_tree.get('%s/a/b/c' % etcd_key, '{}')) == node_value_dict)


def test_delete(etcd_client, etcd_key, wait_for_condition):
    """Test that deleting a subtree deeper in the tree deletes recursively."""
    etcd_tree = EtcdTree(etcd_key)
    etcd_tree.watch()

    wait_for_condition(lambda: etcd_tree.get('%s/a' % etcd_key) is None)
    wait_for_condition(lambda: etcd_tree.get('%s/a/b/e' % etcd_key) is None)
    wait_for_condition(lambda: etcd_tree.get('%s/a/b/c/d' % etcd_key) is None)
    wait_for_condition(lambda: etcd_tree.get('%s/a/b/c/e' % etcd_key) is None)
    wait_for_condition(lambda: etcd_tree.get('%s/a/b/c/f' % etcd_key) is None)

    node_value_dict = {'foo': 'bar', 'baz': 1}
    etcd_client.set('%s/a/b/e' % etcd_key, json.dumps(node_value_dict))
    etcd_client.set('%s/a/b/c/d' % etcd_key, json.dumps(node_value_dict))
    etcd_client.set('%s/a/b/c/e' % etcd_key, json.dumps(node_value_dict))
    etcd_client.set('%s/a/b/c/f' % etcd_key, json.dumps(node_value_dict))

    wait_for_condition(lambda: json.loads(etcd_tree.get('%s/a/b/e' % etcd_key, '{}')) == node_value_dict)
    wait_for_condition(lambda: json.loads(etcd_tree.get('%s/a/b/c/d' % etcd_key, '{}')) == node_value_dict)
    wait_for_condition(lambda: json.loads(etcd_tree.get('%s/a/b/c/e' % etcd_key, '{}')) == node_value_dict)
    wait_for_condition(lambda: json.loads(etcd_tree.get('%s/a/b/c/f' % etcd_key, '{}')) == node_value_dict)

    # Delete one leaf node.
    etcd_client.delete('%s/a/b/c/d' % etcd_key)
    wait_for_condition(lambda: etcd_tree.get('%s/a/b/c/d' % etcd_key) is None)
    assert json.loads(etcd_tree.get('%s/a/b/c/e' % etcd_key, '{}')) == node_value_dict
    assert json.loads(etcd_tree.get('%s/a/b/c/f' % etcd_key, '{}')) == node_value_dict
    assert json.loads(etcd_tree.get('%s/a/b/e' % etcd_key, '{}')) == node_value_dict

    # Delete a proper subtree.
    etcd_client.delete('%s/a/b/c' % etcd_key, directory=True, recursive=True)
    wait_for_condition(lambda: etcd_tree.get('%s/a/b/c' % etcd_key) is None)
    wait_for_condition(lambda: etcd_tree.get('%s/a/b/c/d' % etcd_key) is None)  # from the previous leaf node deletion
    wait_for_condition(lambda: etcd_tree.get('%s/a/b/c/e' % etcd_key) is None)
    wait_for_condition(lambda: etcd_tree.get('%s/a/b/c/f' % etcd_key) is None)

    # Delete the root which wipes out everything.
    etcd_client.delete(etcd_key, recursive=True)
    wait_for_condition(lambda: etcd_tree.get('%s/a' % etcd_key) is None)
    wait_for_condition(lambda: etcd_tree.get('%s/a/b/e' % etcd_key) is None)
    wait_for_condition(lambda: etcd_tree.get('%s/a/b/c/d' % etcd_key) is None)
    wait_for_condition(lambda: etcd_tree.get('%s/a/b/c/e' % etcd_key) is None)
    wait_for_condition(lambda: etcd_tree.get('%s/a/b/c/f' % etcd_key) is None)


def test_update(etcd_client, etcd_key, wait_for_condition):
    etcd_tree = EtcdTree(etcd_key)
    etcd_tree.watch()

    wait_for_condition(lambda: etcd_tree.get('%s/a/b/e' % etcd_key) is None)

    node_value_dict = {'foo': 'bar', 'baz': 1}
    etcd_client.set('%s/a/b/e' % etcd_key, json.dumps(node_value_dict))

    wait_for_condition(lambda: json.loads(etcd_tree.get('%s/a/b/e' % etcd_key, '{}')) == node_value_dict)

    node_value_dict['quz'] = True
    etcd_client.set('%s/a/b/e' % etcd_key, json.dumps(node_value_dict))

    wait_for_condition(lambda: json.loads(etcd_tree.get('%s/a/b/e' % etcd_key, '{}')) == node_value_dict)


def test_tree_path_watchers(etcd_client, etcd_key, wait_for_condition):
    paths_to_values_dict = {
        '%s/foo' % etcd_key: {'foo': 'bar', 'baz': 1},
        '%s/a/foo' % etcd_key: {'a': 1, 'b': True},
        '%s/a/b/c/bar' % etcd_key: {'c': 'd', 'e': 2, 'f': {'g': [1, 2, '3'], 'h': False}},
    }

    # Set values first before watching.
    for path, value in paths_to_values_dict.items():
        etcd_client.set(path, json.dumps(value))

    etcd_tree = EtcdTree(etcd_key)
    watched_node_paths_to_values = {}

    def _watch_callback(path, value):
        if value is None:
            del watched_node_paths_to_values[path]
        else:
            watched_node_paths_to_values[path] = json.loads(value)

    for path in paths_to_values_dict.keys():
        etcd_tree.watch_node(path, _watch_callback)

    for path, value in paths_to_values_dict.items():
        wait_for_condition(lambda: json.loads(etcd_tree.get(path, '{}')) == value)
        wait_for_condition(lambda: watched_node_paths_to_values.get(path) == value)

    # Now update stuff as the tree is watching branches.
    for _, d in paths_to_values_dict.items():
        d['new_value'] = 123

    for path, value in paths_to_values_dict.items():
        etcd_client.set(path, json.dumps(value))

    for path, value in paths_to_values_dict.items():
        wait_for_condition(lambda: json.loads(etcd_tree.get(path, '{}')) == paths_to_values_dict[path])
        wait_for_condition(lambda: watched_node_paths_to_values.get(path) == paths_to_values_dict[path])

    for path, value in paths_to_values_dict.items():
        etcd_client.delete(path)
        wait_for_condition(lambda: etcd_tree.get(path) is None)
        wait_for_condition(lambda: watched_node_paths_to_values.get(path) is None)

    # now, stop watching, then set stuff in etcd, and watch again and get the values.
    # etcd_tree.stop_watching()

    for path, value in paths_to_values_dict.items():
        etcd_client.set(path, json.dumps(value))

    etcd_tree.watch()

    for path, value in paths_to_values_dict.items():
        wait_for_condition(lambda: json.loads(etcd_tree.get(path, '{}')) == value)
        wait_for_condition(lambda: watched_node_paths_to_values.get(path) == value)
