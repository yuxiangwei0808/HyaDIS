import ray


def get_ip_address():
    assert ray.worker._global_node is not None, "Ray is not initialized."
    return ray.worker._global_node.node_ip_address


def get_unused_port():
    assert ray.worker._global_node is not None, "Ray is not initialized."
    return ray.worker._global_node._get_unused_port()
