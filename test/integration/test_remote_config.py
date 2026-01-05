# Copyright 2025 ReductSoftware UG
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

"""Integration test for remote config."""

from reductstore_agent.utils import get_or_create_event_loop

from ..utils import (
    create_remote_bucket,
    fetch_and_count_records,
    get_test_pipelines_yaml_cdr,
    get_test_pipelines_yaml_mcap,
    publish_and_spin_messages_multi,
    remote_bucket_params,
    remove_remote_bucket,
    upload_remote_config,
)


def test_recorder_loads_remote_config_on_startup(
    publisher_node,
    remote_publishers,
    reduct_client,
):
    """Test that the recorder loads configuration from remote storage on startup."""
    # Upload remote config
    loop = get_or_create_event_loop()
    remote_config_yaml = get_test_pipelines_yaml_mcap()
    loop.run_until_complete(
        create_remote_bucket(
            reduct_client,
            bucket_name="remote_bucket",
        )
    )
    loop.run_until_complete(
        upload_remote_config(
            reduct_client,
            bucket_name="remote_bucket",
            entry_name="remote_config",
            config_yaml=remote_config_yaml,
        )
    )

    # Start recorder
    from reductstore_agent.recorder import Recorder

    params = remote_bucket_params()
    node = Recorder(parameter_overrides=params)

    # Publish messages
    publish_and_spin_messages_multi(
        publisher_node,
        remote_publishers,
        node,
        n_msg=7,
    )

    # Verify records
    records_pipeline_one = loop.run_until_complete(
        fetch_and_count_records(
            reduct_client,
            bucket_name="test_bucket",
            entry_name="pipeline_one",
        )
    )
    records_pipeline_two = loop.run_until_complete(
        fetch_and_count_records(
            reduct_client,
            bucket_name="test_bucket",
            entry_name="pipeline_two",
        )
    )
    # Cleanup remote bucket
    loop.run_until_complete(
        remove_remote_bucket(
            reduct_client,
            bucket_name="remote_bucket",
        )
    )

    assert len(records_pipeline_one) == 1, "Expected 1 record in pipeline_one."
    assert len(records_pipeline_two) == 1, "Expected 1 record in pipeline_two."
    node.destroy_node()


def test_recorder_loads_remote_config_cdr(
    publisher_node,
    remote_publishers,
    reduct_client,
):
    """Test that the recorder loads configuration with CDR mode."""
    # Upload remote config
    loop = get_or_create_event_loop()
    remote_config_yaml = get_test_pipelines_yaml_cdr()
    loop.run_until_complete(
        create_remote_bucket(
            reduct_client,
            bucket_name="remote_bucket",
        )
    )
    loop.run_until_complete(
        upload_remote_config(
            reduct_client,
            bucket_name="remote_bucket",
            entry_name="remote_config",
            config_yaml=remote_config_yaml,
        )
    )
    # Start recorder
    from reductstore_agent.recorder import Recorder

    params = remote_bucket_params()
    node = Recorder(parameter_overrides=params)
    # Publish messages
    publish_and_spin_messages_multi(
        publisher_node,
        remote_publishers,
        node,
        n_msg=5,
    )
    # Flush on shutdown to ensure all data is written
    for state in node.pipeline_states.values():
        state.writer.flush_on_shutdown()
    # Verify records
    records_pipeline_one = loop.run_until_complete(
        fetch_and_count_records(
            reduct_client,
            bucket_name="test_bucket",
            entry_name="pipeline_one",
        )
    )
    records_pipeline_two = loop.run_until_complete(
        fetch_and_count_records(
            reduct_client,
            bucket_name="test_bucket",
            entry_name="pipeline_two",
        )
    )
    # Cleanup remote bucket
    loop.run_until_complete(
        remove_remote_bucket(
            reduct_client,
            bucket_name="remote_bucket",
        )
    )

    assert len(records_pipeline_one) == 5, "Expected 5 records in pipeline_one."
    assert len(records_pipeline_two) == 5, "Expected 5 records in pipeline_two."
    node.destroy_node()


def test_recorder_remote_config_periodic_pull(
    publisher_node,
    remote_publishers,
    reduct_client,
):
    """Test that the recorder periodically pulls updated configuration from remote."""
    # Upload initial remote config
    loop = get_or_create_event_loop()
    remote_config_yaml_initial = get_test_pipelines_yaml_mcap()
    loop.run_until_complete(
        create_remote_bucket(
            reduct_client,
            bucket_name="remote_bucket",
        )
    )
    loop.run_until_complete(
        upload_remote_config(
            reduct_client,
            bucket_name="remote_bucket",
            entry_name="remote_config",
            config_yaml=remote_config_yaml_initial,
        )
    )

    # Start recorder
    from reductstore_agent.recorder import Recorder

    params = remote_bucket_params(5)

    node = Recorder(parameter_overrides=params)

    # Publish messages
    publish_and_spin_messages_multi(
        publisher_node,
        remote_publishers,
        node,
        n_msg=7,
    )

    # Upload updated remote config
    remote_config_yaml_updated = get_test_pipelines_yaml_cdr()
    loop.run_until_complete(
        upload_remote_config(
            reduct_client,
            bucket_name="remote_bucket",
            entry_name="remote_config",
            config_yaml=remote_config_yaml_updated,
        )
    )
    # Wait for pull frequency to elapse and publish more messages
    import time

    import rclpy

    start = time.time()
    while time.time() - start < 10.0:  # spin for 10 seconds
        rclpy.spin_once(node, timeout_sec=0.1)

    publish_and_spin_messages_multi(
        publisher_node,
        remote_publishers,
        node,
        n_msg=5,
    )

    # Flush on shutdown to ensure all data is written
    for state in node.pipeline_states.values():
        state.writer.flush_on_shutdown()

    # Verify records
    records_pipeline_one = loop.run_until_complete(
        fetch_and_count_records(
            reduct_client,
            bucket_name="test_bucket",
            entry_name="pipeline_one",
        )
    )
    records_pipeline_two = loop.run_until_complete(
        fetch_and_count_records(
            reduct_client,
            bucket_name="test_bucket",
            entry_name="pipeline_two",
        )
    )
    # Cleanup remote bucket
    loop.run_until_complete(
        remove_remote_bucket(
            reduct_client,
            bucket_name="remote_bucket",
        )
    )
    assert len(records_pipeline_one) == 6, "Expected 6 records in pipeline_one."
    assert len(records_pipeline_two) == 6, "Expected 6 records in pipeline_two."
    node.destroy_node()
