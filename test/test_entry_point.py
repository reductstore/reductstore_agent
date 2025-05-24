from ros2_reduct_agent.recorder import main


def test_main_with_keyboard_interrupt(monkeypatch):
    """Simulate KeyboardInterrupt and verify node cleanup."""

    destroyed = {}
    shutdown_called = {}

    class DummyLogger:
        def info(self, msg):
            pass

        def warn(self, msg):
            pass

        def error(self, msg):
            pass

    class DummyNode:
        def get_logger(self):
            return DummyLogger()

        def destroy_node(self):
            destroyed["ok"] = True

    # Patch everything used in recorder.main()
    monkeypatch.setattr(
        "ros2_reduct_agent.recorder.Recorder", lambda **kwargs: DummyNode()
    )
    monkeypatch.setattr("ros2_reduct_agent.recorder.rclpy.init", lambda: None)
    monkeypatch.setattr(
        "ros2_reduct_agent.recorder.rclpy.spin",
        lambda node: (_ for _ in ()).throw(KeyboardInterrupt),
    )
    monkeypatch.setattr("ros2_reduct_agent.recorder.rclpy.ok", lambda: True)
    monkeypatch.setattr(
        "ros2_reduct_agent.recorder.rclpy.shutdown",
        lambda: shutdown_called.setdefault("ok", True),
    )

    main()

    # Check if the node was destroyed and shutdown was called
    assert destroyed.get("ok"), "Node was not destroyed"
    assert shutdown_called.get("ok"), "rclpy.shutdown() was not called"
