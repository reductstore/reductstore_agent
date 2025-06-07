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
