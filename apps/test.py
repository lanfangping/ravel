from ravel.app import AppConsole

class TestConsole(AppConsole):
    def do_test(self):
        print("test")

shortcut = "test"
description = "Test: shortest path policy"
console = TestConsole
