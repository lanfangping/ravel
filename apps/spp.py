from ravel.app import AppConsole

class SppConsole(AppConsole):
    def do_echo(self):
        print("test")

shortcut = "tt"
description = "Test: shortest path policy"
console = SppConsole
