from ravel.app import AppConsole

class SppConsole(AppConsole):
    def do_echo(self):
        print("test")
    
    def do_adddata(self, line):
        args = line.split()
        if len(args) != 2:
            print("Invalid syntax") 
            return
        
        rib_file = args[0]
        upd_file = args[1]



shortcut = "bgp"
description = "BGP simulation"
console = SppConsole
