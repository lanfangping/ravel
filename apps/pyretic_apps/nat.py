from pyretic.lib.corelib import *
from pyretic.lib.std import *

ip1 = IPAddr('10.0.0.1')
p = IPAddr('10.0.0.11')

policies = [ match(srcip=ip1) >> modify(srcip=p),
             match(dstip=p) >> modify(dstip=ip1) ]

policy = parallel(policies)

def main():
    return policy
