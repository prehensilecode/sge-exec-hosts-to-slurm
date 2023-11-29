#!/usr/bin/env python3
import sys
import os
import subprocess
import re

_QCONF = '/cbica/software/external/sge/8.1.9-1/bin/lx-amd64/qconf'


def get_hosts():
    qconf_output = subprocess.run([_QCONF, '-sel'], capture_output=True, text=True)
    allhosts = qconf_output.stdout.split()

    # non-compute hosts
    #   cubic-login1.bicic.local
    #   cubic-login2.uphs.upenn.edu
    #   cubic-login3.bicic.local
    #   cubic-login4.uphs.upenn.edu
    #   cubic-login5.bicic.local
    #   cubic-sattertt1.bicic.local
    login_node_pat = re.compile(r'^cubic-login')

    to_remove = ['cubic-sattertt1.bicic.local']
    for h in allhosts:
        if login_node_pat.match(h):
            to_remove.append(h)

    for r in to_remove:
        allhosts.remove(r)

    return allhosts


def get_host_resource(hostname):
    pass


def main():
    allhosts = get_hosts()
    print(allhosts)


if __name__ == '__main__':
    main()
