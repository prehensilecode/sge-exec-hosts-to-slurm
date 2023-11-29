#!/usr/bin/env python3
import sys
import os
import subprocess
import re

_DEBUG = False
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


def get_host_resources(hostname):
    # Return: dict of resources in Slurm nomenclature
    # look at
    # - complexes for GPUs
    # - load values for socket/core/thread, mem, tmp
    #
    # Example
    # $ qconf -se 2117ga001.bicic.local
    # hostname              2117ga001.bicic.local
    # load_scaling          NONE
    # complex_values        A100=TRUE,brats=TRUE,cuda11.2=TRUE,gpu=2,gpu_A100=TRUE, \
    #                       h_vmem=1000G,Intel_Xeon=TRUE,openmpi=TRUE,slots=128, \
    #                       tmpfree=3075933436k
    # load_values           arch=lx-amd64,num_proc=128,mem_total=1031323.867188M, \
    #                       swap_total=4095.996094M,virtual_total=1035419.863281M, \
    #                       m_topology=SCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTSCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTTCTT, \
    #                       m_socket=2,m_core=64,m_thread=128,load_avg=3.640000, \
    #                       load_short=3.810000,load_medium=3.640000, \
    #                       load_long=4.910000,mem_free=1001792.296875M, \
    #                       swap_free=4095.996094M,virtual_free=1005888.292969M, \
    #                       mem_used=29531.570312M,swap_used=0.000000M, \
    #                       virtual_used=29531.570312M,cpu=3.000000, \
    #                       m_topology_inuse=ScttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttScttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttcttctt, \
    #                       tmpfree=3075933320k,tmptot=3370532884k, \
    #                       tmpused=294599564k,np_load_avg=0.028438, \
    #                       np_load_short=0.029766,np_load_medium=0.028438, \
    #                       np_load_long=0.038359
    # processors            128
    # user_lists            NONE
    # xuser_lists           NONE
    # projects              NONE
    # xprojects             NONE
    # usage_scaling         NONE
    # report_variables      NONE
    #
    # And example slurm.conf lines
    # NodeName=2115ga[001-004]  Sockets=2 CoresPerSocket=32 ThreadsPerCore=2 RealMemory=1000000 TmpDisk=3291000 Gres=gpu:A100:2
    # NodeName=compute-fed1     Sockets=2 CoresPerSocket=24 ThreadsPerCore=2 RealMemory=773000 TmpDisk=780000 Features=SGX,AVX,AVX2

    qconf_output = subprocess.run([_QCONF, '-se', hostname], capture_output=True, text=True)
    host_resources = {}
    host_resources[hostname] = {}
    values_dict = {}
    line_list = []
    key = None
    value_str = ''
    for line in qconf_output.stdout.split('\n'):
        if re.match(r'^hostname', line) or \
                re.match(r'^load_scaling', line) or \
                re.match(r'^processors', line) or \
                re.match(r'^user_lists', line) or \
                re.match(r'^xuser_lists', line) or \
                re.match(r'^projects', line) or \
                re.match(r'^xprojects', line) or \
                re.match(r'^usage_scaling', line) or \
                re.match(r'^report_variables', line):
                    continue
        else:
            # first line of block
            line_list = line.strip().split()

            # drop trailing backslash
            if '\\' in line_list:
                line_list.remove('\\')

            if _DEBUG:
                print(f'DEBUG: line_list = {line_list}')

            if len(line_list) == 2:
                key = line_list[0]
                value_str = line_list[1]
                if _DEBUG:
                    print(f'DEBUG: value_str = {value_str}')
            elif len(line_list) > 0:
                value_str = ''.join([value_str, line_list[0]])
                if _DEBUG:
                    print(f'DEBUG: value_str = {value_str}')
            elif len(line_list) == 0:
                continue

            values_dict[key] = value_str

    for k, v in values_dict.items():
        if _DEBUG:
            print(f'DEBUG: k = {k}, v = {v}')

        expanded_values = v.split(',')
        ev_dict = {}
        for ev in expanded_values:
            key, val = ev.split('=')

            if val == 'TRUE':
                val = True
            elif val == 'FALSE':
                val = False

            int_vals = ('m_socket', 'm_core', 'm_thread', 'num_proc')
            if key in int_vals:
                val = int(val)

            ev_dict[key] = val

        host_resources[hostname] = {k: ev_dict}

    print(host_resources)

def main():
    allhosts = get_hosts()

    if _DEBUG:
        print(allhosts)

    for h in allhosts[:3]:
        get_host_resources(h)


if __name__ == '__main__':
    main()
