#!/usr/bin/env python3
import sys
import os
import subprocess
import re
import argparse

_DEBUG = False
_QCONF = '/cbica/software/external/sge/8.1.9-1/bin/lx-amd64/qconf'

# N.B. SGE shows total number of threads, while Slurm expects 
#      no. of threads per core

def to_MiB(memstr):
    # takes a mem value string from qconf, e.g. 342590.007812M or
    # 599783284k and convert to floating point MiB
    kibi = 1./1024.
    gibi = 1024
    tebi = gibi * gibi
    retval = 0.

    if memstr[-1] == 'k':
        retval = float(memstr[:-1]) * kibi
    elif memstr[-1] == 'M':
        retval = float(memstr[:-1])
    elif memstr[-1] == 'G':
        retval = float(memstr[:-1]) * gibi
    elif memstr[-1] == 'T':
        retval = float(memstr[:-1]) * tebi
    else:
        retval = 0.

    return retval


def get_hosts():
    qconf_output = subprocess.run([_QCONF, '-sel'], capture_output=True, text=True)
    allhosts = qconf_output.stdout.split()

    # non-compute hosts - to be removed from list allhosts
    #   cubic-login1.bicic.local
    #   cubic-login2.uphs.upenn.edu
    #   cubic-login3.bicic.local
    #   cubic-login4.uphs.upenn.edu
    #   cubic-login5.bicic.local
    #   cubic-sattertt1.bicic.local
    login_node_pat = re.compile(r'^cubic-login')
    fed_node_pat = re.compile(r'^compute-fed')

    to_remove = ['cubic-sattertt1.bicic.local']
    for h in allhosts:
        if login_node_pat.match(h) or fed_node_pat.match(h):
            to_remove.append(h)

    for r in to_remove:
        allhosts.remove(r)

    if _DEBUG:
        print(f'DEBUG: allhosts = {allhosts}')

    return allhosts


def sge_to_slurm_resources(all_host_resources):
    pass


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

    if _DEBUG:
        print(f'DEBUG: hostname = {hostname}')

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

        # We ignore load values, and values which do not go
        # into the node definition of slurm.conf
        expanded_values = v.split(',')

        if _DEBUG:
            print(f'DEBUG: expanded_values = {expanded_values}')

        ev_dict = {}
        res_of_interest = ('A40', 'A100', 'P100', 'V100', 'gpu',
                           'avx', 'avx2', 'SGX',
                           'm_socket', 'm_core', 'm_thread', 'mem_total',
                           'tmptot')
        for ev in expanded_values:
            key, val = ev.split('=')

            if _DEBUG:
                print(f'DEBUG: key = {key}, val = {val}')

            # GPU types are case-sensitive; but we handle that when we generate
            # the slurm.conf line
            if key in res_of_interest:
                bool_vals = ('A40', 'A100', 'P100', 'V100', 'avx', 'avx2', 'SGX')
                if key in bool_vals:
                    if val == 'TRUE':
                        val = True
                    elif val == 'FALSE':
                        val = False

                int_vals = ('gpu', 'm_socket', 'm_core', 'm_thread')
                if key in int_vals:
                    val = int(val)

                # memory and disk space in units of MiB
                mem_vals = ('mem_total', 'tmptot')
                if key in mem_vals:
                    val = to_MiB(val)

                if val:
                    ev_dict[key.lower()] = val

        if _DEBUG:
            print(f'DEBUG: ev_dict = {ev_dict}')

        host_resources[hostname] |= ev_dict

        if _DEBUG:
            print('')

    return host_resources


def convert_to_slurm_node_conf(host_resources):
    # Input: dict - key = hostname, val = dict of resources
    # Output: slurm.conf node config line
    #  E.g. 
    #     NodeName=2118ffn001       Sockets=2 CoresPerSocket=16 ThreadsPerCore=2 RealMemory=773000 TmpDisk=3661000 Gres=gpu:A40:2
    retval = None

    features = ('avx', 'avx2', 'sgx')
    gpu_types = ('a40', 'a100', 'p100', 'v100')

    # we can't guarantee the gpu_type appears before no. of gpus
    # so, first pass, we redefine
    host_gputype_dict = {}
    for hostname, resources in host_resources.items():
        gputype_maybe = set(resources.keys()).intersection(gpu_types)
        if gputype_maybe:
            # there should be only one
            gputype = gputype_maybe.pop()
            host_gputype_dict[hostname] = gputype

    nodelines = []
    for hostname, resources in host_resources.items():
        node_def_str = ''

        nodename = hostname.split('.')[0]

        node_def_str += f'NodeName={nodename} '
        node_def_str += f'Sockets={resources["m_socket"]} '

        cores_per_socket = int(resources['m_core'] / resources['m_socket'])
        node_def_str += f'CoresPerSocket={cores_per_socket} '

        threads_per_core = int(resources['m_thread'] / resources['m_core'])
        node_def_str += f'ThreadsPerCore={threads_per_core} '

        # mem - in MiB; reduce by 3 GiB for system
        real_mem = int(resources['mem_total']) - (3 * 1024)
        node_def_str += f'RealMemory={real_mem} '

        # tmp disk space - in MiB; reduce by 8 GiB
        tmp_disk = int(resources['tmptot'] - (8 * 1024))
        node_def_str += f'TmpDisk={tmp_disk} '

        features_list = []
        gres = ''
        for resname, resval in resources.items():
            if (resname in features) and resval:
                if resname.lower() == 'sgx':
                    resname = resname.upper()
                features_list.append(resname)

        features_str = None
        if features_list:
            features_str = 'Features=' + ','.join(features_list)
            node_def_str += f'{features_str} '

        gpu_str = None
        if 'gpu' in resources.keys():
            gpu_str = f'Gres=gpu:{host_gputype_dict[hostname].upper()}:{resources["gpu"]}'
            node_def_str += f'{gpu_str} '

        if _DEBUG:
            print(f'DEBUG: node_def_str = {node_def_str}')
            print()

        nodelines.append(node_def_str)
        node_def_str = None

    return nodelines



def main():
    parser = argparse.ArgumentParser('Convert CUBIC SGE host records to Slurm')
    parser.add_argument('-d', '--debug', action='store_true', help='Debugging output')
    args = parser.parse_args()

    _DEBUG = args.debug

    allhosts = get_hosts()

    if _DEBUG:
        print(allhosts)

    host_resources = {}
    for h in allhosts:
        host_resources |= get_host_resources(h)

    if _DEBUG:
        for host, resources in host_resources.items():
            print(f'host = {host};  resources = {resources}')

    nodelines = convert_to_slurm_node_conf(host_resources)

    for n in nodelines:
        print(n)


if __name__ == '__main__':
    main()

