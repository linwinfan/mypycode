#!/usr/bin/env python
# vim: set ts=8 sw=4 sts=4 et ai:
from __future__ import print_function
"""
proxtop: Proxmox resource monitor -- list top resource consumers of your
Proxmox VM platform.

This is proxtop.  proxtop is free software: you can redistribute it
and/or modify it under the terms of the GNU General Public License as
published by the Free Software Foundation, version 3 or any later
version.
"""
import os
import re
import sys
import warnings
from argparse import ArgumentParser
from fnmatch import fnmatch
from getpass import getpass
from os import path
from proxmoxer import ProxmoxAPI
from time import time

__author__ = 'Walter Doekes'
__copyright__ = 'Copyright (C) Walter Doekes, OSSO B.V. 2015-2019'
__licence__ = 'GPLv3+'
__version__ = '0.3.0'

# Workaround for older python-requests with proxmoxer.
try:
    # python-requests 2.5.3
    from requests.packages import urllib3
except ImportError:
    # python-requests 2.2.1
    try:
        import requests
    except ImportError:
        # ProxmoxAPI will bail out later, saying:
        # Chosen backend requires 'requests' module
        pass
    else:
        import imp
        import urllib3

        class DummyException(Exception):
            pass
        dummy_func = (lambda *args, **kwargs: None)
        dummy_packages = imp.new_module('requests.packages')
        dummy_packages.urllib3 = urllib3
        urllib3.disable_warnings = dummy_func
        urllib3.exceptions.InsecureRequestWarning = DummyException
        requests.packages = dummy_packages


def humanize(multiplier, units):
    def wrapped(num):
        unitcopy = units[:]
        num *= multiplier
        while unitcopy:
            unit = unitcopy.pop()
            if num < 1024:
                break
            num /= 1024.0
        return '%6.1f %s' % (num, unit)
    return wrapped
humanize_bytes_per_second = humanize(  # noqa
    8, ['TiB/s', 'GiB/s', 'MiB/s', 'KiB/s', 'B/s  '])
humanize_bits_per_second = humanize(
    1, ['Tbps ', 'Gbps ', 'Mbps ', 'Kbps ', 'bps  '])
humanize_percentage = (
    lambda x: '%6d %%    ' % (x * 100))  # align with humanize


PROXMOX_VOLUME_TYPES = ('ide', 'sata', 'scsi', 'virtio')
PROXMOX_VOLUME_TYPES_RE = re.compile(  # "scsi0", but not "scsihw"
    r'^({})\d+$'.format('|'.join(PROXMOX_VOLUME_TYPES)))

ITEMS = (
    ('cpu', humanize_percentage),
    ('diskread', humanize_bytes_per_second),
    ('diskwrite', humanize_bytes_per_second),
    ('netin', humanize_bits_per_second),
    ('netout', humanize_bits_per_second),
    # 'time': rrd sample time
    # 'disk': 0 ?
    # 'maxcpu': cpu-cores * cpu-threads (static)
    # 'maxdisk': total size of all combined disks (static)
    # 'maxmem': total memory (static)
    # 'mem': memory usage
)


def checking_foreach(data, item):
    """For some reason, the Proxmox API hands us crazy large values from
    time to time when we use the month-timeframe."""
    too_large = 0x3fffffffff  # 256 GiB/s..
    valid, ignored = [], []
    for row in data:
        value = row.get(item)
        if value is None:
            # Happens if the machine is new.
            pass
        elif value <= too_large:
            valid.append(value)
        else:
            ignored.append(value)
    return valid, ignored


def is_valid_uuid(uuid):
    parts = uuid.split('-')
    if ([len(i) for i in parts] == [8, 4, 4, 4, 12] and
            all(i in '0123456789abcdef-' for i in uuid)):
        return True
    return False


def get_rrddata(proxmox, timeframe='hour', aggregation='AVERAGE',
                only_names=None, only_storage=None):
    vms = []
    crap = {}
    nodes_down = set()
    uuid_map = {}

    vms_data = proxmox.cluster.resources.get(type='vm')
    for vm_index, vm in enumerate(vms_data):
        # Progress.
        if vm_index % 16 == 0 and os.isatty(sys.stderr.fileno()):
            sys.stderr.write('% 3d%% loaded..\r' % (
                vm_index * 100.0 / len(vms_data)))
            sys.stderr.flush()

        # Find only certain hosts?
        if only_names:
            if not any(fnmatch(vm.get('name', ''), i) for i in only_names):
                continue

        # Find only hosts with specific storage?
        if only_storage:
            volumes = [
                kw for kw in (
                    proxmox.nodes(vm.get('node')).qemu(vm.get('vmid'))
                    .pending.get())
                if PROXMOX_VOLUME_TYPES_RE.match(kw['key']) and 'value' in kw]
            if not any(fnmatch(i['value'], only_storage) for i in volumes):
                continue

        # VM info without status? Node down?
        if 'status' not in vm:
            if vm['node'] not in nodes_down:
                sys.stderr.write(
                    'WARNING: no status for vm %s. Is node %s down? '
                    'Ignoring further errors on that node.\n' % (
                        vm['vmid'], vm['node']))
                nodes_down.add(vm['node'])
            continue

        # Stopped VM?
        if vm['status'] in ('stopped',):
            continue
        # Broken VM? Prelaunch? What is this?
        if vm['status'] in ('internal-error', 'prelaunch'):
            sys.stderr.write(
                'WARNING: vm %s says %s. Full status: %r\n' % (
                    vm['vmid'], vm['status'], vm))
            continue

        assert vm['status'] in ('running', 'paused'), vm

        node = proxmox.nodes(vm['node'])
        container = getattr(node, vm['type'])(vm['vmid'])

        # Fetch UUIDs so we can complain about VMs without UUID.
        smbios1 = container.config.get().get('smbios1', '')
        uuid = ''.join(tmp[5:] for tmp in smbios1.split(',')
                       if tmp.startswith('uuid=')) or None
        assert vm['name'] not in uuid_map, (vm['name'], uuid_map)
        # We could choose to check manufacturer and product too:
        # uuid=648...a45,manufacturer=spindle,product=cacofonisk
        uuid_map[vm['name']] = uuid

        # # Update uuid if it wasn't set?
        # if not is_valid_uuid(uuid or ''):
        #     from uuid import uuid4 as make_uuid
        #     uuid = str(make_uuid())
        #     if smbios1:
        #         smbios1 = 'uuid=%s,%s' % (uuid, smbios1)
        #     else:
        #         smbios1 = 'uuid=%s' % (uuid,)
        #     try:
        #         container.config.post(smbios1=smbios1)
        #     except:
        #         sys.stderr.write(
        #             'Could not update smbios1 to %r on %s\n' % (
        #                 smbios1, vm['name']))
        #     else:
        #         print('UPDATED', smbios1, 'UPDATED on', vm['name'])

        # Get the RRD values.
        rrd_aggregation = (  # MEDIAN does not exist, choose MAX/AVERAGE
            'AVERAGE' if aggregation == 'MEDIAN' else aggregation)
        rrd_timeframe = (  # 5min does not exist, choose hour
            'hour' if timeframe == '5min' else timeframe)
        data = container.rrddata.get(
            timeframe=rrd_timeframe, cf=rrd_aggregation)
        if timeframe == '5min':
            recently = time() - (5 * 60)
            data = [i for i in data if i['time'] >= recently]

        # Values tend to be wrong for only a single row: ignore the VM.
        if len(data) == 1:
            crap[vm['name']] = ['single_row', 1]
            vms.append((vm, dict((item, {'max': -1, 'avg': -1})
                                 for item, humanize_func in ITEMS)))
            continue

        # For 'hour' timeframe, we get 70 entries: one entry per minute:
        # len(data) == 70, (data[-1]['time'] - data[0]['time'] == 69 * 60),
        # For 'month' timeframe, we get ~66 entries: one entry per 12h, but
        # sometimes with gaps.
        totals = {}

        for item, humanize_func in ITEMS:
            median = list(sorted([i[item] for i in data if item in i]))
            half = len(median) // 2
            if len(median) % 1:
                median = median[half]
            else:
                median = (median[half] + median[half + 1]) / 2.0

            valid, ignored = checking_foreach(data, item)
            if not valid:
                sys.stderr.write(
                    'No sane RRD %s values for %s\n' % (item, vm['name']))
                valid = [0]
            totals[item] = {
                'max': max(valid),
                'med': median,
                'avg': sum(valid) / float(len(valid)),
            }
            if ignored:
                if vm['name'] not in crap:
                    crap[vm['name']] = []
                for value in ignored:
                    crap[vm['name']].append((item, value))

        vms.append((vm, totals))

    # Loop over uuid_map and complain about VMs without value UUID.
    valid_uuids, invalid_uuid_vms = [], []
    for name, uuid in uuid_map.items():
        if is_valid_uuid(uuid or ''):
            valid_uuids.append(uuid)
        else:
            invalid_uuid_vms.append(name)
    invalid_uuid_vms.sort()
    # Check validity.
    if invalid_uuid_vms:
        sys.stderr.write(
            'WARNING: These VMs have invalid/unset UUIDs: %s\n' % (
                ', '.join(invalid_uuid_vms),))
    # Check for dupes.
    if len(valid_uuids) != len(set(valid_uuids)):
        tmp = set()
        for test_uuid in valid_uuids:
            if test_uuid in tmp:
                tmp_vms = [name for name, uuid in uuid_map.items()
                           if uuid == test_uuid]
                sys.stderr.write(
                    'WARNING: Duplicate UUID %s in use by these VMs: %s\n' % (
                        test_uuid, ', '.join(tmp_vms)))
            tmp.add(test_uuid)

    return vms, crap


def print_ignored(vms, crap, top):
    print('IGNORED DATA: %d/%d' % (len(crap), len(vms)))
    print('------------------')
    crap = crap.items()
    crap.sort(key=(lambda x: len(x[1])), reverse=True)
    for i, (vm_name, values) in enumerate(crap[0:top]):
        print('#%d: %s  %r' %
              (i, vm_name, values))
    print()


def print_items(vms, top, aggregation):
    # for subtype in ('avg', 'med', 'max'):
    subtype = {'AVERAGE': 'avg', 'MEDIAN': 'med', 'MAX': 'max'}[aggregation]

    for item, humanize_func in ITEMS:
        print('SORTED BY: %s, %s' % (item, subtype))
        print('------------------')
        vms.sort(key=(lambda vm: vm[1][item][subtype]), reverse=True)
        for i, (vminfo, vmstat) in enumerate(vms[0:top]):
            print('#%d: %s  %s (%s)' %
                  (i, humanize_func(vmstat[item][subtype]),
                   vminfo['node'], vminfo['name']))
        print()


class ArgumentParser14191(ArgumentParser):
    """ArgumentParser from argparse that handles out-of-order positional
    arguments.

    This is a workaround created by Glenn Linderman in July 2012. You
    can now do this:

        parser = ArgumentParser14191()
        parser.add_argument('-f', '--foo')
        parser.add_argument('cmd')
        parser.add_argument('rest', nargs='*')
        # some of these would fail with the regular parser:
        for args, res in (('-f1 cmd 1 2 3', 'ok'),
                          ('cmd -f1 1 2 3', 'would_fail'),
                          ('cmd 1 -f1 2 3', 'would_fail'),
                          ('cmd 1 2 3 -f1', 'ok')):
            try: out = parser.parse_args(args.split())
            except: print 'args', 'failed', res
            # out: Namespace(cmd='cmd', foo='1', rest=['1', '2', '3'])

    Bugs: http://bugs.python.org/issue14191
    Files: http://bugs.python.org/file26273/t18a.py
    Changes: renamed to ArgumentParser14191 ** PEP cleaned ** hidden
      ErrorParser inside ArgumentParser14191 ** documented ** used
      new-style classes super calls  (Walter Doekes, March 2015)
    """
    class ErrorParser(ArgumentParser):
        def __init__(self, *args, **kwargs):
            self.__errorobj = None
            super(ArgumentParser14191.ErrorParser, self).__init__(
                *args, add_help=False, **kwargs)

        def error(self, message):
            if self.__errorobj:
                self.__errorobj.error(message)
            else:
                ArgumentParser.error(self, message)

        def seterror(self, errorobj):
            self.__errorobj = errorobj

    def __init__(self, *args, **kwargs):
        self.__setup = False
        self.__opt = ArgumentParser14191.ErrorParser(*args, **kwargs)
        super(ArgumentParser14191, self).__init__(*args, **kwargs)
        self.__opt.seterror(self)
        self.__setup = True

    def add_argument(self, *args, **kwargs):
        super(ArgumentParser14191, self).add_argument(*args, **kwargs)
        if self.__setup:
            chars = self.prefix_chars
            if args and len(args[0]) and args[0][0] in chars:
                self.__opt.add_argument(*args, **kwargs)

    def parse_args(self, args=None, namespace=None):
        ns, remain = self.__opt.parse_known_args(args, namespace)
        ns = super(ArgumentParser14191, self).parse_args(remain, ns)
        return ns


class Proxtop(object):
    def __call__(self):
        # Fetch defaults from ~/.proxtoprc.
        defaults = {'hostname': None, 'username': None, 'password': None}
        proxtoprc = path.expanduser('~/.proxtoprc')
        if path.exists(proxtoprc):
            with open(proxtoprc, 'r') as file_:
                for line in file_:
                    for key in defaults.keys():
                        if line.startswith(key + '='):
                            defaults[key] = line[(len(key) + 1):].strip()

        parser = ArgumentParser14191(
            description=('proxtop lists the top resource consumers on your '
                         'Proxmox VM platform.'),
            epilog=('Default values may be placed in ~/.proxtoprc. '
                    'Lines should look like: hostname=HOSTNAME, '
                    'username=USERNAME, password=PASSWORD'))
        parser.add_argument(
            'hostname', action='store',
            help='Use this API hostname (e.g. proxmox.example.com[:443])')
        parser.add_argument(
            'username', action='store',
            help='Use this API username (e.g. monitor@pve)')
        parser.add_argument(
            '-T', '--top', default=8, action='store', type=int,
            help='Limit results to TOP VMs')
        parser.add_argument(
            '-t', '--timeframe', default='hour', action='store',
            choices=('5min', 'hour', 'day', 'week', 'month', 'year'),
            help='Timeframe, can be one of: 5min | hour* | day | week | month | year')
        parser.add_argument(
            '-g', '--aggregation', default='MEDIAN', action='store',
            choices=('AVERAGE', 'MAX', 'MEDIAN'),
            help='RRD aggregation, can be one of: AVERAGE | MAX | MEDIAN*')
        parser.add_argument(
            '--only-storage', default=None, action='store',
            help='Filter VMs by storage glob (e.g. "nfs03*")')
        parser.add_argument(
            'only_vms', nargs='*',
            help='Limit results to these VM names (globbing is allowed)')

        # Pass adjusted argv, based on defaults found in ~/.proxtoprc.
        argv = sys.argv[1:]
        if defaults['hostname'] and defaults['username']:
            argv.insert(0, defaults['hostname'])
            argv.insert(1, defaults['username'])
        args = parser.parse_args(argv)

        # Extract optional port from hostname.
        if ':' in args.hostname:
            hostname, port = args.hostname.split(':', 1)
        else:
            hostname, port = args.hostname, 443  # used to be 8006

        # Fetch password.
        if defaults['password']:
            args.password = defaults['password']
        else:
            args.password = getpass('Password:')

        # It's nice that urllib3 and requests show SNIMissingWarning and
        # InsecurePlatformWarning warnings, but we don't need them more than
        # once.
        warnings.simplefilter('once')

        # Log in, get the data, display it.
        api = ProxmoxAPI(hostname, port=port, user=args.username,
                         password=args.password, verify_ssl=False)

        vms, crap = get_rrddata(
            api, timeframe=args.timeframe, aggregation=args.aggregation,
            only_names=args.only_vms, only_storage=args.only_storage)

        if crap:
            print_ignored(vms, crap, top=args.top)
        print_items(vms, top=args.top, aggregation=args.aggregation)


if __name__ == '__main__':
    main = Proxtop()
    main()
