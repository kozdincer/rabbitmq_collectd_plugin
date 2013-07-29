#!/usr/bin/env python
# -*- coding: utf-8; -*-
"""
Copyright (C) 2013 - Kaan Özdinçer <kaanozdincer@gmail.com>

This file is part of rabbitmq-collect-plugin.

rabbitmq-collectd-plugin is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

rabbitmq-collectd-plugin is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>

"""
import collectd
import subprocess

NAME = 'rabbitmq'
RABBITMQCTL_BIN = '/usr/sbin/rabbitmqctl'
VERBOSE = True

class RabbitMqReport():
    def __init__(self, report):
        self.report = report
        self.status = self.get_infos('Status of')
        self.status_array = []

        # Remove unnecessary outputs
        beg = self.status.find('[')
        end = self.status.rfind(']') + 1
        self.status = self.status[beg:end].strip()

        # Create stats array
        for m in self.status.split(','):
            m = self.clear_line(m.strip())
            self.status_array.append(m)

        self.file_descriptors = self.get_status('total_used')
        self.socket_descriptors = self.get_status('sockets_used')
        self.erlang_processes = self.get_status('used')
        self.memory = self.get_status('total')
        self.disk_space = self.get_status('disk_free')
        self.uptime = self.get_status('uptime')
        self.connections_count = self.get_count('Connections:')
        self.channels_count = self.get_count('Channels:')
        self.exchanges_count = self.get_count('Exchanges on')
        self.queues_count = self.get_count('Queues on')
        self.consumers_count = self.get_count('Consumers on')

    def get_count(self, stat):
        count = len(self.get_infos(stat).split('\n')) - 1
        return count

    def get_infos(self, info_name):
        beg = self.report.find(info_name)
        end = self.report.find('\n\n', beg)
        return self.report[beg:end]

    # Return stat value from name
    def get_status(self, stat_name):
        try:
            stat_index = self.status_array.index(stat_name) + 1
        except:
            return None
        return self.status_array[stat_index]
    
    # Clear unneecessary chars from stats
    def clear_line(self, line):
        unnec_chars = ['[', ']', '{', '}', ',', '"', '\\n']
        for uc in unnec_chars:
            line = line.replace(uc, '')
        return line

# Config data from collectd
def configure_callback(conf):
    log('verb', 'configure_callback Running')
    for node in conf.children:
        if node.key == 'RmqcBin':
            RABBITMQCTL_BIN = node.values[0]
        elif node.key == 'Verbose':
            VERBOSE = node.values[0]
        else:
            log('warn', 'Unknown config key: %s' %node.key)

# Send rabbitmq stats to collectd
def read_callback():
    log('verb', 'read_callback Running')
    info = get_rabbitmqctl_status()
    
    # Send keys to collectd
    for key in info:
        log('verb', 'Sent value: %s %i' %(key, info[key]))
        value = collectd.Values(plugin=NAME)
        value.type = 'gauge'
        value.type_instance = key
        value.values = [int(info[key])]
        value.dispatch()
    
# Get all statistics with rabbitmqctl
def get_rabbitmqctl_status():
    stats = {}

    # Execute rabbitmqctl
    try:
        p = subprocess.Popen([RABBITMQCTL_BIN, 'report'], shell=False,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except:
        log('err', 'Failed to run %s' %RABBITMQCTL_BIN)
        return None

    rs = RabbitMqReport(p.stdout.read())
    stats['file_descriptors'] = int(rs.file_descriptors)
    stats['socket_descriptors'] = int(rs.socket_descriptors)
    stats['erlang_processes'] = int(rs.erlang_processes)
    stats['memory'] = int(rs.memory)
    stats['disk_space'] = int(rs.disk_space)
    stats['uptime'] = int(rs.uptime)
    stats['connections_count'] = int(rs.connections_count)
    stats['channels_count'] = int(rs.channels_count)
    stats['exchanges_count'] = int(rs.exchanges_count)
    stats['queues_count'] = int(rs.queues_count)
    stats['consumers_count'] = int(rs.consumers_count)

    return stats

# Log messages to collect logger
def log(t, message):
    if t == 'err':
        collectd.error('%s: %s' %(NAME, message))
    elif t == 'warn':
        collectd.warning('%s: %s' %(NAME, message))
    elif t == 'verb' and VERBOSE == True:
        collectd.info('%s: %s' %(NAME, message))
    else:
        collectd.info('%s: %s' %(NAME, message))


# Register to collectd
collectd.register_config(configure_callback)
collectd.warning('Initialising %s' %NAME)
collectd.register_read(read_callback)

