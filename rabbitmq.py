import collectd
import subprocess
import re


NAME = 'rabbitmq_status'
RABBITMQCTL_BIN = '/usr/sbin/rabbitmqctl'
VERBOSE = True

class RabbitMqStatus():
    def __init__(self, message):
        self.message = message
        self.stats_array = []

        # Remove unnecessary outputs
        beg = message.find('[')
        end = message.rfind(']') + 1
        self.message = message[beg:end].strip()

        # Create stats array
        for m in self.message.split(','):
            m = self.clear_line(m.strip())
            self.stats_array.append(m)

        self.file_descriptors = self.get_stat('total_used')
        self.socket_descriptors = self.get_stat('sockets_used')
        self.erlang_processes = self.get_stat('used')
        self.memory = self.get_stat('total')
        self.disk_space = self.get_stat('disk_free')
        self.uptime = self.get_stat('uptime')

    # Return all 'rabbitmqctl status' output
    def get_status(self):
        return self.message

    # Return stat value from name
    def get_stat(self, stat_name):
        try:
            stat_index = self.stats_array.index(stat_name) + 1
        except:
            return None
        return self.stats_array[stat_index]
    
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
        p = subprocess.Popen([RABBITMQCTL_BIN, 'status'], shell=False,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except:
        log('err', 'Failed to run %s' %RABBITMQCTL_BIN)
        return None

    rs = RabbitMqStatus(p.stdout.read())
    stats['file_descriptors'] = int(rs.file_descriptors)
    stats['socket_descriptors'] = int(rs.socket_descriptors)
    stats['erlang_processes'] = int(rs.erlang_processes)
    stats['memory'] = int(rs.memory)
    stats['disk_space'] = int(rs.disk_space)
    stats['uptime'] = int(rs.uptime)

    return stats


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
