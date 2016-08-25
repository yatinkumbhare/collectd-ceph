#!/usr/bin/env python
#
# vim: tabstop=4 shiftwidth=4

# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; only version 2 of the License is applicable.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
#
# Authors:
#   Yatin Kumbhare<yatinkumbhare@gmail.com>
#
# About this plugin:
#   This plugin collects information regarding Ceph Status 
#
# collectd:
#   http://collectd.org
# collectd-python:
#   http://collectd.org/documentation/manpages/collectd-python.5.shtml
#

import collectd
import json
import traceback
import subprocess

import base

class CephStatusPlugin(base.Base):

    def __init__(self):
        base.Base.__init__(self)
        self.prefix = 'ceph'

    def get_stats(self):
        """Retrieves stats from ceph pgs"""

        ceph_cluster = "%s.%s" % (self.prefix, self.cluster)

        data = { ceph_cluster: { } }
        output = None
        try:
            cephstatus_cmdline ='ceph -s -f json --cluster '+ self.cluster
            output_ceph_s = subprocess.check_output(cephstatus_cmdline, shell=True)
        except Exception as exc:
            collectd.error("ceph: failed ceph -s :: %s :: %s"
                    % (exc, traceback.format_exc()))
            return

        if output_ceph_s is None:
            collectd.error('ceph: failed to ceph -s :: output was None')

        json_stats_data = json.loads(output_ceph_s)

        # Get PG Status
        data[ceph_cluster]['pg'] = {}
        pg_data = data[ceph_cluster]['pg']
        pg_data['num_pgs'] = json_stats_data['pgmap']['num_pgs']
        for pgmap in json_stats_data['pgmap']['pgs_by_state']:
            pg_data[pgmap['state_name']] = pgmap['count']
    
        # Get Cluster Read/Write Bandwidth and IOPS
        data[ceph_cluster]['cluster'] = {}
        pgmap = json_stats_data['pgmap']
        for stat in ('read_bytes_sec', 'write_bytes_sec', 'read_op_per_sec', 'write_op_per_sec'):
            data[ceph_cluster]['cluster'][stat] = pgmap[stat] if stat in pgmap else 0

        # Get Monitors: this can replace ceph_monitor_plugin
        data[ceph_cluster]['mon'] = { 'number': 0, 'quorum': 0 }
        data[ceph_cluster]['mon']['number'] = len(json_stats_data['monmap']['mons'])
        data[ceph_cluster]['mon']['quorum'] = len(json_stats_data['quorum'])

        # Get Ceph Health
        health = json_stats_data['health']['overall_status']
        health_status = ('HEALTH_ERR', 'HEALTH_WARN', 'HEALTH_OK')
        data[ceph_cluster]['cluster']['health'] = health_status.index(health)
        return data

try:
    plugin = CephStatusPlugin()
except Exception as exc:
    collectd.error("ceph-s: failed to initialize ceph pg plugin :: %s :: %s"
            % (exc, traceback.format_exc()))

def configure_callback(conf):
    """Received configuration information"""
    plugin.config_callback(conf)

def read_callback():
    """Callback triggerred by collectd on read"""
    plugin.read_callback()

collectd.register_config(configure_callback)
collectd.register_read(read_callback, plugin.interval)
