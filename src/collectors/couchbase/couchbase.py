# coding=utf-8

"""
Originally forked from https://github.com/pythianliappis/couchbase_collector.

Collecting CouchBase metrics - per bucket and overall.

#### Dependencies

"""

import os
import sys
import urllib2
from urlparse import urlparse
import base64
import diamond.collector


try:
  import json
except ImportError:
  import simplejson as json
except ImportError:
   err = "ERROR: json or simplejson module not found (couchbase collector)"
   log.info(err)


class CouchBaseCollector(diamond.collector.Collector):


   def get_default_config_help(self):
      config_help = super(CouchBaseCollector, self).get_default_config_help()
      config_help.update({
         'host':  'Hostname or IP address of the couchbase server',
         'port':  'Port to contact couchbase instance on',
         'buckets': 'Database(s) to record metrics for, comma separated',
         'username': 'Username to authenticate with',
         'password': 'Password to authenticate with'
      })
      return config_help


   def get_default_config(self):
     """
     Returns the default collector settings.
     """
     config = super(CouchBaseCollector, self).get_default_config()
     config.update({
      'host': 'localhost',
      'port': 8091,
      'buckets': '',
      'username': 'admin',
      'password': 'admin'
     })
     return config


   def get_data(self, url):
      auth = "{0}:{1}".format(self.config['username'], self.config['password'])
      auth = base64.encodestring(auth).replace('\n', '')
      request = urllib2.Request(url);
      request.add_header("Authorization", "Basic {0}".format(auth))

      resp = None
      try:
         resp = urllib2.urlopen(url)
      except urllib2.HTTPError, err:
         self.log.error("CouchBaseCollector: Error: {0}: {1}".format(url, err))
         return
      if resp is None:
         return

      return json.load(resp)


   def collect_bucket(self, bucket):
      url_fmt = "http://{0}:{1}/pools/default/buckets/{2}"
      url = url_fmt.format(self.config['host'], self.config['port'], bucket)
      data = self.get_data(url)

      rstats = [ "basicStats", "quota" ]
      for stat in rstats:
         if stat in data:
            for metric, val in data[stat].items():
               fmetric = "{0}.{1}.{2}".format(stat, bucket, metric)
               self.publish(fmetric, val)
         else:
            err = "CouchBaseCollector: {0} missing from {1}\n"
            self.log.error(err.format(stat, url))

      node_stats = "nodes"
      if node_stats in data:
         for node in data[node_stats]:
            # only record metrics for the current node
            if "thisNode" in node and node["thisNode"]:
               # record all key,val in interestingStats
               for metric,val in node["interestingStats"].items():
                  fmetric = "istats.{0}.{1}".format(bucket, metric)
                  self.publish(fmetric, val)
               record = [ "memoryTotal", "memoryFree", "mcdMemoryReserved", "mcdMemoryAllocated" ]
               for metric in record:
                  fmetric = "stats.{0}.{1}".format(bucket, metric)
                  self.publish(fmetric, val)


   def collect(self):
      buckets_str = self.config['buckets'] if self.config['buckets'] else ""
      buckets = buckets_str.split(",")

      for bucket in buckets:
         self.collect_bucket(bucket)
