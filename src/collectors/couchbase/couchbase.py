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


   def get_pools(self):
      ret = []
      url_fmt = "http://{0}:{1}/pools/"
      url = url_fmt.format(self.config['host'], self.config['port'])
      data = self.get_data(url)
      if "pools" in data:
         for pool in data["pools"]:
            if "name" in pool and "uri" in pool:
               ret.append(pool)
      return ret


   def publish_bucket_basic(self, pool):
      url_fmt = "http://{0}:{1}/pools/{2}/buckets/"
      url = url_fmt.format(self.config['host'], self.config['port'], pool['name'])
      data = self.get_data(url)
      # response should be an array of dicts - we want basicStats from each
      for bucket in data:
         if "name" in bucket and "basicStats" in bucket:
            name = bucket["name"]
            stat = "basicStats"
            for metric, val in bucket[stat].items():
               fmetric = "{0}.buckets.basic.{1}.{2}".format(pool, name, metric)
               self.publish(fmetric, val)
         # also publish stats for bucket
         self.publish_bucket_stats(pool['name'], name)


   def publish_bucket_stats(self, pool, bucket):
      url_fmt = "http://{0}:{1}/pools/{2}/buckets/{3}/stats"
      url = url_fmt.format(self.config['host'], self.config['port'], pool, bucket)
      data = self.get_data(url)

      if "op" in data and "samples" in data["op"]:
         for metric, val in data["op"]["samples"].items():
            fmetric = "{0}.buckets.stats.{1}.{2}".format(pool, bucket, metric)
            self.publish(fmetric, val)


   def publish_node_stats(self):
      ## fix me - pool is name: url dict
      url_fmt = "http://{0}:{1}/pools/nodes/"
      url = url_fmt.format(self.config['host'], self.config['port'])
      data = self.get_data(url)

      stat = "storageTotals" # duplicating metrics for this one..
      if stat in data:
         for key in data[stat].keys():
            for metric, val in data[stat][key].items():
               fmetric = "{0}.node.storage.{1}.{2}".format(pool, key, metric)
               self.publish(fmetric, val)

      stat = "nodes"
      if stat in data:
         for node in data[stat]:
            # only record metrics for the current node
            if "thisNode" in node and node["thisNode"]:
               # record all key,val in interestingStats
               for metric,val in node["interestingStats"].items():
                  fmetric = "{0}.node.stats.{1}".format(pool, metric)
                  self.publish(fmetric, val)

      stat = "counters" ## duplicating data for this one as well..
      if stat in data:
         # record all key,val in interestingStats
         for metric,val in node[stat].items():
            fmetric = "{0}.node.{1}.{2}".format(pool, stat, metric)
            self.publish(fmetric, val)


   def collect(self):
      self.publish_node_stats()
      pools = self.get_pools()
      for pool in pools:
         self.publish_bucket_basic(pool)
