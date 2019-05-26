#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import requests
from fuse import Fuse

from sys import argv, exit

rubrikHost = "shrd1-rbk01.rubrikdemo.com"
rubrikKey = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIyOWNiNzFhMS0yZGIwLTRlZGQtYjA1Mi1kNmQ1NWRlMjBiOTRfMmY2MzFmYjItNzUyMi00ZTcwLWFjNzgtMzk1Y2EzNTIwMmRjIiwiaXNzIjoiMjljYjcxYTEtMmRiMC00ZWRkLWIwNTItZDZkNTVkZTIwYjk0IiwianRpIjoiYjNjYzUzYTUtNDIwMi00ZDc5LWE4ZDctMmFjNGI3ODk3YmU3In0.CyijHNB9H1-VTPD0MHcnvegHI0e0ZoA80y8n_W0yliI"
rubrikSnapshot = "92281431-bfb6-4a76-aa75-e5c33a0d1958"

# Base Stat Variables (Not needed yet)
class rubrikStat(fuse.Stat):
 def __init__(self):
   self.st_mode = 0
   self.st_ino = 0
   self.st_dev = 0
   self.st_nlink = 0
   self.st_uid = 0
   self.st_gid = 0
   self.st_size = 0
   self.st_atime = 0
   self.st_mtime = 0
   self.st_ctime = 0


# Simple dump of a directory
class RubrikFS(Fuse):
    def readdir(self, path, offset):
        for r in '.', '..', hello_path[1:]:
            yield fuse.Direntry(r)


class Rubrik(rubrikHost,rubrikKey):
    class RubrikException(Exception):
        def __init__(self, msg):
            self.msg = msg

        def __str__(self):
            return self.msg

    def __init__(self, rubrik_addr, rubrik_api_key):
        # Prompt for configuration info
        self.rubrik_addr = rubrik_addr
        self.baseurl = "https://" + self.rubrik_addr + "/api/v1/"
        self.internal_baseurl = "https://" + self.rubrik_addr + "/api/internal/"
        self.rubrik_api_key = rubrik_api_key
        self.headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
                        'Authorization': 'Bearer ' + self.rubrik_api_key}
        self.callFileSets = "/fileset"
        self.callFileSetDetail = "/fileset/{}"
        self.callFilesetBrowse = "/fileset/snapshot/{}/browse"

        # Disable ssl warnings for Requests
        requests.packages.urllib3.disable_warnings()

    def browse_path(self, snap="", path="/"):
        data = self.apicall(self.callFilesetBrowse.format(snap) + "?path=" + path)
        print(data)

    def apicall(self, call, method="get", data="", internal=False):
      uri = self.baseurl + call
      if internal:
        uri = self.internal_baseurl + call
      else:
        uri = self.baseurl + call
      try:
        r = getattr(requests, method)(uri, data=data, verify=False, auth=self.auth)
        r.raise_for_status()
      except requests.RequestException as e:
        print
        e
        raise self.RubrikException("POST Call Failed: " + str(e))
      except (requests.exceptions.HTTPError, requests.exceptions.RequestException) as e:
        print
        e
        response = r.json()
        if response.has_key('message'):
          print
          response['message']
        raise self.RubrikException("Call Failed: " + response['message'])
        sys.exit(1)


def main():
    usage="""
Userspace Rubrik
""" + Fuse.fusage
    server = RubrikFS(version="%prog " + fuse.__version__,
                     usage=usage,
                     dash_s_do='setsingle')

    server.parse(errex=1)
    server.main()


if __name__ == '__main__':
    main()
