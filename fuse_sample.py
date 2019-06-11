#!/usr/bin/env python
from __future__ import print_function, absolute_import, division
import logging
from errno import ENOENT
from stat import S_IFDIR, S_IFREG
from time import time
import urllib.parse as ul
import os
from datetime import datetime
import re
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn, fuse_get_context
import requests
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

rubrikHost = "amer1-rbk01.rubrikdemo.com"
rubrikKey = str("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiI1YTc1YWU5Yy0zMzdkLTQ3ZDMtYjUxNS01MmFmNzE5MTcxMmNfMmY2MzFmYjItNzUyMi00ZTcwLWFjNzgtMzk1Y2EzNTIwMmRjIiwiaXNzIjoiNWE3NWFlOWMtMzM3ZC00N2QzLWI1MTUtNTJhZjcxOTE3MTJjIiwianRpIjoiY2QwMzgyZTgtZTk1OC00MWUxLWJhNGUtYTc2YTY5N2NhZDM3In0.iGwpmJASop36bGCrMIZmRc8lRG34QLpCdYTBQ0K3Tvs")
rubrikSnapshot = str("3fa0f5b3-2a63-4361-b999-decb8794faad")
rubrikOperatingSystemType = "Windows"

#rubrikHost = "amer1-rbk01.rubrikdemo.com"
#rubrikKey = str("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIyOWNiNzFhMS0yZGIwLTRlZGQtYjA1Mi1kNmQ1NWRlMjBiOTRfMmY2MzFmYjItNzUyMi00ZTcwLWFjNzgtMzk1Y2EzNTIwMmRjIiwiaXNzIjoiMjljYjcxYTEtMmRiMC00ZWRkLWIwNTItZDZkNTVkZTIwYjk0IiwianRpIjoiYjNjYzUzYTUtNDIwMi00ZDc5LWE4ZDctMmFjNGI3ODk3YmU3In0.CyijHNB9H1-VTPD0MHcnvegHI0e0ZoA80y8n_W0yliI")
#rubrikSnapshot = str("20200577-fa4d-4953-a00a-6aacb6869cfa")


class RubrikDB:
    def __init__(self):
        con = psycopg2.connect(host='localhost', sslmode='disable', port=26257, user='root')
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute("CREATE DATABASE IF NOT EXISTS _{};".format(rubrikSnapshot))


class RubrikFS(LoggingMixIn, Operations):
    def __init__(self):
        self.rubrik = Rubrik(rubrikHost, rubrikKey)
        self.rubrikdb = RubrikDB()

    def getattr(self, path, fh=None):
        st = os.lstat('/tmp')
        out = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                       'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size',
                                                       'st_uid'))
        if rubrikOperatingSystemType == "Windows":
            name = None
            path = re.sub(r'^\/(\S+.*)', '\\1', path)
            if not path.startswith("/"):
                path = path.replace('/', '\\')
                if "\\" in path:
                    [path, name] = path.rsplit('\\', 1)
                if not name:
                    name = path
                for obj in self.rubrik.browse_path(rubrikSnapshot, path)['data']:
                    if obj['filename'] == name:
                        if obj['fileMode'] == "directory":
                            st = os.lstat('test_dir')
                        else:
                            st = os.lstat('test_dir/test_file')
                        out = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                       'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size',
                                                       'st_uid'))
                        out['st_size'] = obj['size']
                        out['st_mtime'] = (datetime.strptime(obj['lastModified'], '%Y-%m-%dT%H:%M:%S+0000') - datetime(1970, 1, 1)).total_seconds()
        return out

    def readdir(self, path, fh):
        if rubrikOperatingSystemType == "Windows":
            path = re.sub(r'^\/(\S+.*)', '\\1', path)
            if not path.startswith("/"):
                path = path.replace('/', '\\')
        objs = ['.', '..']
        for obj in self.rubrik.browse_path(rubrikSnapshot, path)['data']:
            objs.append(obj['filename'])
            #objs.append(obj['filename'].replace(' ', '\ '))
        return objs


class Rubrik:
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
        self.callFileSets = "fileset"
        self.callFileSetDetail = "fileset/{}"
        self.callFilesetBrowse = "fileset/snapshot/{}/browse?path={}"

        # Disable ssl warnings for Requests
        requests.packages.urllib3.disable_warnings()

    def browse_path(self, snap="", path=""):
        return self.apicall(self.callFilesetBrowse.format(snap, ul.quote_plus(path)))

    def apicall(self, call, method="get", data="", internal=False):
      uri = self.baseurl + call
      if internal:
        uri = self.internal_baseurl + call
      else:
        uri = self.baseurl + call
      try:
        r = getattr(requests, method)(uri, data=data, verify=False, headers=self.headers)
        r.raise_for_status()
        return r.json()
      except requests.RequestException as e:
        print
        e
        raise self.RubrikException("Rubrik API Call Failed: " + str(e))
      except (requests.exceptions.HTTPError, requests.exceptions.RequestException) as e:
        print
        e
        response = r.json()
        if response.has_key('message'):
          print
          response['message']
        raise self.RubrikException("Call Failed: " + response['message'])
        sys.exit(1)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('mount')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(
        RubrikFS(), args.mount, foreground=True, ro=True, allow_other=True)
