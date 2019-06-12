#!/usr/bin/env python
from __future__ import print_function, absolute_import, division
import logging
import os
import re
import requests
import psycopg2
import urllib.parse as ul
import uuid
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn, fuse_get_context
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import datetime
from errno import ENOENT
from stat import S_IFDIR, S_IFREG
from time import time


rubrikHost = "amer1-rbk01.rubrikdemo.com"
rubrikKey = str("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiI1YTc1YWU5Yy0zMzdkLTQ3ZDMtYjUxNS01MmFmNzE5MTcxMmNfMmY2MzFmYjItNzUyMi00ZTcwLWFjNzgtMzk1Y2EzNTIwMmRjIiwiaXNzIjoiNWE3NWFlOWMtMzM3ZC00N2QzLWI1MTUtNTJhZjcxOTE3MTJjIiwianRpIjoiY2QwMzgyZTgtZTk1OC00MWUxLWJhNGUtYTc2YTY5N2NhZDM3In0.iGwpmJASop36bGCrMIZmRc8lRG34QLpCdYTBQ0K3Tvs")
rubrikSnapshot = str("3fa0f5b3-2a63-4361-b999-decb8794faad")
rubrikOperatingSystemType = "Windows"

#rubrikHost = "shrd1-rbk01.rubrikdemo.com"
#rubrikKey = str(
 #   "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIyOWNiNzFhMS0yZGIwLTRlZGQtYjA1Mi1kNmQ1NWRlMjBiOTRfMmY2MzFmYjItNzUyMi00ZTcwLWFjNzgtMzk1Y2EzNTIwMmRjIiwiaXNzIjoiMjljYjcxYTEtMmRiMC00ZWRkLWIwNTItZDZkNTVkZTIwYjk0IiwianRpIjoiYjNjYzUzYTUtNDIwMi00ZDc5LWE4ZDctMmFjNGI3ODk3YmU3In0.CyijHNB9H1-VTPD0MHcnvegHI0e0ZoA80y8n_W0yliI")
#rubrikSnapshot = str("92281431-bfb6-4a76-aa75-e5c33a0d1958")
#rubrikOperatingSystemType = "Windows"




class RubrikDB:
    def __init__(self):
        self.rubrik = Rubrik(rubrikHost, rubrikKey)
        self.dbname = "_{}".format(rubrikSnapshot.replace('-', '_'))
        self.con = psycopg2.connect(host='localhost', sslmode='disable', port=26257, user='root')
        self.con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.fstat = os.lstat('test_dir/test_file')
        self.dstat = os.lstat('test_dir')
        cur = self.con.cursor()
        cur.execute("CREATE DATABASE IF NOT EXISTS {};".format(self.dbname))
        cur.execute("use {};".format(self.dbname))
        cur.execute("CREATE TABLE IF NOT EXISTS filestore ( "
                    "id UUID PRIMARY KEY DEFAULT gen_random_uuid(), "
                    "filename string, "
                    "fullPath string, "
                    "path string, "
                    "lastModified string, "
                    "size int, "
                    "fileMode string, "
                    "statusMessage string, "
                    "st_atime numeric, "
                    "st_ctime numeric , "
                    "st_gid int, "
                    "st_mode int, "
                    "st_mtime numeric, "
                    "st_nlink int, "
                    "st_size numeric, "
                    "st_uid int, "
                    "file_local bool DEFAULT false,"
                    "index path_idx (path)"
                    ");")

    def db_readdir(self, path):
        cur = self.con.cursor()
        q = "select filename from filestore where path='{}';".format(path)
        cur.execute(q)
        out = []
        join = ""
        if re.search(r'^/[A-Z]:', path):
            print("In first match")
            join = "/"
        if cur.rowcount > 0:
            print("Found {} rows in readdir using {}".format(cur.rowcount, path))
            print("Query : {}".format(q))
            for r in cur.fetchall():
                out.append(r[0])
        else:
            print("No rows found in readdir")
            print("Query : {}".format(q))
            for obj in self.rubrik.browse_path(rubrikSnapshot, path)['data']:
                if obj['fileMode'] == 'drive' or obj['fileMode'] == 'directory':
                    st = self.dstat
                else:
                    st = self.fstat
                fullpath = path
                out.append(obj['filename'])
                fullpath = "{}{}{}".format(path, join, obj['filename'])
                print("In join of {}".format(fullpath))
                cur.execute("insert into filestore ("
                            "filename, fullPath, path, lastModified, "
                            "size, filemode, statusMessage, st_atime, st_ctime, st_gid, st_mode, st_mtime, st_nlink, st_size, st_uid"
                            ") values ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}');".format(
                    obj['filename'],  # File or directory name
                    fullpath,  # Full path in local filesystem
                    path,  # Path on Rubrik for query
                    obj['lastModified'],
                    obj['size'],
                    obj['fileMode'],
                    obj['statusMessage'],
                    st.st_atime,
                    st.st_ctime,
                    st.st_gid,
                    st.st_mode,
                    st.st_mtime,
                    st.st_nlink,
                    st.st_size,
                    st.st_uid
                ))
        return out

    def db_getattr(self, path):
        cur = self.con.cursor()
        q = "select * from filestore where fullPath='{}';".format(path)
        cur.execute(q)
        name = None

        # Check DB for Cache Values
        if cur.rowcount == 1:
            print("Found {} rows in getattr using {}".format(cur.rowcount, path))
            r = dict(zip([col.name for col in cur.description], cur.fetchone()))
            out = dict((key, r[key]) for key in ('st_atime', 'st_ctime',
                                                          'st_gid', 'st_mode', 'st_mtime', 'st_nlink',
                                                          'st_size',
                                                          'st_uid'))
        # Carry on with API hit
        else:
            print("No rows found in getattr")
            print("Query : {}".format(q))
            for obj in self.rubrik.browse_path(rubrikSnapshot, path)['data']:
                print("found {} and {}".format(obj['filename'], name))
                if obj['fileMode'] == "directory" or obj['fileMode'] == "drive":
                    st = os.lstat('test_dir')
                else:
                    st = os.lstat('test_dir/test_file')
                out = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                                   'st_gid', 'st_mode', 'st_mtime', 'st_nlink',
                                                                   'st_size',
                                                                   'st_uid'))
                out['st_size'] = obj['size']
                out['st_mtime'] = (
                            datetime.strptime(obj['lastModified'],
                                              '%Y-%m-%dT%H:%M:%S+0000')
                            - datetime(1970, 1, 1)).total_seconds()

        return out


class RubrikFS(LoggingMixIn, Operations):
    def __init__(self):
        self.rubrikdb = RubrikDB()

    def getattr(self, path, fh=None):
        return self.rubrikdb.db_getattr(path)

    def readdir(self, path, fh):


        # Seed directory array for navigation
        objs = ['.', '..']

        # Add actual directory content
        objs.extend(self.rubrikdb.db_readdir(path))
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
        # Modify path if its a windows volume
        if re.search(r'^/[A-Z]:', path):
            path = re.sub(r'^\/', "", path)
            path = re.sub(r'\/', r'\\', path)
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
