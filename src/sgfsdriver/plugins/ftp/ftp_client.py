#!/usr/bin/env python

"""
   Copyright 2016 The Trustees of University of Arizona

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import traceback
import os
import logging
import time
import ftplib

from datetime import datetime
from expiringdict import ExpiringDict
from io import BytesIO

logger = logging.getLogger('ftp_client')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('ftp_client.log')
fh.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)

METADATA_CACHE_SIZE = 10000
METADATA_CACHE_TTL = 60     # 60 sec

"""
Interface class to FTP
"""


class ftp_status(object):
    def __init__(self,
                 directory=False,
                 symlink=False,
                 path=None,
                 name=None,
                 size=0,
                 create_time=0,
                 modify_time=0):
        self.directory = directory
        self.symlink = symlink
        self.path = path
        self.name = name
        self.size = size
        self.create_time = create_time
        self.modify_time = modify_time

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        rep_d = "F"
        if self.directory:
            rep_d = "D"

        rep_s = "-"
        if self.symlink:
            rep_s = "S"

        return "<ftp_status %s%s %s %d>" % \
            (rep_d, rep_s, self.name, self.size)


class ftp_client(object):
    def __init__(self,
                 host=None,
                 port=21,
                 user='anonymous',
                 password='anonymous@email.com'):
        self.host = host
        if port:
            self.port = port
        else:
            self.port = 21

        if user:
            self.user = user
        else:
            self.user = "anonymous"

        if password:
            self.password = password
        else:
            self.password = "anonymous@email.com"

        self.session = None

        # init cache
        self.meta_cache = ExpiringDict(
            max_len=METADATA_CACHE_SIZE,
            max_age_seconds=METADATA_CACHE_TTL
        )

    def connect(self):
        self.session = ftplib.FTP()
        self.session.connect(self.host, self.port)
        self.session.login(self.user, self.password)

    def close(self):
        try:
            self.session.quit()
        except:
            pass

    def reconnect(self):
        self.close()
        self.connect()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _parse_MLSD(self, parent, line):
        # modify=20170623195719;perm=adfr;size=454060;type=file;unique=13U670966;UNIX.group=570;UNIX.mode=0444;UNIX.owner=14; gbrel.txt
        fields = line.split(";")
        stat_dict = {}
        for field in fields:
            if "=" in field:
                # key-value field
                fv = field.strip()
                fv_idx = fv.index("=")
                key = fv[:fv_idx].lower()
                value = fv[fv_idx+1:]
            else:
                key = "name"
                value = field.strip()

            stat_dict[key] = value

        full_path = parent.rstrip("/") + "/" + stat_dict["name"]

        directory = False
        symlink = False
        if "type" in stat_dict:
            t = stat_dict["type"]
            if t in ["cdir", "pdir"]:
                return None

            if t == "dir":
                directory = True
            elif t == "OS.unix=symlink":
                symlink = True
            elif t.startswith("OS.unix=slink:"):
                symlink = True

            if t not in ["dir", "file", "OS.unix=symlink"]:
                raise IOError("Unknown type : %s" % t)

        size = 0
        if "size" in stat_dict:
            size = long(stat_dict["size"])

        modify_time = None
        if "modify" in stat_dict:
            modify_time_obj = datetime.strptime(stat_dict["modify"], "%Y%m%d%H%M%S")
            modify_time = time.mktime(modify_time_obj.timetuple())

        create_time = None
        if "create" in stat_dict:
            create_time_obj = datetime.strptime(stat_dict["create"], "%Y%m%d%H%M%S")
            create_time = time.mktime(create_time_obj.timetuple())

        if "name" in stat_dict:
            return ftp_status(
                directory=directory,
                symlink=symlink,
                path=full_path,
                name=stat_dict["name"],
                size=size,
                create_time=create_time,
                modify_time=modify_time
            )
        else:
            return None

    def _ensureDirEntryStatLoaded(self, path):
        # reuse cache
        if path in self.meta_cache:
            return self.meta_cache[path]

        stats = []
        try:
            entries = []
            self.session.cwd(path)
            self.session.retrlines("MLSD", entries.append)
            for ent in entries:
                st = self._parse_MLSD(path, ent)
                if st:
                    stats.append(st)
        except ftplib.error_perm:
            logger.error("_ensureDirEntryStatLoaded: " + traceback.format_exc())
            traceback.print_exc()

        self.meta_cache[path] = stats
        return stats

    """
    Returns ftp_status
    """
    def stat(self, path):
        try:
            # try bulk loading of stats
            parent = os.path.dirname(path)
            stats = self._ensureDirEntryStatLoaded(parent)
            if stats:
                for sb in stats:
                    if sb.path == path:
                        return sb
            return None
        except Exception:
            # fall if cannot access the parent dir
            return None

    """
    Returns directory entries in string
    """
    def list_dir(self, path):
        stats = self._ensureDirEntryStatLoaded(path)
        entries = []
        if stats:
            for sb in stats:
                entries.append(sb.name)
        return entries

    def is_dir(self, path):
        sb = self.stat(path)
        if sb:
            return sb.directory
        return False

    def make_dirs(self, path):
        if not self.exists(path):
            # make parent dir first
            self.make_dirs(os.path.dirname(path))
            self.session.mkd(path)
            # invalidate stat cache
            self.clear_stat_cache(os.path.dirname(path))

    def exists(self, path):
        try:
            sb = self.stat(path)
            if sb:
                return True
            return False
        except Exception:
            return False

    def clear_stat_cache(self, path=None):
        if(path):
            if path in self.meta_cache:
                # directory
                del self.meta_cache[path]
            else:
                # file
                parent = os.path.dirname(path)
                if parent in self.meta_cache:
                    del self.meta_cache[parent]
        else:
            self.meta_cache.clear()

    def read(self, path, offset, size):
        logger.info(
            "read : %s, off(%d), size(%d)" %
            (path, offset, size)
        )
        buf = None
        try:
            buf = BytesIO()

            logger.info("read: reading size - %d" % size)
            self.session.retrbinary(
                "RETR %s" % path,
                buf.write,
                rest=offset
            )
            logger.info("read: read done")
        except Exception, e:
            logger.error("read: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        #logger.info("read: returning the buf(" + buf + ")")
        return buf

    def write(self, path, offset, buf):
        logger.info(
            "write : %s, off(%d), size(%d)" %
            (path, offset, len(buf)))
        try:
            logger.info("write: writing buffer %d" % len(buf))

            bio = BytesIO()
            bio.write(buf)
            bio.seek(0)
            self.session.storbinary(
                "STOR %s" % path,
                bio,
                rest=offset
            )
            logger.info("write: writing done")
        except Exception, e:
            logger.error("write: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        # invalidate stat cache
        self.clear_stat_cache(path)

    def truncate(self, path, size):
        logger.info("truncate : %s" % path)
        raise IOError("truncate is not supported")

    def unlink(self, path):
        logger.info("unlink : %s" % path)
        try:
            logger.info("unlink: deleting a file - %s" % path)
            self.session.delete(path)
            logger.info("unlink: deleting done")

        except Exception, e:
            logger.error("unlink: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        # invalidate stat cache
        self.clear_stat_cache(path)

    def rename(self, path1, path2):
        logger.info("rename : %s -> %s" % (path1, path2))
        try:
            logger.info("rename: renaming a file - %s to %s" % (path1, path2))
            self.session.rename(path1, path2)
            logger.info("rename: renaming done")

        except Exception, e:
            logger.error("rename: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        # invalidate stat cache
        self.clear_stat_cache(path1)
        self.clear_stat_cache(path2)
