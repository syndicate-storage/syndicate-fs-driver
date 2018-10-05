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

FTP_TIMEOUT = 30    # 30 sec

"""
Interface class to FTP
"""


class MLSD_NOT_SUPPORTED(Exception):
    pass


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
        self.last_comm = None
        self.opened_file = None
        self.opened_file_offset = 0
        self.opened_conn = None

        self.mlsd_supported = True

        # init cache
        self.meta_cache = ExpiringDict(
            max_len=METADATA_CACHE_SIZE,
            max_age_seconds=METADATA_CACHE_TTL
        )

    def connect(self):
        logger.info("connect: connecting to FTP server (%s)" % self.host)

        self.session = ftplib.FTP()
        self.session.connect(self.host, self.port)
        self.session.login(self.user, self.password)
        self.last_comm = datetime.now()

    def close(self):
        try:
            logger.info("close: closing a connectinn to FTP server (%s)" % self.host)
            self.session.quit()
            self.last_comm = None
        except:
            pass

    def reconnect(self):
        logger.info("reconnect: reconnecting to FTP server (%s)" % self.host)
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

    def _parse_LIST(self, parent, line):
        # drwxr-xr-x    8 20002    2006         4096 Dec 08  2015 NANOGrav_9y
        fields = line.split()
        stat_dict = {}
        stat_dict["perm"] = fields[0]
        stat_dict["owner"] = fields[2]
        stat_dict["group"] = fields[3]
        stat_dict["size"] = fields[4]
        stat_dict["month"] = fields[5]
        stat_dict["day"] = fields[6]
        stat_dict["d3"] = fields[7]
        stat_dict["name"] = fields[8]

        full_path = parent.rstrip("/") + "/" + stat_dict["name"]

        directory = False
        symlink = False

        if stat_dict["perm"].startswith("d"):
            directory = True
        elif stat_dict["perm"].startswith("-"):
            directory = False
        elif stat_dict["perm"].startswith("l"):
            directory = True
            symlink = True
        else:
            raise IOError("Unknown type : %s" % stat_dict["perm"])

        size = 0
        if "size" in stat_dict:
            size = long(stat_dict["size"])

        now = datetime.now()
        year = now.year
        hour = 0
        minute = 0

        if stat_dict["d3"].isdigit():
            year = int(stat_dict["d3"])
        else:
            hm = stat_dict["d3"].split(":")
            hour = int(hm[0])
            minute = int(hm[1])

        d = "%d %s %s %d %d" % (
            year, stat_dict["month"], stat_dict["day"], hour, minute
        )

        modify_time = None
        if "modify" in stat_dict:
            modify_time_obj = datetime.strptime(d, "%Y %b %d %H %M")
            modify_time = time.mktime(modify_time_obj.timetuple())

        create_time = None
        if "create" in stat_dict:
            create_time_obj = datetime.strptime(d, "%Y %b %d %H %M")
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

    def _reconnect_when_needed(self):
        expired = True
        if self.last_comm:
            delta = datetime.now() - self.last_comm
            if delta.total_seconds() < FTP_TIMEOUT:
                expired = False

        if expired:
            # perform a short command then reconnect at fail
            try:
                logger.info("_reconnect_when_needed: check live")
                self.session.pwd()
                self.last_comm = datetime.now()
                return False
            except:
                self.reconnect()
                return True
        else:
            return False

    def _list_dir_and_stat_MLSD(self, path):
        logger.info("_list_dir_and_stat_MLSD: retrlines with MLSD - %s" % path)
        stats = []
        try:
            entries = []
            try:
                self.session.retrlines("MLSD", entries.append)
            except ftplib.error_perm, e:
                msg = str(e)
                if "500" in msg or "unknown command" in msg.lower():
                    raise MLSD_NOT_SUPPORTED("MLSD is not supported")

            self.last_comm = datetime.now()
            for ent in entries:
                st = self._parse_MLSD(path, ent)
                if st:
                    stats.append(st)
        except ftplib.error_perm:
            logger.error("_list_dir_and_stat_MLSD: " + traceback.format_exc())
            traceback.print_exc()

        return stats

    def _list_dir_and_stat_LIST(self, path):
        logger.info("_list_dir_and_stat_LIST: retrlines with LIST - %s" % path)
        stats = []
        try:
            entries = []
            self.session.retrlines("LIST", entries.append)
            self.last_comm = datetime.now()
            for ent in entries:
                st = self._parse_LIST(path, ent)
                if st:
                    stats.append(st)
        except ftplib.error_perm:
            logger.error("_list_dir_and_stat_LIST: " + traceback.format_exc())
            traceback.print_exc()

        return stats

    def _list_dir_and_stat(self, path):
        logger.info("_list_dir_and_stat: retrlines - %s" % path)
        stats = []
        if self.mlsd_supported:
            try:
                stats = self._list_dir_and_stat_MLSD(path)
            except MLSD_NOT_SUPPORTED:
                self.mlsd_supported = False
                stats = self._list_dir_and_stat_LIST(path)
        else:
            stats = self._list_dir_and_stat_LIST(path)

        return stats

    def _ensureDirEntryStatLoaded(self, path):
        # reuse cache
        if path in self.meta_cache:
            return self.meta_cache[path]

        logger.info("_ensureDirEntryStatLoaded: change working directory - %s" % path)
        self._reconnect_when_needed()
        self.session.cwd(path)

        stats = self._list_dir_and_stat(path)
        self.meta_cache[path] = stats
        return stats

    def _openFile(self, path, offset):
        logger.info("_openFile : %s, off(%d)" % (path, offset))

        reset = True
        reconn = self._reconnect_when_needed()
        if self.opened_conn and self.opened_file == path and self.opened_file_offset == offset:
            # reuse
            # if reconnected, reset
            reset = reconn

        if reset:
            logger.info("_openFile : resetting file transfer for %s" % path)
            try:
                if self.opened_conn:
                    self.opened_conn.close()
                    self.session.voidresp()
            except ftplib.error_temp:
                # abortion of transfer causes this type of error
                pass

            self.session.voidcmd("TYPE I")
            conn = self.session.transfercmd("RETR %s" % path, offset)
            self.last_comm = datetime.now()
            self.opened_file = path
            self.opened_file_offset = offset
            self.opened_conn = conn
            return conn
        else:
            logger.info("_openFile : reusing file transfer for %s" % path)
            return self.opened_conn

    def _readRange(self, path, offset, size):
        logger.info("_readRange : %s, off(%d), size(%d)" % (path, offset, size))

        conn = self._openFile(path, offset)

        buf = BytesIO()
        total_read = 0

        while total_read < size:
            data = conn.recv(size - total_read)
            if data:
                buf.write(data)
                data_len = len(data)
                total_read += data_len
                self.opened_file_offset += data_len
            else:
                break

        self.last_comm = datetime.now()

        bybuf = buf.getvalue()
        return bybuf

    """
    Returns ftp_status
    """
    def stat(self, path):
        logger.info("stat: %s" % path)
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
        logger.info("list_dir: %s" % path)
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
        logger.info("make_dirs: %s" % path)
        if not self.exists(path):
            # make parent dir first
            self._reconnect_when_needed()
            self.make_dirs(os.path.dirname(path))
            self.session.mkd(path)
            self.last_comm = datetime.now()
            # invalidate stat cache
            self.clear_stat_cache(os.path.dirname(path))

    def exists(self, path):
        logger.info("exists: %s" % path)
        try:
            sb = self.stat(path)
            if sb:
                return True
            return False
        except Exception:
            return False

    def clear_stat_cache(self, path=None):
        logger.info("clear_stat_cache: %s" % path)
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
            buf = self._readRange(path, offset, size)
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
            self._reconnect_when_needed()
            self.session.storbinary(
                "STOR %s" % path,
                bio,
                rest=offset
            )
            self.last_comm = datetime.now()
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
            self._reconnect_when_needed()
            self.session.delete(path)
            self.last_comm = datetime.now()
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
            self._reconnect_when_needed()
            self.session.rename(path1, path2)
            self.last_comm = datetime.now()
            logger.info("rename: renaming done")

        except Exception, e:
            logger.error("rename: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        # invalidate stat cache
        self.clear_stat_cache(path1)
        self.clear_stat_cache(path2)
