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
import threading

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
METADATA_CACHE_TTL = 60 * 60     # 1 hour

FTP_TIMEOUT = 5 * 60    # 5 min
BYTES_MAX_SKIP = 1024 * 1024 * 2 # 2MB
CONNECTIONS_MAX_NUM = 5

downloader_sessions = {}


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


class downloader_connection(object):
    def __init__(self,
                 path,
                 connection,
                 offset=0,
                 last_comm=0):
         self.path = path
         self.offset = offset
         self.connection = connection
         self.last_comm = last_comm
         self._lock = threading.RLock()

    def __repr__(self):
        return "<downloader_connection path(%s) off(%d) last_comm(%s)>" % \
            (self.path, self.offset, self.last_comm)

    def lock(self):
        self._lock.acquire()

    def unlock(self):
        self._lock.release()


class downloader_session(object):
    def __init__(self,
                 host,
                 port=21,
                 session=None):
         self.host = host
         self.port = port
         self.session = session
         self.connections = {}
         self._lock = threading.RLock()

    def __repr__(self):
        return "<downloader_session host(%s) port(%d)>" % \
            (self.host, self.port)

    def lock(self):
        self._lock.acquire()

    def unlock(self):
        self._lock.release()

    def _new_connection(self, path, offset=0):
        logger.info("_new_connection : %s, off(%d)" % (path, offset))

        self.lock()
        self.session.voidcmd("TYPE I")
        conn = self.session.transfercmd("RETR %s" % path, offset)
        connection = downloader_connection(path, conn, offset, datetime.now())
        self.connections[path] = connection
        self.unlock()
        return connection

    def _close_connection(self, connection):
        path = connection.path
        logger.info("_close_connection : %s" % path)

        self.lock()
        connection.lock()
        try:
            conn = connection.connection
            conn.close()
            self.session.voidresp()
        except ftplib.error_temp:
            # abortion of transfer causes this type of error
            pass

        del self.connections[path]
        connection.unlock()
        self.unlock()

    def _get_connection(self, path, offset):
        connection = None
        logger.info("_get_connection : %s, off(%d)" % (path, offset))

        self.lock()

        if path in self.connections:
            connection = self.connections[path]
            if connection:
                reusable = False
                connection.lock()

                coffset = connection.offset
                if coffset <= offset and coffset + BYTES_MAX_SKIP >= offset:
                    reusable = True

                time_delta = datetime.now() - connection.last_comm
                if time_delta.total_seconds() < FTP_TIMEOUT:
                    reusable = True

                connection.unlock()

                if not reusable:
                    self._close_connection(connection)
                    connection = None

        if len(self.connections) >= CONNECTIONS_MAX_NUM:
            # remove oldest
            oldest = None
            for live_connection in self.connections.values():
                if not oldest:
                    oldest = live_connection
                else:
                    if oldest.last_comm > live_connection.last_comm:
                        oldest = live_connection

            if oldest:
                self._close_connection(oldest)

        if connection:
            connection.lock()

            # may need to move offset
            skip_bytes = offset - connection.offset
            total_read = 0
            EOF = False
            while total_read < skip_bytes:
                data = connection.connection.recv(skip_bytes - total_read)
                if data:
                    data_len = len(data)
                    total_read += data_len
                    connection.offset += data_len
                else:
                    #EOF
                    EOF = True
                    break

            if total_read > 0:
                connection.last_comm = datetime.now()

            #connection.unlock()
        else:
            connection = self._new_connection(path, offset)
            connection.lock()

        self.unlock()
        return connection

    def read_data(self, path, offset, size):
        logger.info("read_data : %s, off(%d), size(%d)" % (path, offset, size))

        #connection is locked
        self.lock()
        connection = self._get_connection(path, offset)

        buf = BytesIO()
        total_read = 0
        EOF = False

        if connection.offset != offset:
            connection.unlock()
            self._close_connection(connection)
            connection = self._new_connection(path, offset)
            connection.lock()
            #raise Exception("Connection does not have right offset %d - %d" % (connection.offset, offset))

        while total_read < size:
            data = connection.connection.recv(size - total_read)
            if data:
                buf.write(data)
                data_len = len(data)
                total_read += data_len
                connection.offset += data_len
            else:
                #EOF
                EOF = True
                break

        if total_read > 0:
            connection.last_comm = datetime.now()

        connection.unlock()

        if EOF:
            self._close_connection(connection)

        self.unlock()
        return buf.getvalue()

    def close(self):
        self.lock()
        for connection in self.connections.values():
            logger.info("_close_connection : %s" % connection.path)

            connection.lock()
            try:
                conn = connection.connection
                conn.close()
                self.session.voidresp()
            except ftplib.error_temp:
                # abortion of transfer causes this type of error
                pass

            del self.connections[path]
            connection.unlock()

        self.unlock()

    def quit(self):
        self.session.quit()
        self.session = None

class prefetch_task(threading.Thread):
    def __init__(self,
                 group=None,
                 target=None,
                 name=None,
                 args=(),
                 kwargs=None,
                 verbose=None):
        threading.Thread.__init__(self, group=group, target=target, name=name, verbose=verbose)
        self.args = args
        self.kwargs = kwargs
        self.host = kwargs["host"]
        self.port = int(kwargs["port"])
        self.path = kwargs["path"]
        self.offset = kwargs["offset"]
        self.size = kwargs["size"]
        self.complete = False

    def run(self):
        logger.info("prefetch_task : %s:%d - %s, off(%d), size(%d)" % (self.host, self.port, self.path, self.offset, self.size))
        key = "%s:%d" % (self.host, self.port)
        session = downloader_sessions[key]
        try:
            buf = session.read_data(self.path, self.offset, self.size)
            logger.info("prefetch_task: read done")
        except Exception, e:
            logger.error("prefetch_task: " + traceback.format_exc())
            traceback.print_exc()

        self.data = buf
        self.complete = True


class ftp_client(object):
    def __init__(self,
                 host=None,
                 port=21,
                 user='anonymous',
                 password='anonymous@email.com'):
        self.host = host
        if port > 0:
            self.port = int(port)
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
        self.mlsd_supported = True
        self.prefetch_thread = None

        # init cache
        self.meta_cache = ExpiringDict(
            max_len=METADATA_CACHE_SIZE,
            max_age_seconds=METADATA_CACHE_TTL
        )

    def connect(self):
        logger.info("connect: connecting to FTP server (%s)" % self.host)
        ftp_session = ftplib.FTP()
        ftp_session.connect(self.host, self.port)
        ftp_session.login(self.user, self.password)
        self.last_comm = datetime.now()

        key = "%s:%d" % (self.host, self.port)
        self.session = downloader_session(self.host, self.port, ftp_session)
        downloader_sessions[key] = self.session

    def close(self):
        try:
            logger.info("close: closing a connectinn to FTP server (%s)" % self.host)
            key = "%s:%d" % (self.host, self.port)
            del downloader_sessions[key]

            self.session.close()
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
            logger.info("_reconnect_when_needed: expired - check live")
            self.session.lock()
            try:
                self.session.close()
                self.session.session.pwd()
                self.last_comm = datetime.now()
                return False
            except:
                self.reconnect()
                return True
            finally:
                self.session.unlock()
        else:
            return False

    def _list_dir_and_stat_MLSD(self, path):
        logger.info("_list_dir_and_stat_MLSD: retrlines with MLSD - %s" % path)
        stats = []
        self.session.lock()
        try:
            entries = []
            try:
                self.session.session.retrlines("MLSD", entries.append)
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
        finally:
            self.session.unlock()

        return stats

    def _list_dir_and_stat_LIST(self, path):
        logger.info("_list_dir_and_stat_LIST: retrlines with LIST - %s" % path)
        stats = []
        self.session.lock()
        try:
            entries = []
            self.session.session.retrlines("LIST", entries.append)
            self.last_comm = datetime.now()
            for ent in entries:
                st = self._parse_LIST(path, ent)
                if st:
                    stats.append(st)
        except ftplib.error_perm:
            logger.error("_list_dir_and_stat_LIST: " + traceback.format_exc())
            traceback.print_exc()
        finally:
            self.session.unlock()

        return stats

    def _ensureDirEntryStatLoaded(self, path):
        # reuse cache
        if path in self.meta_cache:
            return self.meta_cache[path]

        logger.info("_ensureDirEntryStatLoaded: loading - %s" % path)
        self._reconnect_when_needed()
        self.session.lock()
        self.session.session.cwd(path)
        self.session.unlock()

        stats = []
        if self.mlsd_supported:
            try:
                stats = self._list_dir_and_stat_MLSD(path)
            except MLSD_NOT_SUPPORTED:
                self.mlsd_supported = False
                stats = self._list_dir_and_stat_LIST(path)
        else:
            stats = self._list_dir_and_stat_LIST(path)

        self.meta_cache[path] = stats
        return stats

    def _invoke_prefetch(self, path, offset, size):
        logger.info("_invoke_prefetch : %s, off(%d), size(%d)" % (path, offset, size))
        self.prefetch_thread = prefetch_task(name="prefetch_task_thread", kwargs={'host':self.session.host, 'port':self.session.port, 'path':path, 'offset':offset, 'size':size})
        self.prefetch_thread.start()

    def _get_prefetch_data(self, path, offset, size):
        logger.info("_get_prefetch_data : %s, off(%d), size(%d)" % (path, offset, size))
        if not self.prefetch_thread:
            self._invoke_prefetch(path, offset, size)

        if self.prefetch_thread.path == path:
            if self.prefetch_thread.offset <= offset and (self.prefetch_thread.offset + self.prefetch_thread.size) >= offset and (self.prefetch_thread.offset + self.prefetch_thread.size) >= (offset + size):
                if not self.prefetch_thread.complete:
                    self.prefetch_thread.join()

                data = self.prefetch_thread.data
                self.prefetch_thread = None
                return data

        self.prefetch_thread = None
        self._invoke_prefetch(path, offset, size)
        self.prefetch_thread.join()
        data = self.prefetch_thread.data
        self.prefetch_thread = None
        return data

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
            self.session.lock()
            self.session.session.mkd(path)
            self.session.unlock()
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
            time1 = datetime.now()
            buf = self._get_prefetch_data(path, offset, size)
            #buf = self.session.read_data(path, offset, size)
            if len(buf) == size:
                self._invoke_prefetch(path, offset + size, size)
            time2 = datetime.now()
            delta = time2 - time1
            logger.info("read: took - %s" % delta)
            logger.info("read: read done")
        except Exception, e:
            logger.error("read: " + traceback.format_exc())
            traceback.print_exc()
            raise e

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
            self.session.lock()
            self.session.session.storbinary(
                "STOR %s" % path,
                bio,
                rest=offset
            )
            self.session.unlock()
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
            self.session.lock()
            self.session.session.delete(path)
            self.session.unlock()
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
            self.session.lock()
            self.session.session.rename(path1, path2)
            self.session.unlock()
            self.last_comm = datetime.now()
            logger.info("rename: renaming done")

        except Exception, e:
            logger.error("rename: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        # invalidate stat cache
        self.clear_stat_cache(path1)
        self.clear_stat_cache(path2)
