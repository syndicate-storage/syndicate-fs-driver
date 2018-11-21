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
import threading

from datetime import datetime
from irods.session import iRODSSession
from irods.models import DataObject
from irods.meta import iRODSMeta
from irods.exception import CollectionDoesNotExist, DataObjectDoesNotExist
from expiringdict import ExpiringDict
from io import BytesIO

logger = logging.getLogger('irods_client')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('irods_client.log')
fh.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)

METADATA_CACHE_SIZE = 10000
METADATA_CACHE_TTL = 60 * 60     # 1 hour

IRODS_TIMEOUT = 5 * 60    # 5 min
OBJECTS_MAX_NUM = 5

"""
Interface class to iRODS
"""


class irods_status(object):
    def __init__(self,
                 directory=False,
                 path=None,
                 name=None,
                 size=0,
                 checksum=0,
                 create_time=0,
                 modify_time=0):
        self.directory = directory
        self.path = path
        self.name = name
        self.size = size
        self.checksum = checksum
        self.create_time = create_time
        self.modify_time = modify_time

    @classmethod
    def fromCollection(cls, col):
        return irods_status(
            directory=True,
            path=col.path,
            name=col.name
        )

    @classmethod
    def fromDataObject(cls, obj):
        return irods_status(
            directory=False,
            path=obj.path,
            name=obj.name,
            size=obj.size,
            checksum=obj.checksum,
            create_time=obj.create_time,
            modify_time=obj.modify_time
        )

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        rep_d = "F"
        if self.directory:
            rep_d = "D"

        return "<irods_status %s %s %d %s>" % \
            (rep_d, self.name, self.size, self.checksum)


class irods_object(object):
    def __init__(self,
                 path,
                 dataobj,
                 offset=0,
                 last_comm=0):
         self.path = path
         self.offset = offset
         self.dataobj = dataobj
         self.last_comm = last_comm
         self._lock = threading.RLock()

    def __repr__(self):
        return "<irods_object path(%s) off(%d) last_comm(%s)>" % \
            (self.path, self.offset, self.last_comm)

    def lock(self):
        self._lock.acquire()

    def unlock(self):
        self._lock.release()


class irods_object_pool(object):
    def __init__(self):
         self.session = None
         self.last_comm = None

         self.objects = {}
         self._lock = threading.RLock()

    def init(self, session):
        self.session = session

    def clear(self):
        logger.info("close")
        self.lock()

        for obj in self.objects.values():
            logger.info("close : %s" % obj.path)

            obj.lock()
            try:
                obj.dataobj.close()
            except:
                pass

            del self.objects[obj.path]
            obj.unlock()

        self.unlock()

    def lock(self):
        self._lock.acquire()

    def unlock(self):
        self._lock.release()

    def _new_dataobject(self, path, offset=0):
        logger.info("_new_dataobject : %s, off(%d)" % (path, offset))

        self.lock()
        try:
            obj = self.session.data_objects.get(path)
            f = obj.open('r')
            if offset != 0:
                f.seek(offset)

            dataobj = irods_object(path, f, offset, datetime.now())
            self.objects[path] = dataobj
            self.last_comm = datetime.now()
        finally:
            self.unlock()
        return dataobj

    def _close_dataobject(self, obj):
        path = obj.path
        logger.info("_close_dataobject : %s" % path)

        self.lock()
        obj.lock()
        try:
            f = obj.dataobj
            f.close()
            self.last_comm = datetime.now()
        finally:
            del self.objects[path]
            obj.unlock()
            self.unlock()

    def _get_dataobject(self, path, offset):
        obj = None
        logger.info("_get_dataobject : %s, off(%d)" % (path, offset))

        self.lock()

        if path in self.objects:
            obj = self.objects[path]
            if obj:
                reusable = False
                obj.lock()

                time_delta = datetime.now() - obj.last_comm
                if time_delta.total_seconds() < IRODS_TIMEOUT:
                    reusable = True

                obj.unlock()

                if not reusable:
                    self._close_dataobject(obj)
                    obj = None

        if len(self.objects) >= OBJECTS_MAX_NUM:
            # remove oldest
            oldest = None
            for live_obj in self.objects.values():
                if not oldest:
                    oldest = live_obj
                else:
                    if oldest.last_comm > live_obj.last_comm:
                        oldest = live_obj

            if oldest:
                self._close_dataobject(oldest)

        if obj:
            obj.lock()

            # may need to move offset
            if offset != obj.offset:
                f = obj.dataobj
                f.seek(offset)
                obj.offset = offset

                obj.last_comm = datetime.now()
                self.last_comm = datetime.now()
        else:
            obj = self._new_dataobject(path, offset)
            obj.lock()

        self.unlock()
        return obj

    def read_data(self, path, offset, size):
        logger.info("read_data : %s, off(%d), size(%d)" % (path, offset, size))

        #obj is locked
        self.lock()
        obj = self._get_dataobject(path, offset)

        EOF = False

        if obj.offset != offset:
            obj.unlock()
            self._close_dataobject(obj)
            obj = self._new_dataobject(path, offset)
            obj.lock()
            #raise Exception("Connection does not have right offset %d - %d" % (connection.offset, offset))

        f = obj.dataobj
        buf = f.read(size)
        data_len = len(buf)
        obj.offset = obj.offset + data_len

        if data_len == 0:
            EOF = True

        obj.last_comm = datetime.now()

        obj.unlock()

        if EOF:
            self._close_dataobject(obj)

        self.unlock()
        return buf


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
        self.session = kwargs["session"]
        self.path = kwargs["path"]
        self.offset = kwargs["offset"]
        self.size = kwargs["size"]
        self.irods_client = kwargs["irods_client"]
        self.complete = False
        self.data = None

    def run(self):
        logger.info("prefetch_task : %s, off(%d), size(%d)" % (self.path, self.offset, self.size))
        object_pool = self.irods_client.get_object_pool()
        object_pool.lock()
        buf = None
        try:
            buf = object_pool.read_data(self.path, self.offset, self.size)
            logger.info("prefetch_task: read done")
        except Exception, e:
            logger.error("prefetch_task: " + traceback.format_exc())
        finally:
            object_pool.unlock()

        self.data = buf
        self.complete = True


class irods_client(object):
    def __init__(self,
                 host=None,
                 port=1247,
                 user=None,
                 password=None,
                 zone=None):
        self.host = host
        if port:
            self.port = port
        else:
            self.port = 1247
        self.user = user
        self.password = password
        self.zone = zone
        self.session = None
        self.object_pool = irods_object_pool()

        self.prefetch_thread = None

        # init cache
        self.meta_cache = ExpiringDict(
            max_len=METADATA_CACHE_SIZE,
            max_age_seconds=METADATA_CACHE_TTL
        )

    def connect(self):
        logger.info("connect: connecting to iRODS server (%s)" % self.host)
        self.session = iRODSSession(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            zone=self.zone
        )
        self.object_pool.init(self.session)

    def close(self):
        try:
            self.object_pool.clear()
            self.session.cleanup()
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

    def get_object_pool(self):
        return self.object_pool

    def _ensureDirEntryStatLoaded(self, path):
        # reuse cache
        if path in self.meta_cache:
            return self.meta_cache[path]

        coll = self.session.collections.get(path)
        stats = []
        for col in coll.subcollections:
            stats.append(irods_status.fromCollection(col))

        for obj in coll.data_objects:
            stats.append(irods_status.fromDataObject(obj))

        self.meta_cache[path] = stats
        return stats

    def _invoke_prefetch(self, path, offset, size):
        logger.info("_invoke_prefetch : %s, off(%d), size(%d)" % (path, offset, size))
        self.prefetch_thread = prefetch_task(name="prefetch_task_thread", kwargs={'session':self.session, 'path':path, 'offset':offset, 'size':size, 'irods_client':self})
        self.prefetch_thread.start()

    def _get_prefetch_data(self, path, offset, size):
        logger.info("_get_prefetch_data : %s, off(%d), size(%d)" % (path, offset, size))
        invoke_new_thread = False
        if self.prefetch_thread:
            if not self.prefetch_thread.complete:
                self.prefetch_thread.join()

            if self.prefetch_thread.path != path or self.prefetch_thread.offset != offset or self.prefetch_thread.size != size:
                invoke_new_thread = True
        else:
            invoke_new_thread = True

        if invoke_new_thread:
            self._invoke_prefetch(path, offset, size)
            self.prefetch_thread.join()

        data = self.prefetch_thread.data
        self.prefetch_thread = None
        return data

    """
    Returns irods_status
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
        except (CollectionDoesNotExist):
            # fall if cannot access the parent dir
            try:
                # we only need to check the case if the path is a collection
                # because if it is a file, it's parent dir must be accessible
                # thus, _ensureDirEntryStatLoaded should succeed.
                return irods_status.fromCollection(
                    self.session.collections.get(path))
            except (CollectionDoesNotExist):
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
            self.session.collections.create(path)
            # invalidate stat cache
            self.clear_stat_cache(os.path.dirname(path))

    def exists(self, path):
        try:
            sb = self.stat(path)
            if sb:
                return True
            return False
        except (CollectionDoesNotExist, DataObjectDoesNotExist):
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
            "read : %s, off(%d), size(%d)" % (path, offset, size)
        )
        buf = None
        try:
            sb = self.stat(path)
            if offset >= sb.size:
                # EOF
                buf = BytesIO()
                return buf.getvalue()

            time1 = datetime.now()
            buf = self._get_prefetch_data(path, offset, size)
            read_len = len(buf)
            if read_len + offset < sb.size:
                self._invoke_prefetch(path, offset + read_len, size)

            time2 = datetime.now()
            delta = time2 - time1
            logger.info("read: took - %s" % delta)
            logger.info("read: read done")

        except Exception, e:
            logger.error("read: " + traceback.format_exc())
            raise e

        return buf

    def write(self, path, offset, buf):
        logger.info(
            "write : %s, off(%d), size(%d)" %
            (path, offset, len(buf)))
        try:
            obj = None
            if self.exists(path):
                logger.info("write: opening a file - %s" % path)
                obj = self.session.data_objects.get(path)
            else:
                logger.info("write: creating a file - %s" % path)
                obj = self.session.data_objects.create(path)
            with obj.open('w') as f:
                if offset != 0:
                    logger.info("write: seeking at %d" % offset)
                    new_offset = f.seek(offset)
                    if new_offset != offset:
                        logger.error(
                            "write: offset mismatch - requested(%d), "
                            "but returned(%d)" %
                            (offset, new_offset))
                        raise Exception(
                            "write: offset mismatch - requested(%d), "
                            "but returned(%d)" %
                            (offset, new_offset))

                logger.info("write: writing buffer %d" % len(buf))
                f.write(buf)
                logger.info("write: writing done")

        except Exception, e:
            logger.error("write: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        # invalidate stat cache
        self.clear_stat_cache(path)

    def truncate(self, path, size):
        logger.info("truncate : %s" % path)
        try:
            logger.info("truncate: truncating a file - %s" % path)
            self.session.data_objects.truncate(path, size)
            logger.info("truncate: truncating done")

        except Exception, e:
            logger.error("truncate: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        # invalidate stat cache
        self.clear_stat_cache(path)

    def unlink(self, path):
        logger.info("unlink : %s" % path)
        try:
            logger.info("unlink: deleting a file - %s" % path)
            self.session.data_objects.unlink(path)
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
            self.session.data_objects.move(path1, path2)
            logger.info("rename: renaming done")

        except Exception, e:
            logger.error("rename: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        # invalidate stat cache
        self.clear_stat_cache(path1)
        self.clear_stat_cache(path2)

    def set_xattr(self, path, key, value):
        logger.info("set_xattr : %s - %s" % (key, value))
        try:
            logger.info(
                "set_xattr: set extended attribute to a file %s %s=%s" %
                (path, key, value))
            self.session.metadata.set(DataObject, path, iRODSMeta(key, value))
            logger.info("set_xattr: done")

        except Exception, e:
            logger.error("set_xattr: " + traceback.format_exc())
            traceback.print_exc()
            raise e

    def get_xattr(self, path, key):
        logger.info("get_xattr : " + key)
        value = None
        try:
            logger.info(
                "get_xattr: get extended attribute from a file - %s %s" %
                (path, key))
            attrs = self.session.metadata.get(DataObject, path)
            for attr in attrs:
                if key == attr.name:
                    value = attr.value
                    break
            logger.info("get_xattr: done")

        except Exception, e:
            logger.error("get_xattr: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        return value

    def list_xattr(self, path):
        logger.info("list_xattr : %s" % key)
        keys = []
        try:
            logger.info(
                "list_xattr: get extended attributes from a file - %s" %
                path)
            attrs = self.session.metadata.get(DataObject, path)
            for attr in attrs:
                keys.append(attr.name)
            logger.info("list_xattr: done")

        except Exception, e:
            logger.error("list_xattr: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        return keys
