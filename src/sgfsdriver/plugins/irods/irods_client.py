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

from irods.session import iRODSSession
from irods.models import DataObject
from irods.meta import iRODSMeta
from irods.exception import CollectionDoesNotExist, DataObjectDoesNotExist
from expiringdict import ExpiringDict

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
METADATA_CACHE_TTL = 60     # 60 sec

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

        # init cache
        self.meta_cache = ExpiringDict(
            max_len=METADATA_CACHE_SIZE,
            max_age_seconds=METADATA_CACHE_TTL
        )

    def connect(self):
        self.session = iRODSSession(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            zone=self.zone
        )

    def close(self):
        self.session.cleanup()

    def reconnect(self):
        self.close()
        self.connect()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

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
            logger.info("read: opening a file - %s" % path)
            obj = self.session.data_objects.get(path)
            with obj.open('r') as f:
                if offset != 0:
                    logger.info("read: seeking at %d" % offset)
                    new_offset = f.seek(offset)
                    if new_offset != offset:
                        logger.error(
                            "read: offset mismatch - requested(%d), "
                            "but returned(%d)" %
                            (offset, new_offset))
                        raise Exception(
                            "read: offset mismatch - requested(%d), "
                            "but returned(%d)" %
                            (offset, new_offset))

                logger.info("read: reading size - %d" % size)
                buf = f.read(size)
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

    def download(self, path, to):
        obj = self.session.data_objects.get(path)
        with obj.open('r') as f:
            try:
                with open(to, 'w') as wf:
                    while(True):
                        buf = f.read(1024*1024)

                        if not buf:
                            break

                        wf.write(buf)
            except Exception, e:
                logger.error("download: " + traceback.format_exc())
                traceback.print_exc()
                raise e

        return to
