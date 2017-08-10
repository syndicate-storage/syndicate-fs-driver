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

"""
General Dropbox Plugin
"""
import os
import logging
import threading
import sgfsdriver.lib.abstractfs as abstractfs
import sgfsdriver.plugins.dropbox.dropbox_client as dropbox_client

logger = logging.getLogger('syndicate_Dropbox_filesystem')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('syndicate_Dropbox_filesystem.log')
fh.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)


def reconnectAtDropboxFail(func):
    def wrap(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            logger.info("failed to process an operation : " + str(e))
            if self.dropbox:
                logger.info("reconnect: trying to reconnect to Dropbox")
                self.dropbox.reconnect()
                logger.info("calling the operation again")
                return func(self, *args, **kwargs)

    return wrap


class plugin_impl(abstractfs.afsbase):
    def __init__(self, config, role=abstractfs.afsrole.DISCOVER):
        logger.info("__init__")

        if not config:
            raise ValueError("fs configuration is not given correctly")

        work_root = config.get("work_root")
        if not work_root:
            raise ValueError("work_root configuration is not given correctly")

        secrets = config.get("secrets")
        if not secrets:
            raise ValueError("secrets are not given correctly")

        access_token = secrets.get("access_token")
        access_token = access_token.encode('ascii', 'ignore')
        if not access_token:
            raise ValueError("user is not given correctly")

        # set role
        self._role = role

        # config can have unicode strings
        work_root = work_root.encode('ascii', 'ignore')
        self.work_root = work_root.rstrip("/")

        logger.info("__init__: initializing dropbox_client")
        self.dropbox = dropbox_client.dropbox_client(access_token=access_token)

        self.notification_cb = None
        # create a re-entrant lock (not a read lock)
        self.lock = threading.RLock()

    def _lock(self):
        self.lock.acquire()

    def _unlock(self):
        self.lock.release()

    def _get_lock(self):
        return self.lock

    def on_update_detected(self, operation, path):
        logger.info("on_update_detected - %s, %s" % (operation, path))

        ascii_path = path.encode('ascii', 'ignore')
        driver_path = self._make_driver_path(ascii_path)

        self.clear_cache(driver_path)
        if operation == "remove":
            if self.notification_cb:
                entry = abstractfs.afsevent(driver_path, None)
                self.notification_cb([], [], [entry])
        elif operation in ["create", "modify"]:
            if self.notification_cb:
                sb = self.stat(driver_path)
                if sb:
                    entry = abstractfs.afsevent(driver_path, sb)
                    if operation == "create":
                        self.notification_cb([], [entry], [])
                    elif operation == "modify":
                        self.notification_cb([entry], [], [])

    def _make_dropbox_path(self, path):
        logger.info("_make_dropbox_path: %s" % path)
        if path.startswith(self.work_root):
            temp = path.rstrip("/")
            if temp == '/':
                return ''
            else:
                return temp

        if path.startswith("/"):
	     temp = self.work_root.rstrip("/") + path.rstrip("/")
             if temp == '/':
                 return ''
             else:
                 return temp

        temp = self.work_root.rstrip("/") + "/" + path.rstrip("/")

	if temp == '/':
             return ''

        return temp

    def _make_driver_path(self, path):
        logger.info("_make_driver_path: %s" % path)
        if path.startswith(self.work_root):
            return path[len(self.work_root):].rstrip("/")
        return path.rstrip("/")

    def connect(self):
        logger.info("connect: connecting to Dropbox")

        self.dropbox.connect()

        if self._role == abstractfs.afsrole.DISCOVER:
            if not self.dropbox.exists(self.work_root):
                raise IOError("work_root does not exist")

    def close(self):
        logger.info("close")
        logger.info("close: closing Dropbox")
        if self.dropbox:
            self.dropbox.close()

    @reconnectAtDropboxFail
    def stat(self, path):
        logger.info("stat - %s" % path)

        with self._get_lock():
            ascii_path = path.encode('ascii', 'ignore')
            dropbox_path = self._make_dropbox_path(ascii_path)
            driver_path = self._make_driver_path(ascii_path)
            # get stat
            sb = self.dropbox.stat(dropbox_path)
            if sb:
                return abstractfs.afsstat(directory=sb.directory,
                                          path=driver_path,
                                          name=os.path.basename(driver_path),
                                          size=sb.size,
                                          checksum=sb.checksum,
                                          create_time=sb.create_time,
                                          modify_time=sb.modify_time)
            else:
                return None

    @reconnectAtDropboxFail
    def exists(self, path):
        logger.info("exists - %s" % path)

        with self._get_lock():
            logger.info("exists - 1")
            ascii_path = path.encode('ascii', 'ignore')
            logger.info("exists - 2")
            dropbox_path = self._make_dropbox_path(ascii_path)
            logger.info("exists - 3")
            exist = self.dropbox.exists(dropbox_path)
            logger.info("exists - 4")
            return exist

    @reconnectAtDropboxFail
    def list_dir(self, dirpath):
        logger.info("list_dir - %s" % dirpath)

        with self._get_lock():
            ascii_path = dirpath.encode('ascii', 'ignore')
            dropbox_path = self._make_dropbox_path(ascii_path)
            l = self.dropbox.list_dir(dropbox_path)
            return l

    @reconnectAtDropboxFail
    def is_dir(self, dirpath):
        logger.info("is_dir - %s" % dirpath)

        with self._get_lock():
            ascii_path = dirpath.encode('ascii', 'ignore')
            dropbox_path = self._make_dropbox_path(ascii_path)
            d = self.dropbox.is_dir(dropbox_path)
            return d

    @reconnectAtDropboxFail
    def make_dirs(self, dirpath):
        logger.info("make_dirs - %s" % dirpath)

        with self._get_lock():
            ascii_path = dirpath.encode('ascii', 'ignore')
            dropbox_path = self._make_dropbox_path(ascii_path)
            if not self.exists(dropbox_path):
                self.dropbox.make_dirs(dropbox_path)

    @reconnectAtDropboxFail
    def read(self, filepath, offset, size):
        logger.info("read - %s, %d, %d" % (filepath, offset, size))

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            dropbox_path = self._make_dropbox_path(ascii_path)
            buf = self.dropbox.read(dropbox_path, offset, size)
            return buf

    @reconnectAtDropboxFail
    def write(self, filepath, offset, buf):
        logger.info("write - %s, %d, %d" % (filepath, offset, len(buf)))

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            dropbox_path = self._make_dropbox_path(ascii_path)
            self.dropbox.write(dropbox_path, offset, buf)

    @reconnectAtDropboxFail
    def truncate(self, filepath, size):
        logger.info("truncate - %s, %d" % (filepath, size))

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            dropbox_path = self._make_dropbox_path(ascii_path)
            self.dropbox.truncate(dropbox_path, size)

    @reconnectAtDropboxFail
    def clear_cache(self, path):
        logger.info("clear_cache - %s" % path)

        with self._get_lock():
            if path:
                ascii_path = path.encode('ascii', 'ignore')
                dropbox_path = self._make_dropbox_path(ascii_path)
                self.dropbox.clear_stat_cache(dropbox_path)
            else:
                self.dropbox.clear_stat_cache(None)

    @reconnectAtDropboxFail
    def unlink(self, filepath):
        logger.info("unlink - %s" % filepath)

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            dropbox_path = self._make_dropbox_path(ascii_path)
            self.dropbox.unlink(dropbox_path)

    @reconnectAtDropboxFail
    def rename(self, filepath1, filepath2):
        logger.info("rename - %s to %s" % (filepath1, filepath2))

        with self._get_lock():
            ascii_path1 = filepath1.encode('ascii', 'ignore')
            ascii_path2 = filepath2.encode('ascii', 'ignore')
            dropbox_path1 = self._make_dropbox_path(ascii_path1)
            dropbox_path2 = self._make_dropbox_path(ascii_path2)
            self.dropbox.rename(dropbox_path1, dropbox_path2)

    '''
    @reconnectAtDropboxFail
    def set_xattr(self, filepath, key, value):
        logger.info("set_xattr - %s, %s=%s" % (filepath, key, value))

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            dropbox_path = self._make_dropbox_path(ascii_path)
            self.dropbox.set_xattr(dropbox_path, key, value)

    @reconnectAtDropboxFail
    def get_xattr(self, filepath, key):
        logger.info("get_xattr - %s, %s" % (filepath, key))

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            dropbox_path = self._make_dropbox_path(ascii_path)
            return self.dropbox.get_xattr(dropbox_path, key)

    @reconnectAtDropboxFail
    def list_xattr(self, filepath):
        logger.info("list_xattr - %s" % filepath)

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            dropbox_path = self._make_dropbox_path(ascii_path)
            return self.dropbox.list_xattr(dropbox_path)
    '''

    def plugin(self):
        return self.__class__

    def role(self):
        return self._role

    def set_notification_cb(self, notification_cb):
        logger.info("set_notification_cb")

        self.notification_cb = notification_cb

    def get_supported_gateways(self):
        return [abstractfs.afsgateway.AG, abstractfs.afsgateway.RG]

    def get_supported_replication_mode(self):
        return [
            #abstractfs.afsreplicationmode.BLOCK
            abstractfs.afsreplicationmode.FILE
        ]
