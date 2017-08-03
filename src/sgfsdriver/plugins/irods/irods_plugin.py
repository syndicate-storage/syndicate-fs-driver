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
General iRODS Plugin
"""
import os
import logging
import threading
import sgfsdriver.lib.abstractfs as abstractfs
import sgfsdriver.plugins.irods.irods_client as irods_client

logger = logging.getLogger('syndicate_iRODS_filesystem')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('syndicate_iRODS_filesystem.log')
fh.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)


def reconnectAtIRODSFail(func):
    def wrap(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            logger.info("failed to process an operation : " + str(e))
            if self.irods:
                logger.info("reconnect: trying to reconnect to iRODS")
                self.irods.reconnect()
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

        user = secrets.get("user")
        user = user.encode('ascii', 'ignore')
        if not user:
            raise ValueError("user is not given correctly")

        password = secrets.get("password")
        password = password.encode('ascii', 'ignore')
        if not password:
            raise ValueError("password is not given correctly")

        irods_config = config.get("irods")
        if not irods_config:
            raise ValueError("irods configuration is not given correctly")

        # set role
        self._role = role

        # config can have unicode strings
        work_root = work_root.encode('ascii', 'ignore')
        self.work_root = work_root.rstrip("/")

        self.irods_config = irods_config

        # init irods client
        # we convert unicode (maybe) strings to ascii
        # since python-irodsclient cannot accept unicode strings
        irods_host = self.irods_config["host"]
        irods_host = irods_host.encode('ascii', 'ignore')
        irods_zone = self.irods_config["zone"]
        irods_zone = irods_zone.encode('ascii', 'ignore')

        logger.info("__init__: initializing irods_client")
        self.irods = irods_client.irods_client(
            host=irods_host,
            port=self.irods_config["port"],
            user=user,
            password=password,
            zone=irods_zone
        )

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

    def _make_irods_path(self, path):
        if path.startswith(self.work_root):
            if path == "/":
                return path
            else:
                return path.rstrip("/")

        if path.startswith("/"):
            return self.work_root + path.rstrip("/")

        return self.work_root + "/" + path.rstrip("/")

    def _make_driver_path(self, path):
        if path.startswith(self.work_root):
            return path[len(self.work_root):].rstrip("/")
        return path.rstrip("/")

    def connect(self):
        logger.info("connect: connecting to iRODS")

        self.irods.connect()

        if self._role == abstractfs.afsrole.DISCOVER:
            if not self.irods.exists(self.work_root):
                raise IOError("work_root does not exist")

    def close(self):
        logger.info("close")
        logger.info("close: closing iRODS")
        if self.irods:
            self.irods.close()

    @reconnectAtIRODSFail
    def stat(self, path):
        logger.info("stat - %s" % path)

        with self._get_lock():
            ascii_path = path.encode('ascii', 'ignore')
            irods_path = self._make_irods_path(ascii_path)
            driver_path = self._make_driver_path(ascii_path)
            # get stat
            sb = self.irods.stat(irods_path)
            if sb:
                return abstractfs.afsstat(
                    directory=sb.directory,
                    path=driver_path,
                    name=os.path.basename(driver_path),
                    size=sb.size,
                    checksum=sb.checksum,
                    create_time=sb.create_time,
                    modify_time=sb.modify_time
                )
            else:
                return None

    @reconnectAtIRODSFail
    def exists(self, path):
        logger.info("exists - %s" % path)

        with self._get_lock():
            ascii_path = path.encode('ascii', 'ignore')
            irods_path = self._make_irods_path(ascii_path)
            exist = self.irods.exists(irods_path)
            return exist

    @reconnectAtIRODSFail
    def list_dir(self, dirpath):
        logger.info("list_dir - %s" % dirpath)

        with self._get_lock():
            ascii_path = dirpath.encode('ascii', 'ignore')
            irods_path = self._make_irods_path(ascii_path)
            l = self.irods.list_dir(irods_path)
            return l

    @reconnectAtIRODSFail
    def is_dir(self, dirpath):
        logger.info("is_dir - %s" % dirpath)

        with self._get_lock():
            ascii_path = dirpath.encode('ascii', 'ignore')
            irods_path = self._make_irods_path(ascii_path)
            d = self.irods.is_dir(irods_path)
            return d

    @reconnectAtIRODSFail
    def make_dirs(self, dirpath):
        logger.info("make_dirs - %s" % dirpath)

        with self._get_lock():
            ascii_path = dirpath.encode('ascii', 'ignore')
            irods_path = self._make_irods_path(ascii_path)
            if not self.exists(irods_path):
                self.irods.make_dirs(irods_path)

    @reconnectAtIRODSFail
    def read(self, filepath, offset, size):
        logger.info("read - %s, %d, %d" % (filepath, offset, size))

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            irods_path = self._make_irods_path(ascii_path)
            buf = self.irods.read(irods_path, offset, size)
            return buf

    @reconnectAtIRODSFail
    def write(self, filepath, offset, buf):
        logger.info("write - %s, %d, %d" % (filepath, offset, len(buf)))

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            irods_path = self._make_irods_path(ascii_path)
            self.irods.write(irods_path, offset, buf)

    @reconnectAtIRODSFail
    def truncate(self, filepath, size):
        logger.info("truncate - %s, %d" % (filepath, size))

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            irods_path = self._make_irods_path(ascii_path)
            self.irods.truncate(irods_path, size)

    @reconnectAtIRODSFail
    def clear_cache(self, path):
        logger.info("clear_cache - %s" % path)

        with self._get_lock():
            if path:
                ascii_path = path.encode('ascii', 'ignore')
                irods_path = self._make_irods_path(ascii_path)
                self.irods.clear_stat_cache(irods_path)
            else:
                self.irods.clear_stat_cache(None)

    @reconnectAtIRODSFail
    def unlink(self, filepath):
        logger.info("unlink - %s" % filepath)

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            irods_path = self._make_irods_path(ascii_path)
            self.irods.unlink(irods_path)

    @reconnectAtIRODSFail
    def rename(self, filepath1, filepath2):
        logger.info("rename - %s to %s" % (filepath1, filepath2))

        with self._get_lock():
            ascii_path1 = filepath1.encode('ascii', 'ignore')
            ascii_path2 = filepath2.encode('ascii', 'ignore')
            irods_path1 = self._make_irods_path(ascii_path1)
            irods_path2 = self._make_irods_path(ascii_path2)
            self.irods.rename(irods_path1, irods_path2)

    @reconnectAtIRODSFail
    def set_xattr(self, filepath, key, value):
        logger.info("set_xattr - %s, %s=%s" % (filepath, key, value))

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            irods_path = self._make_irods_path(ascii_path)
            self.irods.set_xattr(irods_path, key, value)

    @reconnectAtIRODSFail
    def get_xattr(self, filepath, key):
        logger.info("get_xattr - %s, %s" % (filepath, key))

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            irods_path = self._make_irods_path(ascii_path)
            return self.irods.get_xattr(irods_path, key)

    @reconnectAtIRODSFail
    def list_xattr(self, filepath):
        logger.info("list_xattr - %s" % filepath)

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            localfs_path = self._make_irods_path(ascii_path)
            return self.irods.list_xattr(localfs_path)

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
            abstractfs.afsreplicationmode.BLOCK,
            abstractfs.afsreplicationmode.FILE
        ]
