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
S3 Plugin
"""
import os
import logging
import threading
import sgfsdriver.lib.abstractfs as abstractfs
import sgfsdriver.plugins.s3.s3_client as s3_client

logger = logging.getLogger('syndicate_s3_filesystem')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('syndicate_s3_filesystem.log')
fh.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)


def reconnectAtS3Fail(func):
    def wrap(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            logger.info("failed to process an operation : " + str(e))
            if self.s3:
                logger.info("reconnect: trying to reconnect to S3 server")
                self.s3.reconnect()
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

        aws_access_key_id = secrets.get("aws_access_key_id")
        aws_access_key_id = aws_access_key_id.encode('ascii', 'ignore')
        if not aws_access_key_id:
            raise ValueError("aws_access_key_id is not given correctly")

        aws_secret_access_key = secrets.get("aws_secret_access_key")
        aws_secret_access_key = aws_secret_access_key.encode('ascii', 'ignore')
        if not aws_secret_access_key:
            raise ValueError("aws_secret_access_key is not given correctly")

        s3_config = config.get("s3")
        if not s3_config:
            raise ValueError("S3 configuration is not given correctly")

        # set role
        self._role = role

        # config can have unicode strings
        work_root = work_root.encode('ascii', 'ignore')
        self.work_root = work_root.rstrip("/")

        self.s3_config = s3_config

        # init s3 client
        # we convert unicode (maybe) strings to ascii
        s3_bucket = self.s3_config["bucket"]
        s3_bucket = s3_bucket.encode('ascii', 'ignore')

        s3_region = self.s3_config["region"]
        s3_region = s3_region.encode('ascii', 'ignore')

        logger.info("__init__: initializing s3_client")
        self.s3 = s3_client.s3_client(
            bucket=s3_bucket,
            access_id=aws_access_key_id,
            access_key=aws_secret_access_key,
            s3_region=s3_region
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
                st = self.stat(driver_path)
                if st:
                    entry = abstractfs.afsevent(driver_path, st)
                    if operation == "create":
                        self.notification_cb([], [entry], [])
                    elif operation == "modify":
                        self.notification_cb([entry], [], [])

    def _make_s3_path(self, path):
        if path.startswith(self.work_root):
            return path.rstrip("/")

        if path.startswith("/"):
            return self.work_root.rstrip("/") + path.rstrip("/")

        return self.work_root.rstrip("/") + "/" + path.rstrip("/")

    def _make_driver_path(self, path):
        if path.startswith(self.work_root):
            return path[len(self.work_root):].rstrip("/")
        return path.rstrip("/")

    def connect(self):
        logger.info("connect: connecting to S3 server")

        self.s3.connect()

        if self._role == abstractfs.afsrole.DISCOVER:
            if not self.s3.exists(self.work_root):
                raise IOError("work_root does not exist")

    def close(self):
        logger.info("close")
        logger.info("close: closing S3 connection")
        if self.s3:
            self.s3.close()

    @reconnectAtS3Fail
    def stat(self, path):
        logger.info("stat - %s" % path)

        with self._get_lock():
            ascii_path = path.encode('ascii', 'ignore')
            s3_path = self._make_s3_path(ascii_path)
            driver_path = self._make_driver_path(ascii_path)
            # get stat
            sb = self.s3.stat(s3_path)
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

    @reconnectAtS3Fail
    def exists(self, path):
        logger.info("exists - %s" % path)

        with self._get_lock():
            ascii_path = path.encode('ascii', 'ignore')
            s3_path = self._make_s3_path(ascii_path)
            exist = self.s3.exists(s3_path)
            return exist

    @reconnectAtS3Fail
    def list_dir(self, dirpath):
        logger.info("list_dir - %s" % dirpath)

        with self._get_lock():
            ascii_path = dirpath.encode('ascii', 'ignore')
            s3_path = self._make_s3_path(ascii_path)
            l = self.s3.list_dir(s3_path)
            return l

    @reconnectAtS3Fail
    def is_dir(self, dirpath):
        logger.info("is_dir - %s" % dirpath)

        with self._get_lock():
            ascii_path = dirpath.encode('ascii', 'ignore')
            s3_path = self._make_s3_path(ascii_path)
            d = self.s3.is_dir(s3_path)
            return d

    @reconnectAtS3Fail
    def make_dirs(self, dirpath):
        logger.info("make_dirs - %s" % dirpath)

        with self._get_lock():
            ascii_path = dirpath.encode('ascii', 'ignore')
            s3_path = self._make_s3_path(ascii_path)
            if not self.exists(s3_path):
                self.s3.make_dirs(s3_path)

    @reconnectAtS3Fail
    def read(self, filepath, offset, size):
        logger.info("read - %s, %d, %d" % (filepath, offset, size))

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            s3_path = self._make_s3_path(ascii_path)
            buf = self.s3.read(s3_path, offset, size)
            return buf

    @reconnectAtS3Fail
    def write(self, filepath, offset, buf):
        logger.info("write - %s, %d, %d" % (filepath, offset, len(buf)))

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            s3_path = self._make_s3_path(ascii_path)
            self.s3.write(s3_path, offset, buf)

    @reconnectAtS3Fail
    def truncate(self, filepath, size):
        logger.info("truncate - %s, %d" % (filepath, size))

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            s3_path = self._make_s3_path(ascii_path)
            self.s3.truncate(s3_path, size)

    @reconnectAtS3Fail
    def clear_cache(self, path):
        logger.info("clear_cache - %s" % path)

        with self._get_lock():
            if path:
                ascii_path = path.encode('ascii', 'ignore')
                s3_path = self._make_s3_path(ascii_path)
                self.s3.clear_stat_cache(s3_path)
            else:
                self.s3.clear_stat_cache(None)

    @reconnectAtS3Fail
    def unlink(self, filepath):
        logger.info("unlink - %s" % filepath)

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            s3_path = self._make_s3_path(ascii_path)
            self.s3.unlink(s3_path)

    @reconnectAtS3Fail
    def rename(self, filepath1, filepath2):
        logger.info("rename - %s to %s" % (filepath1, filepath2))

        with self._get_lock():
            ascii_path1 = filepath1.encode('ascii', 'ignore')
            ascii_path2 = filepath2.encode('ascii', 'ignore')
            s3_path1 = self._make_s3_path(ascii_path1)
            s3_path2 = self._make_s3_path(ascii_path2)
            self.s3.rename(s3_path1, s3_path2)

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
        return [abstractfs.afsreplicationmode.BLOCK]
