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
Local-filesystem Plugin
"""
import os
import xattr
import stat
import logging
import threading
import pyinotify


import sgfsdriver.lib.abstractfs as abstractfs

logger = logging.getLogger('syndicate_local_filesystem')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('syndicate_local_filesystem.log')
fh.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)


class InotifyEventHandler(pyinotify.ProcessEvent):
    def __init__(self, plugin):
        self.plugin = plugin

    def process_IN_CREATE(self, event):
        logger.info("Creating: %s" % event.pathname)
        self.plugin.on_update_detected("create", event.pathname)

    def process_IN_DELETE(self, event):
        logger.info("Removing: %s" % event.pathname)
        self.plugin.on_update_detected("remove", event.pathname)

    def process_IN_MODIFY(self, event):
        logger.info("Modifying: %s" % event.pathname)
        self.plugin.on_update_detected("modify", event.pathname)

    def process_IN_ATTRIB(self, event):
        logger.info("Modifying attributes: %s" % event.pathname)
        self.plugin.on_update_detected("modify", event.pathname)

    def process_IN_MOVED_FROM(self, event):
        logger.info("Moving a file from : %s" % event.pathname)
        self.plugin.on_update_detected("remove", event.pathname)

    def process_IN_MOVED_TO(self, event):
        logger.info("Moving a file to : %s" % event.pathname)
        self.plugin.on_update_detected("create", event.pathname)

    def process_default(self, event):
        logger.info("Unhandled event to a file : %s" % event.pathname)
        logger.info("- %s" % event)


class plugin_impl(abstractfs.afsbase):
    def __init__(self, config, role=abstractfs.afsrole.DISCOVER):
        logger.info("__init__")

        if not config:
            raise ValueError("fs configuration is not given correctly")

        work_root = config.get("work_root")
        if not work_root:
            raise ValueError("work_root configuration is not given correctly")

        # set role
        self._role = role

        # config can have unicode strings
        work_root = work_root.encode('ascii', 'ignore')
        self.work_root = work_root.rstrip("/")

        if self._role == abstractfs.afsrole.DISCOVER:
            # set inotify
            self.watch_manager = pyinotify.WatchManager()
            self.notify_handler = InotifyEventHandler(self)
            self.notifier = pyinotify.ThreadedNotifier(
                self.watch_manager,
                self.notify_handler
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
                entry = abstractfs.afsevent(driver_path, st)
                if operation == "create":
                    self.notification_cb([], [entry], [])
                elif operation == "modify":
                    self.notification_cb([entry], [], [])

    def _make_localfs_path(self, path):
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
        logger.info("connect")

        if self._role == abstractfs.afsrole.DISCOVER:
            if not os.path.exists(self.work_root):
                raise IOError("work_root does not exist")

            try:
                # start monitoring
                self.notifier.start()

                mask = (
                    pyinotify.IN_DELETE | pyinotify.IN_CREATE |
                    pyinotify.IN_MODIFY | pyinotify.IN_ATTRIB |
                    pyinotify.IN_MOVED_FROM | pyinotify.IN_MOVED_TO |
                    pyinotify.IN_MOVE_SELF
                )
                self.watch_directory = self.watch_manager.add_watch(
                    self.work_root,
                    mask,
                    rec=True,
                    auto_add=True
                )
            except:
                self.close()

    def close(self):
        logger.info("close")

        if self._role == abstractfs.afsrole.DISCOVER:
            if self.watch_manager and self.watch_directory:
                self.watch_manager.rm_watch(self.watch_directory.values())

            if self.notifier:
                self.notifier.stop()

    def stat(self, path):
        logger.info("stat - %s" % path)

        with self._get_lock():
            ascii_path = path.encode('ascii', 'ignore')
            localfs_path = self._make_localfs_path(ascii_path)
            driver_path = self._make_driver_path(ascii_path)
            # get stat
            sb = os.stat(localfs_path)
            return abstractfs.afsstat(
                directory=stat.S_ISDIR(sb.st_mode),
                symlink=stat.S_ISLNK(sb.st_mode),
                path=driver_path,
                name=os.path.basename(driver_path),
                size=sb.st_size,
                checksum=0,
                create_time=sb.st_ctime,
                modify_time=sb.st_mtime
            )

    def exists(self, path):
        logger.info("exists - %s" % path)

        with self._get_lock():
            ascii_path = path.encode('ascii', 'ignore')
            localfs_path = self._make_localfs_path(ascii_path)
            exist = os.path.exists(localfs_path)
            return exist

    def list_dir(self, dirpath):
        logger.info("list_dir - %s" % dirpath)

        with self._get_lock():
            ascii_path = dirpath.encode('ascii', 'ignore')
            localfs_path = self._make_localfs_path(ascii_path)
            l = os.listdir(localfs_path)
            return l

    def is_dir(self, dirpath):
        logger.info("is_dir - %s" % dirpath)

        with self._get_lock():
            ascii_path = dirpath.encode('ascii', 'ignore')
            localfs_path = self._make_localfs_path(ascii_path)
            d = False
            if os.path.exists(localfs_path):
                sb = os.stat(localfs_path)
                d = stat.S_ISDIR(sb.st_mode)
            return d

    def make_dirs(self, dirpath):
        logger.info("make_dirs - %s" % dirpath)

        with self._get_lock():
            ascii_path = dirpath.encode('ascii', 'ignore')
            localfs_path = self._make_localfs_path(ascii_path)
            if not os.path.exists(localfs_path):
                os.makedirs(localfs_path)

    def read(self, filepath, offset, size):
        logger.info("read - %s, %d, %d" % (filepath, offset, size))

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            localfs_path = self._make_localfs_path(ascii_path)
            with open(localfs_path, "r") as f:
                f.seek(offset, 0)
                buf = f.read(size)
            return buf

    def write(self, filepath, offset, buf):
        logger.info("write - %s, %d, %d" % (filepath, offset, len(buf)))

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            localfs_path = self._make_localfs_path(ascii_path)
            fd = os.open(localfs_path, os.O_WRONLY | os.O_CREAT)
            os.lseek(fd, offset, os.SEEK_SET)
            os.write(fd, buf)
            os.close(fd)

    def truncate(self, filepath, size):
        logger.info("truncate - %s, %d" % (filepath, size))

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            localfs_path = self._make_localfs_path(ascii_path)
            fd = os.open(localfs_path, os.O_WRONLY | os.O_CREAT)
            os.ftruncate(fd, size)
            os.close(fd)

    def clear_cache(self, path):
        logger.info("clear_cache - %s" % path)

    def unlink(self, filepath):
        logger.info("unlink - %s" % filepath)

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            localfs_path = self._make_localfs_path(ascii_path)
            os.unlink(localfs_path)

    def rename(self, filepath1, filepath2):
        logger.info("rename - %s to %s" % (filepath1, filepath2))

        with self._get_lock():
            ascii_path1 = filepath1.encode('ascii', 'ignore')
            ascii_path2 = filepath2.encode('ascii', 'ignore')
            localfs_path1 = self._make_localfs_path(ascii_path1)
            localfs_path2 = self._make_localfs_path(ascii_path2)
            os.rename(localfs_path1, localfs_path2)

    def set_xattr(self, filepath, key, value):
        logger.info("set_xattr - %s, %s=%s" % (filepath, key, value))

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            localfs_path = self._make_localfs_path(ascii_path)
            xattr.setxattr(localfs_path, key, value)

    def get_xattr(self, filepath, key):
        logger.info("get_xattr - %s, %s" % (filepath, key))

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            localfs_path = self._make_localfs_path(ascii_path)
            return xattr.getxattr(localfs_path, key)

    def list_xattr(self, filepath):
        logger.info("list_xattr - %s" % filepath)

        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            localfs_path = self._make_localfs_path(ascii_path)
            return xattr.listxattr(localfs_path)

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
