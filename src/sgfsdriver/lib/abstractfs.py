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

import json
import time

from abc import ABCMeta, abstractmethod
from datetime import datetime

"""
Abstraction of filesystem insterface
"""


class afsstat(object):
    """
    Abstract File Status
    """
    def __init__(self,
                 directory=False,
                 symlink=False,
                 path=None,
                 name=None,
                 size=0,
                 checksum=0,
                 create_time=0,
                 modify_time=0):
        self.directory = directory
        self.symlink = symlink
        self.path = path
        self.name = name
        self.size = size
        self.checksum = checksum
        if isinstance(create_time, datetime):
            self.create_time = time.mktime(create_time.timetuple())
        else:
            self.create_time = create_time

        if isinstance(modify_time, datetime):
            self.modify_time = time.mktime(modify_time.timetuple())
        else:
            self.modify_time = modify_time

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        rep_d = "F"
        if self.directory:
            rep_d = "D"

        rep_s = "-"
        if self.symlink:
            rep_s = "L"

        return "<afsstat %s%s %s %d %s>" % \
            (rep_d, rep_s, self.name, self.size, self.checksum)

    def toJson(self):
        return json.dumps(self.__dict__)

    @classmethod
    def fromJson(cls, json_str):
        json_dict = json.loads(json_str)
        return cls(**json_dict)


class afsevent(object):
    def __init__(self,
                 path=None,
                 stat=None):
        self.path = path
        self.stat = stat

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "<afsevent %s %s>" % (self.path, self.stat)


class afsrole(object):
    DISCOVER = 1
    READ = 2
    WRITE = 3


class afsgateway(object):
    AG = "AG"
    RG = "RG"


class afsreplicationmode(object):
    BLOCK = "BLOCK"
    FILE = "FILE"


class afsbase(object):
    __metaclass__ = ABCMeta

    # connect to remote system if necessary
    @abstractmethod
    def connect(self):
        pass

    # disconnect and finalize
    @abstractmethod
    def close(self):
        pass

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    # get stat of path (a file or a directory)
    @abstractmethod
    def stat(self, path):
        pass

    # check existence of path (a file or a directory) and return True/False
    @abstractmethod
    def exists(self, path):
        pass

    # list directory entries (files and sub-directories)
    # and return names of found items
    @abstractmethod
    def list_dir(self, dirpath):
        pass

    # check if given path is a directory and return True/False
    @abstractmethod
    def is_dir(self, dirpath):
        pass

    # make a directory hierarchy
    @abstractmethod
    def make_dirs(self, dirpath):
        pass

    # read bytes at given offset in given size from given path
    # and return byte[]
    @abstractmethod
    def read(self, filepath, offset, size):
        pass

    # write bytes to given path with bytes
    @abstractmethod
    def write(self, filepath, offset, buf):
        pass

    # truncate given path with size
    @abstractmethod
    def truncate(self, filepath, size):
        pass

    # remove given file
    @abstractmethod
    def unlink(self, filepath):
        pass

    # rename given file
    @abstractmethod
    def rename(self, filepath1, filepath2):
        pass

    # clear cache if exists
    @abstractmethod
    def clear_cache(self, path):
        pass

    # return a class of plugin
    @abstractmethod
    def plugin(self):
        pass

    # return a role of plugin
    @abstractmethod
    def role(self):
        pass

    # set a notification callback
    @abstractmethod
    def set_notification_cb(self, notification_cb):
        pass

    # check what gateways are suported with this plugin
    @abstractmethod
    def get_supported_gateways(self):
        pass

    @abstractmethod
    def get_supported_replication_mode(self):
        pass
