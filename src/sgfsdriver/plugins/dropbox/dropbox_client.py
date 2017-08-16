#!/usr/bin/env python

"""
   Copyright 2016 The Trustees of University of Arizona, 2017 The Trustees of University of North Carolina at Chapel Hill

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
import tempfile
import dropbox

from expiringdict import ExpiringDict

logger = logging.getLogger('dropbox_client')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('dropbox_client.log')
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
Interface class to Dropbox
"""


def dropbox_dirname(path):
    parent = os.path.dirname(path)
    if parent == '/':
        return ''
    return parent


class dropbox_status(object):
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
    def fromFolder(cls, col):
        return dropbox_status(
            directory=True,
            path=col.path_display,
            name=col.name
        )

    @classmethod
    def fromFile(cls, obj):
        return dropbox_status(
            directory=False,
            path=obj.path_display,
            name=obj.name,
            size=obj.size,
            checksum=obj.content_hash,
            modify_time=obj.server_modified
        )

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        rep_d = "F"
        if self.directory:
            rep_d = "D"

        return "<dropbox_status %s %s %d %s>" % \
            (rep_d, self.name, self.size, self.checksum)


def download_file(dbx, path, f):
    _, res = dbx.files_download(path)
    f.write(res.content)
    f.flush()
    f.seek(0)


def upload_file(dbx, f, path):
    # https://stackoverflow.com/questions/37397966/dropbox-api-v2-upload-large-files-using-python
    file_size = os.fstat(f.fileno()).st_size
    CHUNK_SIZE = 4 * 1024 * 1024
    if file_size <= CHUNK_SIZE:
        dbx.files_upload(f.read(), path)
    else:
        upload_session_start_result = dbx.files_upload_session_start(
            f.read(CHUNK_SIZE)
        )
        cursor = dropbox.files.UploadSessionCursor(
            session_id=upload_session_start_result.session_id,
            offset=f.tell()
        )
        commit = dropbox.files.CommitInfo(path=path)

    while f.tell() < file_size:
        if ((file_size - f.tell()) <= CHUNK_SIZE):
            dbx.files_upload_session_finish(
                f.read(CHUNK_SIZE),
                cursor,
                commit
            )
        else:
            dbx.files_upload_session_append(
                f.read(CHUNK_SIZE),
                cursor.session_id,
                cursor.offset
            )
            cursor.offset = f.tell()


class dropbox_client(object):
    def __init__(self,
                 access_token=None):
        self.access_token = access_token
        self.dbx = None

        # init cache
        self.meta_cache = ExpiringDict(
            max_len=METADATA_CACHE_SIZE,
            max_age_seconds=METADATA_CACHE_TTL)

    def connect(self):
        self.dbx = dropbox.Dropbox(self.access_token)

    def close(self):
        self.dbx = None

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

        coll = self.dbx.files_list_folder(path)
        stats = []
        while True:
            for col in coll.entries:
                if type(col) is dropbox.files.FileMetadata:
                    stats.append(dropbox_status.fromFile(col))
                elif type(col) is dropbox.files.FolderMetadata:
                    stats.append(dropbox_status.fromFolder(col))
                else:
                    pass
            if not coll.has_more:
                break
            coll = self.dbx.files_list_folder_continue(coll.cursor)

        self.meta_cache[path] = stats
        return stats

    """
    Returns dropbox_status
    """
    def stat(self, path):
        try:
            # try bulk loading of stats
            logger.info("stat - 1")
            #parent = os.path.dirname(path)
            parent = dropbox_dirname(path)
            logger.info("stat - 2")
            stats = self._ensureDirEntryStatLoaded(parent)
            logger.info("stat - 3")
            if stats:
                logger.info("stat - 4")
                for sb in stats:
                    if sb.path == path:
                        logger.info("stat - 5")
                        return sb
            logger.info("stat - 6")
            return None
        except (dropbox.exceptions.ApiError):
            # fall if cannot access the parent dir
            try:
                # we only need to check the case if the path is a collection
                # because if it is a file, it's parent dir must be accessible
                # thus, _ensureDirEntryStatLoaded should succeed.
                logger.info("stat - 7")
                return dropbox_status.fromFolder(
                    self.dbx.files_get_metadata(path)
                )
            except (dropbox.exceptions.ApiError):
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
        logger.info("make_dirs: path %s" % path)
        if not self.exists(path):
            # make parent dir first
            logger.info("make_dirs - 1")
            #self.make_dirs(os.path.dirname(path))
            self.make_dirs(dropbox_dirname(path))
            logger.info("make_dirs - 2")
            self.dbx.files_create_folder(path)
            logger.info("make_dirs - 3")
            # invalidate stat cache
            #self.clear_stat_cache(os.path.dirname(path))
            self.clear_stat_cache(dropbox_dirname(path))
            logger.info("make_dirs - 4")

    def exists(self, path):
        if path == '':
            return True

        logger.info("exists - 1")
        sb = self.stat(path)
        logger.info("exists - 2")
        if sb:
            return True
        return False

    def clear_stat_cache(self, path=None):
        if(path):
            if path in self.meta_cache:
                # directory
                del self.meta_cache[path]
            else:
                # file
                #parent = os.path.dirname(path)
                parent = dropbox_dirname(path)
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
            _, res = self.dbx.files_download(path)
            data = res.content
            buf = data[offset:offset+size]
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
            with tempfile.TemporaryFile() as f:
                if self.exists(path):
                    logger.info("write: opening a file - %s" % path)
                    download_file(self.dbx, path, f)
                else:
                    logger.info("write: creating a file - %s" % path)

                if offset != 0:
                    logger.info("write: seeking at %d" % offset)
                    new_offset = f.seek(offset)
                    if new_offset != offset:
                        logger.error(
                            "write: offset mismatch - requested(%d), "
                            "but returned(%d)" %
                            (offset, new_offset)
                        )
                        raise Exception(
                            "write: offset mismatch - requested(%d), "
                            "but returned(%d)" %
                            (offset, new_offset)
                        )

                logger.info("write: writing buffer %d" % len(buf))
                f.write(buf)
                f.flush()
                f.seek(0)
                logger.info("write: writing done")
                upload_file(self.dbx, f, path)

        except Exception, e:
            logger.error("write: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        # invalidate stat cache
        self.clear_stat_cache(path)

    def truncate(self, path, size):
        logger.info("truncate : %s" % path)
        try:
            with tempfile.TemporaryFile() as f:
                if self.exists(path):
                    logger.info("truncate: opening a file - %s" % path)
                    download_file(self.dbx, path, f)
                else:
                    logger.info("truncate: creating a file - %s" % path)
                f.truncate(size)  # what should be done if size overflow
                f.flush()
                upload_file(self.dbx, f, path)
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
            self.dbx.files_delete(path)
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
            self.dbx.files_move(path1, path2)
            logger.info("rename: renaming done")

        except Exception, e:
            logger.error("rename: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        # invalidate stat cache
        self.clear_stat_cache(path1)
        self.clear_stat_cache(path2)
