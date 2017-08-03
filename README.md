# syndicate-fs-driver
Syndicate Gateway filesystem driver and plugins

Build
=====

To build, type:
```
sudo ./setup.py install
```

Modules installed will be located under `/usr/local/lib/python2.7/dist-packages/sgfsdriver`

Supported plugins
=================

Following plugins are supported currently:

| **Name** | **Backend Storage** | **AG Support** | **RG Support** |
| -------------| ----------- | ----------- | ----------- |
| `datastore` | Cyverse Datastore | O (Auto Re-sync at Dataset Updates) | O (File & Block Replication Mode) |
| `irods` | iRODS | O | O (File & Block Replication Mode) |
| `local` | Linux Local Filesystem | O (Auto Re-sync at Dataset Updates) | O (File & Block Replication Mode) |
| `ftp` | FTP Server | O | O (Block Replication Mode) |
| `s3` | Amazon S3 | O | O (Block Replication Mode) |
