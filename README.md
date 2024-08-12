# TorFS: RocksDB Storage Backend for FDP SSDs

TorFS is a plugin that enables RocksDB to access FDP SSDs

# Getting started

## Build

Download RocksDB and TorFS:

```shell
git clone git@github.com:facebook/rocksdb.git
git clone git@github.com:SamsungDS/TorFS.git rocksdb/plugin/torfs
```

Install and enable TorFS in micro-bench tool of RocksDB

```shell
DEBUG_LEVEL=0 ROCKSDB_PLUGINS=torfs make -j8 db_bench
```



## Testing with db_bench

TorFS supports xNVMe back-end that provides unified interfaces for multiple IO paths.

|           | io_uring_cmd (character device) | io_uring (block device) | libaio (block device) | SPDK |
| :-------: | :-----------------------------: | :---------------------: | :-------------------: | :--: |
| **xNVMe** |                √                |            O            |           O           |  √   |
|  **raw**  |                X                |            X            |           X           |  X   |

- √: Support IO path and data directive			
- O: Support IO path only, not data directive
- X: Not support IO path and data directive

```bash
./db_bench --benchmarks=<IO pattern> --fs_uri=torfs:<backend>:<dev>?be=<IO path>
e.g.
./db_bench --benchmarks=fillseq --fs_uri=torfs:xnvme:/dev/ng0n1?be=io_uring_cmd
```

