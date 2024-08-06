/* TorFS: RocksDB Storage Backend for FDP SSDs
 *
 * Copyright 2024 Samsung Electronics
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

#pragma once
#include <iostream>
#include <memory>

#include "io_torfs.h"

enum META_OPS { Base = 0, Update, Replace, Delete };

namespace rocksdb {

#define FILE_NAME_LEN 128
#define METADATA_MAGIC 0x3D

struct SuperBlock {
  union {
    struct {
      uint8_t magic;
      uint32_t sequence;
      char aux_fs_path[256] = {0};
    } info;
    char addr[TORFS_ALIGNMENT];
  };
  SuperBlock() {
    info.magic = METADATA_MAGIC;
    info.sequence = 0;
  }
};

struct MetadataHead {
 public:
  uint32_t crc;
  uint32_t data_length;
  uint8_t tag;
  MetadataHead() {
    crc = 0;
    data_length = 0;
    tag = 0;
  }
};

struct FileMeta {
  int8_t level;
  uint64_t file_size;
  int32_t nr_segments;
  char file_name[FILE_NAME_LEN];

  FileMeta() {
    level = -1;
    file_size = 0;
    nr_segments = 0;
    memset(file_name, 0, sizeof(file_name));
  }
};

class MetaSegment {
 public:
  uint64_t start_;
  uint64_t wp_;
  uint64_t capacity_;
  std::shared_ptr<IOInterface> io_interface_;

  MetaSegment() {
    start_ = 0;
    wp_ = 0;
    capacity_ = 0;
    io_interface_ = nullptr;
  }

  void Reset() { wp_ = start_; }
  IOStatus Append(unsigned char* buf, uint64_t len);
};

};  // namespace rocksdb
