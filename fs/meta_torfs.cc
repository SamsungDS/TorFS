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

#include "meta_torfs.h"

namespace rocksdb {

IOStatus MetaSegment::Append(unsigned char* buf, uint64_t len) {
  if (wp_ + len > start_ + capacity_) {
    return IOStatus::NoSpace();
  }

  int err = io_interface_->Write(wp_, len, buf, 0);
  if (err) {
    return IOStatus::IOError("Failed to write metadata segment");
  }

  wp_ += len;
  return IOStatus::OK();
}

};  // namespace rocksdb
