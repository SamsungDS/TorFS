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

#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <util/coding.h>

#include <atomic>
#include <iostream>

#include "fs_torfs.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "util/string_util.h"

namespace rocksdb {

IOStatus FDPSequentialFile::ReadOffset(uint64_t offset, size_t n, Slice* result,
                                       char* scratch, size_t* read_len) const {
  if (fdp_file_ == NULL || offset >= fdp_file_->size_) {
    *result = Slice(scratch, 0);
    return IOStatus::OK();
  }

  if (offset + n > fdp_file_->size_) {
    n = fdp_file_->size_ - offset;
  }

  size_t cache_len = 0;
  if (fdp_file_->cache_offset_ && fdp_file_->wcache_) {
    cache_len = fdp_file_->cache_offset_ - fdp_file_->wcache_;
  }

  size_t cache_pos = fdp_file_->size_ - cache_len;
  if (offset >= (uint64_t)cache_pos) {
    *read_len = n;
    *result = Slice(fdp_file_->wcache_ + offset - cache_pos, n);
    return IOStatus::OK();
  }

  std::vector<struct Segment> seg_list;
  {
    seg_list.assign(fdp_file_->file_segments_.begin(),
                    fdp_file_->file_segments_.end());
  }

  size_t piece_off = 0, left, size;
  int err;
  uint64_t off;
  unsigned nr_seg;
  off = 0;
  for (nr_seg = 0; nr_seg < seg_list.size(); nr_seg++) {
    struct Segment* seg = &seg_list.at(nr_seg);
    size = seg->size - seg->padding;
    if (off + size > offset) {
      piece_off = offset - off;
      break;
    }
    off += size;
  }

  if (nr_seg == seg_list.size()) {
    return IOStatus::IOError("Failed to lookup segments from file:" +
                             fdp_file_->name_);
  }

  left = n;
  uint32_t buf_size = left + 2 * TORFS_ALIGNMENT;
  char* rcache =
      reinterpret_cast<char*>(fs_tor_->io_interface_->AllocBuf(buf_size));
  if (!rcache) return IOStatus::IOError("Failed to alloc buf");
  size_t read_actual_sum = 0;
  size_t begin_off = piece_off % fs_tor_->block_size_;
  while (left) {
    struct Segment* seg = &seg_list.at(nr_seg);
    size_t actual_size = seg->size - seg->padding;
    actual_size =
        (actual_size - piece_off > left) ? left : actual_size - piece_off;
    off = seg->offset + piece_off;

    size_t align_off = off % fs_tor_->block_size_;
    size_t read_align_off = off - align_off;
    size_t tmp_size = actual_size + align_off;
    size_t read_align_size =
        tmp_size % fs_tor_->block_size_ == 0
            ? tmp_size
            : fs_tor_->block_size_ * (tmp_size / fs_tor_->block_size_ + 1);
    err = fs_tor_->io_interface_->Read(read_align_off, read_align_size,
                                       rcache + read_actual_sum);
    if (err) {
      fs_tor_->io_interface_->FreeBuf(rcache);
      return IOStatus::IOError("Failed to read data from file: %s",
                               fdp_file_->name_.c_str());
    }

    read_actual_sum += tmp_size;
    left -= actual_size;
    piece_off = 0;
    nr_seg++;
  }

  memcpy(scratch, rcache + begin_off, n);
  *read_len = n;
  *result = Slice(scratch, n);
  fs_tor_->io_interface_->FreeBuf(rcache);
  return IOStatus::OK();
}

IOStatus FDPSequentialFile::Read(size_t n, const IOOptions& /*options*/,
                                 Slice* result, char* scratch,
                                 IODebugContext* /*dbg*/) {
  size_t read_len = 0;
  IOStatus status = ReadOffset(read_off_, n, result, scratch, &read_len);

  if (status.ok()) read_off_ += read_len;

  return status;
}

IOStatus FDPSequentialFile::PositionedRead(uint64_t offset, size_t n,
                                           const IOOptions& /*options*/,
                                           Slice* result, char* scratch,
                                           IODebugContext* /*dbg*/) {
  size_t read_len = 0;
  (void)offset;
  IOStatus status = ReadOffset(read_off_, n, result, scratch, &read_len);

  *result = Slice(scratch, read_len);

  return IOStatus::OK();
  return status;
}

IOStatus FDPSequentialFile::Skip(uint64_t n) {
  if (read_off_ + n <= fdp_file_->size_) {
    read_off_ += n;
  }
  return IOStatus::OK();
}

IOStatus FDPSequentialFile::InvalidateCache(size_t offset, size_t length) {
  (void)offset;
  (void)length;
  return IOStatus::OK();
}

IOStatus FDPRandomAccessFile::ReadOffset(uint64_t offset, size_t n,
                                         Slice* result, char* scratch) const {
  if (fdp_file_ == NULL || offset >= fdp_file_->size_) {
    return IOStatus::OK();
  }

  if (offset + n > fdp_file_->size_) {
    n = fdp_file_->size_ - offset;
  }

  int err;
  size_t piece_off = 0;
  uint64_t off = 0;
  size_t i = 0;
  struct Segment* seg = NULL;
  for (; i < fdp_file_->file_segments_.size(); i++) {
    seg = &fdp_file_->file_segments_[i];
    size_t msize = seg->size - seg->padding;
    if (off + msize > offset) {
      piece_off = offset - off;
      break;
    }
    off += msize;
  }

  if (i == fdp_file_->file_segments_.size()) {
    return IOStatus::IOError("Failed to lookup data segments from file: %s",
                             fdp_file_->name_.c_str());
  }

  size_t left = n;
  uint32_t buf_size = left + 2 * TORFS_ALIGNMENT;
  char* rcache =
      reinterpret_cast<char*>(fs_tor_->io_interface_->AllocBuf(buf_size));

  size_t readAlignSum = 0;
  size_t begin_off = piece_off % fs_tor_->block_size_;
  while (left) {
    seg = &fdp_file_->file_segments_[i];
    size_t actual_size = seg->size - seg->padding;
    actual_size =
        (actual_size - piece_off > left) ? left : actual_size - piece_off;
    off = seg->offset + piece_off;

    size_t align_off = off % fs_tor_->block_size_;
    size_t read_align_off = off - align_off;
    size_t tmp_size = actual_size + align_off;
    size_t read_align_size =
        tmp_size % fs_tor_->block_size_ == 0
            ? tmp_size
            : fs_tor_->block_size_ * (tmp_size / fs_tor_->block_size_ + 1);

    err = fs_tor_->io_interface_->Read(read_align_off, read_align_size,
                                       rcache + readAlignSum);
    if (err) {
      fs_tor_->io_interface_->FreeBuf(rcache);
      return IOStatus::IOError("Failed to read data from file: %s",
                               fdp_file_->name_.c_str());
    }
    readAlignSum += tmp_size;
    left -= actual_size;
    piece_off = 0;
    i++;
  }
  memcpy(scratch, rcache + begin_off, n);
  *result = Slice(scratch, n);

  fs_tor_->io_interface_->FreeBuf(rcache);
  return IOStatus::OK();
}

IOStatus FDPRandomAccessFile::Read(uint64_t offset, size_t n,
                                   const IOOptions& /*options*/, Slice* result,
                                   char* scratch,
                                   IODebugContext* /*dbg*/) const {
  if (!fdp_file_ || offset >= fdp_file_->size_) {
    return IOStatus::OK();
  }

  if (offset + n > fdp_file_->size_) {
    n = fdp_file_->size_ - offset;
  }

  size_t cache_len = fdp_file_->cache_offset_ - fdp_file_->wcache_;
  size_t cache_pos = fdp_file_->size_ - cache_len;
  if (offset >= cache_pos) {
    *result = Slice(fdp_file_->wcache_ + offset - cache_pos, n);
    return IOStatus::OK();
  }

  return ReadOffset(offset, n, result, scratch);
}

IOStatus FDPRandomAccessFile::Prefetch(uint64_t offset, size_t n,
                                       const IOOptions& /*options*/,
                                       IODebugContext* /*dbg*/) {
  (void)offset;
  (void)n;
  if (fdp_file_ == NULL) {
    return IOStatus::OK();
  }

#if FDP_PREFETCH
  Slice result;
  IOStatus status;
  std::atomic_flag* flag = const_cast<std::atomic_flag*>(&prefetch_lock_);

  while (flag->test_and_set(std::memory_order_acquire)) {
  }
  if (n > FDP_PREFETCH_BUF_SZ) {
    n = FDP_PREFETCH_BUF_SZ;
  }

  status = ReadOffset(offset, n, &result, prefetch_);
  if (!status.ok()) {
    prefetch_sz_ = 0;
    flag->clear(std::memory_order_release);
    return status;
  }

  prefetch_sz_ = n;
  prefetch_off_ = offset;

  flag->clear(std::memory_order_release);
#endif

  return IOStatus::OK();
}

size_t FDPRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
  if (max_size < (kMaxVarint64Length * 3)) {
    return 0;
  }

  char* rid = id;
  uint64_t base = 0;

  if (!fdp_file_) {
    base = ((uint64_t)fs_tor_->files_[filename_]->uuididx_);
  } else {
    base = (uint64_t)this;
  }

  rid = EncodeVarint64(rid, (uint64_t)fdp_file_);
  rid = EncodeVarint64(rid, base);
  assert(rid >= id);

  return static_cast<size_t>(rid - id);
}

IOStatus FDPRandomAccessFile::InvalidateCache(size_t offset, size_t length) {
  (void)offset;
  (void)length;
  return IOStatus::OK();
}

/* ### WritableFile method implementation ### */
IOStatus FDPWritableFile::Append(
    const Slice& /*data*/, const IOOptions& /*opts*/,
    const DataVerificationInfo& /* verification_info */,
    IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus FDPWritableFile::Append(const Slice& data,
                                 const IOOptions& /*options*/,
                                 IODebugContext* /*dbg*/) {
  if (!fdp_file_->wcache_) {
    return IOStatus::NoSpace("Failed to allocate file buffer");
  }

  size_t size = data.size();
  size_t offset = 0;
  IOStatus s;

  if (!fdp_file_->cache_offset_) {
    return IOStatus::NoSpace("Failed to allocate buffer");
  }

  size_t cpytobuf_size = TORFS_MAX_BUF;
  if (fdp_file_->cache_offset_ + data.size() >
      fdp_file_->wcache_ + TORFS_MAX_BUF) {
    size = cpytobuf_size - (fdp_file_->cache_offset_ - fdp_file_->wcache_);
    memcpy(fdp_file_->cache_offset_, data.data(), size);
    fdp_file_->cache_offset_ += size;
    s = DataSync();
    if (!s.ok()) {
      return s;  // IOStatus::IOError("Failed to flush data");
    }

    offset = size;
    size = data.size() - size;
    while (size > cpytobuf_size) {
      memcpy(fdp_file_->cache_offset_, data.data() + offset, cpytobuf_size);
      offset = offset + cpytobuf_size;
      size = size - cpytobuf_size;
      fdp_file_->cache_offset_ += cpytobuf_size;
      s = DataSync();
      if (!s.ok()) {
        return s;  // IOStatus::IOError("Failed to flush data");
      }
    }
  }

  memcpy(fdp_file_->cache_offset_, data.data() + offset, size);
  fdp_file_->cache_offset_ += size;

  filesize_ += data.size();
  fdp_file_->size_ += data.size();

  return IOStatus::OK();
}

IOStatus FDPWritableFile::PositionedAppend(const Slice& data, uint64_t offset,
                                           const IOOptions& options,
                                           IODebugContext* dbg) {
  (void)offset;
  return Append(data, options, dbg);
}

IOStatus FDPWritableFile::Truncate(uint64_t size, const IOOptions& /*options*/,
                                   IODebugContext* /*dbg*/) {
  if (!fdp_file_) {
    return IOStatus::OK();
  }

  size_t cache_size =
      static_cast<size_t>(fdp_file_->cache_offset_ - fdp_file_->wcache_);
  size_t trun_size = fdp_file_->size_ - size;

  if (cache_size < trun_size) {
    return IOStatus::OK();
  }

  fdp_file_->cache_offset_ -= trun_size;
  filesize_ = size;
  fdp_file_->size_ = size;

  return IOStatus::OK();
}

IOStatus FDPWritableFile::Close(const IOOptions& /*options*/,
                                IODebugContext* /*dbg*/) {
  IOStatus s = DataSync();
  if (!s.ok()) return s;

  if (fdp_file_->wcache_) {
    fdp_file_->io_interface_->FreeBuf(fdp_file_->wcache_);
    fdp_file_->wcache_ = nullptr;
    fdp_file_->cache_offset_ = nullptr;
  }

  return IOStatus::OK();
}

IOStatus FDPWritableFile::Flush(const IOOptions& /*options*/,
                                IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus FDPWritableFile::Sync(const IOOptions& /*options*/,
                               IODebugContext* /*dbg*/) {
  return DataSync();
}

IOStatus FDPWritableFile::Fsync(const IOOptions& /*options*/,
                                IODebugContext* /*dbg*/) {
  return DataSync();
}

IOStatus FDPWritableFile::DataSync() {
  size_t size;

  if (!fdp_file_->cache_offset_) return IOStatus::OK();

  size = static_cast<size_t>(fdp_file_->cache_offset_ - fdp_file_->wcache_);
  if (!size) {
    return IOStatus::OK();
  }

  uint32_t padding = 0;
  size_t newSize = size;
  if (size % fs_tor_->block_size_ != 0) {
    newSize = fs_tor_->block_size_ * (size / fs_tor_->block_size_ + 1);
    padding = newSize - size;
  }

  std::vector<Segment> segs;
  IOStatus s = fs_tor_->AllocCap(newSize, segs);
  if (!s.ok()) return s;

  uint64_t tsize = 0;
  int err = true;
  for (size_t i = 0; i < segs.size(); i++) {
    err = fs_tor_->io_interface_->Write(segs[i].offset, segs[i].size,
                                        fdp_file_->wcache_ + tsize,
                                        fdp_file_->pid_);
    if (err) {
      return IOStatus::IOError("Failed to write data to the device");
    }

    if (i == segs.size() - 1) {
      segs[i].padding = padding;
    }

    fdp_file_->file_segments_.push_back(segs[i]);

    tsize += segs[i].size;
  }

  fs_tor_->file_mutex_.Lock();
  fs_tor_->FlushAppendMetaData(fdp_file_);
  fs_tor_->file_mutex_.Unlock();

  fdp_file_->cache_offset_ = fdp_file_->wcache_;
  return IOStatus::OK();
}

IOStatus FDPWritableFile::InvalidateCache(size_t offset, size_t length) {
  (void)offset;
  (void)length;
  return IOStatus::OK();
}

void FDPWritableFile::SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) {
  write_hint_ = hint;
  fdp_file_->level_ = hint - Env::WLTH_SHORT;
  fdp_file_->pid_ =
      fdp_file_->level_ >= MAX_PID ? (MAX_PID - 1) : fdp_file_->level_;
}

IOStatus FDPWritableFile::RangeSync(uint64_t offset, uint64_t nbytes,
                                    const IOOptions& /*options*/,
                                    IODebugContext* /*dbg*/) {
  (void)offset;
  (void)nbytes;
  return IOStatus::OK();
}

size_t FDPWritableFile::GetUniqueId(char* id, size_t max_size) const {
  if (max_size < (kMaxVarint64Length * 3)) {
    return 0;
  }

  char* rid = id;
  uint64_t base = 0;

  if (!fdp_file_) {
    base = (uint64_t)this;
  } else {
    base = ((uint64_t)fdp_file_->uuididx_);
  }

  rid = EncodeVarint64(rid, (uint64_t)base);
  rid = EncodeVarint64(rid, (uint64_t)base);
  rid = EncodeVarint64(rid, (uint64_t)base);
  assert(rid >= id);

  return static_cast<size_t>(rid - id);
}

}  // namespace rocksdb
