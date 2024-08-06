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

#include "fs_torfs.h"

#include <sys/time.h>

#include <algorithm>
#include <iostream>
#include <memory>

#include "file/filename.h"
#include "meta_torfs.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

namespace rocksdb {

const char *async_str[] = {"thrpool",
                            "libaio",
                            "io_uring",
                            "io_uring_cmd",
                            "nvme"};
const std::string& TORFS_LOG = "/mnt/rocksdb_torfs.log";

FDPFile::FDPFile(const std::string& fname, int lvl, int placementId,
                 std::shared_ptr<IOInterface> io_interface,
                 std::shared_ptr<Logger> logger, bool enable_buf = true)
    : name_(fname),
      uuididx_(0),
      level_(lvl),
      pid_(placementId),
      logger_(logger) {
  size_ = 0;
  cur_seg_idx_ = 0;
  wcache_ = nullptr;
  cache_offset_ = nullptr;
  io_interface_ = io_interface;

  if (enable_buf) {
    wcache_ = reinterpret_cast<char*>(io_interface_->AllocBuf(TORFS_MAX_BUF));
    if (!wcache_) {
      Error(logger_, "Failed to allocate file buffer");
    }
    cache_offset_ = wcache_;
  }
}

FDPFile::~FDPFile() {
  if (wcache_) {
    io_interface_->FreeBuf(wcache_);
    wcache_ = nullptr;
    cache_offset_ = nullptr;
  }
  io_interface_.reset();
}

uint32_t FDPFile::GetFileMetaLen() {
  uint32_t meta_len = sizeof(FileMeta);
  meta_len += file_segments_.size() * sizeof(struct Segment);
  return meta_len;
}

uint32_t FDPFile::WriteMetaToBuf(unsigned char* buf, bool append) {
  // reserved single file head
  uint32_t length = sizeof(FileMeta);

  FileMeta fileMetaData;
  fileMetaData.file_size = size_;
  fileMetaData.level = level_;
  fileMetaData.nr_segments = file_segments_.size();
  uint32_t i = 0;
  if (append) {
    i = cur_seg_idx_;
    fileMetaData.nr_segments = file_segments_.size() - cur_seg_idx_;
  }

  memcpy(fileMetaData.file_name, name_.c_str(), name_.length());
  memcpy(buf, &fileMetaData, sizeof(FileMeta));
  for (; i < file_segments_.size(); i++) {
    memcpy(buf + length, &file_segments_[i], sizeof(struct Segment));
    length += sizeof(struct Segment);
  }

  cur_seg_idx_ = file_segments_.size();

  return length;
}

TorFS::TorFS(std::shared_ptr<FileSystem> aux_fs, const std::string& option,
             std::shared_ptr<Logger> logger)
    : FileSystemWrapper(aux_fs), logger_(logger) {
  env_enabled = false;
  uuididx_ = 0;
  sequence_ = 0;
  meta_buf_ = NULL;
  cur_segment_ = 0;

  Info(logger_, "Building TorFS Environment");
  std::string tor_opt = option;
  transform(tor_opt.begin(), tor_opt.end(), tor_opt.begin(), ::tolower);
  TorfsConfig* torfs_config = new TorfsConfig();

  int err = ParseTorfsOption(tor_opt, torfs_config);
  if (err) throw(err);

  io_interface_ = std::make_shared<IOInterface>(torfs_config);
  block_size_ = io_interface_->GetBlockSize();
  max_capacity_ = io_interface_->GetNlba() * block_size_;
  dev_offset_ = META_SEGMENT_CAPACITY * MAX_META_SEGMENTS;
  for (int seg = 0; seg < MAX_META_SEGMENTS; seg++) {
    meta_segments_[seg].start_ = seg * META_SEGMENT_CAPACITY;
    meta_segments_[seg].wp_ = seg * META_SEGMENT_CAPACITY;
    meta_segments_[seg].capacity_ = META_SEGMENT_CAPACITY;
    meta_segments_[seg].io_interface_ = io_interface_;
  }
  Info(logger_, "Max Capacity (Bytes): %lu, Block Size (Bytes): %lu",
       max_capacity_, block_size_);

  for (int i = 0; i < MAX_PID; i++) {
    seg_queue_used_cap_[i] = 0;
  }

  avail_capacity_ = max_capacity_ - dev_offset_;
  Segment first_seg;
  first_seg.offset = dev_offset_;
  first_seg.size = avail_capacity_;
  first_seg.padding = 0;
  all_seg_queue_.push(first_seg);
  run_deallocate_worker_ = true;
  for (int i = 0; i < NR_DEALLOCATE_THREADS; i++) {
    deallocate_worker_[i].reset(
        new std::thread(&TorFS::DeallocateWorker, this));
  }
}

TorFS::~TorFS() {
  if (FDP_META_ENABLED) {
    io_interface_->FreeBuf(meta_buf_);
  }

  run_deallocate_worker_ = false;
  for (int i = 0; i < NR_DEALLOCATE_THREADS; i++) {
    deallocate_worker_[i]->join();
  }

  for (auto& it : files_) {
    FDPFile* f = it.second;
    delete f;
    it.second = nullptr;
  }
  files_.clear();

  for (int seg = 0; seg < MAX_META_SEGMENTS; seg++) {
    meta_segments_[seg].io_interface_.reset();
  }
  io_interface_.reset();
  // TORFS_INFO("Destroying TorFS Environment");
  Info(logger_, "Destroying TorFS Environment");
}

std::string TorFS::ToAuxPath(std::string path) { return fs_path_ + path; }

std::string TorFS::FormatPathLexically(std::filesystem::path filepath) {
  std::filesystem::path path = "/" / filepath.lexically_normal();
  return path.string();
}

IOStatus TorFS::NewSequentialFile(const std::string& fname,
                                  const FileOptions& options,
                                  std::unique_ptr<FSSequentialFile>* result,
                                  IODebugContext* dbg) {
  std::string new_fname = NormalizePath(fname);
  if (files_[new_fname] == NULL) {
    return target_->NewSequentialFile(new_fname, options, result, dbg);
  }

  result->reset();
  FDPSequentialFile* f = new FDPSequentialFile(new_fname, this, options);
  result->reset(dynamic_cast<FSSequentialFile*>(f));

  return IOStatus::OK();
}

IOStatus TorFS::NewRandomAccessFile(const std::string& fname,
                                    const FileOptions& options,
                                    std::unique_ptr<FSRandomAccessFile>* result,
                                    IODebugContext* /*dbg     */) {
  std::string new_fname = NormalizePath(fname);
  FDPRandomAccessFile* f = new FDPRandomAccessFile(new_fname, this, options);
  result->reset(dynamic_cast<FSRandomAccessFile*>(f));

  return IOStatus::OK();
}

IOStatus TorFS::NewWritableFile(const std::string& fname,
                                const FileOptions& options,
                                std::unique_ptr<FSWritableFile>* result,
                                IODebugContext* /*dbg*/) {
  std::string new_fname = NormalizePath(fname);
  uint32_t file_num = 0;

  target_->NewWritableFile(new_fname, options, result, nullptr);

  file_mutex_.Lock();
  file_num = files_.count(new_fname);
  if (file_num != 0) {
    delete files_[new_fname];
    files_.erase(new_fname);
  }

  int pId = 0;
  if (fname.find("MANIFEST") != std::string::npos) {
    pId = 4;
  }

  files_[new_fname] = new FDPFile(new_fname, 0, pId, io_interface_, logger_);
  files_[new_fname]->uuididx_ = uuididx_++;

  FDPWritableFile* f = new FDPWritableFile(new_fname, this, options);
  result->reset(dynamic_cast<FSWritableFile*>(f));
  file_mutex_.Unlock();

  return IOStatus::OK();
}

IOStatus TorFS::DeleteFile(const std::string& fname, const IOOptions& options,
                           IODebugContext* dbg) {
  std::string new_fname = NormalizePath(fname);

  target_->DeleteFile(new_fname, options, dbg);
  FDPFile* fdpFile = nullptr;
  file_mutex_.Lock();
  if (files_.find(new_fname) == files_.end() || files_[new_fname] == NULL) {
    file_mutex_.Unlock();
    return IOStatus::OK();
  }

  fdpFile = files_[new_fname];
  files_.erase(new_fname);
  FlushDelMetaData(new_fname);
  file_mutex_.Unlock();

  for (size_t i = 0; i < fdpFile->file_segments_.size(); i++) {
    SubmitDeallocate(fdpFile->file_segments_[i]);
  }
  delete fdpFile;
  return IOStatus::OK();
}

IOStatus TorFS::GetFileSize(const std::string& fname,
                            const IOOptions& /*options*/, uint64_t* size,
                            IODebugContext* /*dbg*/) {
  std::string new_fname = NormalizePath(fname);

  file_mutex_.Lock();
  if (files_.find(new_fname) == files_.end() || files_[new_fname] == NULL) {
    file_mutex_.Unlock();
    return IOStatus::OK();
  }

  *size = files_[new_fname]->size_;
  file_mutex_.Unlock();

  return IOStatus::OK();
}

IOStatus TorFS::GetFileModificationTime(const std::string& fname,
                                        const IOOptions& /*options*/,
                                        uint64_t* file_mtime,
                                        IODebugContext* /*dbg*/) {
  std::string new_fname = NormalizePath(fname);
  *file_mtime = 0;

  return IOStatus::OK();
}

uint64_t TorFS::gettid() {
  assert(sizeof(pthread_t) <= sizeof(uint64_t));
  return (uint64_t)pthread_self();
}

IOStatus TorFS::NewDirectory(const std::string& fname, const IOOptions& io_opts,
                             std::unique_ptr<FSDirectory>* result,
                             IODebugContext* dbg) {
  return target_->NewDirectory(fname, io_opts, result, dbg);
}

IOStatus TorFS::RenameFile(const std::string& src, const std::string& dst,
                           const IOOptions& options, IODebugContext* dbg) {
  const std::string nsrc = NormalizePath(src);
  const std::string ntarget = NormalizePath(dst);

  target_->RenameFile(nsrc, ntarget, options, dbg);
  file_mutex_.Lock();
  if (files_.find(nsrc) == files_.end() || files_[nsrc] == NULL) {
    file_mutex_.Unlock();
    return IOStatus::OK();
  }
  if (files_.find(ntarget) != files_.end()) {
    FDPFile* fdpFile = files_[ntarget];
    delete fdpFile;
    files_.erase(ntarget);
  }
  FDPFile* fdpFile = files_[nsrc];
  fdpFile->name_ = ntarget;
  files_[ntarget] = fdpFile;
  files_.erase(nsrc);

  FlushRenameMetaData(nsrc, ntarget);
  file_mutex_.Unlock();

  return IOStatus::OK();
}

IOStatus TorFS::FileExists(const std::string& fname, const IOOptions& options,
                           IODebugContext* dbg) {
  std::string new_fname = NormalizePath(fname);
  file_mutex_.Lock();
  if (files_.find(new_fname) != files_.end() && files_[new_fname] != nullptr) {
    file_mutex_.Unlock();
    return IOStatus::OK();
  }

  file_mutex_.Unlock();
  return target_->FileExists(new_fname, options, dbg);
}

IOStatus TorFS::GetChildren(const std::string& path, const IOOptions& options,
                            std::vector<std::string>* result,
                            IODebugContext* dbg) {
  return target_->GetChildren(path, options, result, dbg);
}

IOStatus TorFS::CreateDir(const std::string& fname, const IOOptions& options,
                          IODebugContext* dbg) {
  return target_->CreateDir(fname, options, dbg);
}

IOStatus TorFS::CreateDirIfMissing(const std::string& fname,
                                   const IOOptions& options,
                                   IODebugContext* dbg) {
  return target_->CreateDirIfMissing(fname, options, dbg);
}

IOStatus TorFS::DeleteDir(const std::string& fname, const IOOptions& options,
                          IODebugContext* dbg) {
  return target_->DeleteDir(fname, options, dbg);
}

IOStatus TorFS::LockFile(const std::string& fname, const IOOptions& options,
                         FileLock** lock, IODebugContext* dbg) {
  return target_->LockFile(fname, options, lock, dbg);
}

IOStatus TorFS::UnlockFile(FileLock* lock, const IOOptions& options,
                           IODebugContext* dbg) {
  return target_->UnlockFile(lock, options, dbg);
}

IOStatus TorFS::IsDirectory(const std::string& path, const IOOptions& options,
                            bool* is_dir, IODebugContext* dbg) {
  return target_->IsDirectory(path, options, is_dir, dbg);
}
IOStatus TorFS::NewLogger(const std::string& fname, const IOOptions& options,
                          std::shared_ptr<Logger>* result,
                          IODebugContext* dbg) {
  return target_->NewLogger(fname, options, result, dbg);
}

const char* TorFS::Name() const {
  return "TorFS - The FDP-enabled RocksDB plugin";
}

IOStatus TorFS::GetTestDirectory(const IOOptions& /*opts*/, std::string* result,
                                 IODebugContext* /*dbg*/) {
  const char* env = getenv("TEST_TMPDIR");
  if (env && env[0] != '\0') {
    *result = env;
  } else {
    char buf[100];
    snprintf(buf, sizeof(buf), "/mnt/rocksdbtest-%d",
             static_cast<int>(geteuid()));
    *result = buf;
  }
  // Directory may already exist
  {
    IOOptions opts;
    return CreateDirIfMissing(*result, opts, nullptr);
  }
  return IOStatus::OK();
}

IOStatus TorFS::GetAbsolutePath(const std::string& db_path,
                                const IOOptions& options,
                                std::string* output_path, IODebugContext* dbg) {
  return target_->GetAbsolutePath(db_path, options, output_path, dbg);
}

IOStatus TorFS::FlushTotalMetaData() {
  if (!FDP_META_ENABLED) {
    return IOStatus::OK();
  }

  memset(meta_buf_, 0, FILE_METADATA_BUF_SIZE);
  // reserved head position
  uint32_t file_num = 0;

  SuperBlock metaHead;
  metaHead.info.sequence = sequence_++;
  strncpy(reinterpret_cast<char*>(metaHead.info.aux_fs_path), fs_path_.c_str(),
          sizeof(metaHead.info.aux_fs_path) - 1);
  std::string tmp = metaHead.info.aux_fs_path;
  memcpy(meta_buf_, &metaHead, sizeof(SuperBlock));
  int data_len = sizeof(SuperBlock) + sizeof(MetadataHead) + sizeof(file_num);
  std::unordered_map<std::string, FDPFile*>::iterator itermap = files_.begin();

  for (; itermap != files_.end(); ++itermap) {
    FDPFile* pfile = itermap->second;

    if (pfile == NULL) {
      continue;
    }

    file_num++;

    if (data_len + pfile->GetFileMetaLen() >= FILE_METADATA_BUF_SIZE) {
      Error(logger_, "Metadata is oversized buffer size");
      return IOStatus::IOError("Failed to flush metadata");
    }

    int length = pfile->WriteMetaToBuf(meta_buf_ + data_len);
    data_len += length;
  }

  memcpy(meta_buf_ + sizeof(struct SuperBlock) + sizeof(MetadataHead),
         &file_num, sizeof(file_num));

  // sector align
  if (data_len % block_size_ != 0) {
    data_len = (data_len / block_size_ + 1) * block_size_;
  }

  if (data_len > FILE_METADATA_BUF_SIZE) {
    Error(logger_, "Metadata is oversized buffer size");
    return IOStatus::IOError("Failed to flush metadata");
  }

  MetadataHead metadataHead;
  metadataHead.data_length =
      data_len - sizeof(MetadataHead) - sizeof(SuperBlock);
  metadataHead.tag = Base;

  memcpy(meta_buf_ + sizeof(SuperBlock), &metadataHead, sizeof(MetadataHead));
  IOStatus status = meta_segments_[cur_segment_].Append(meta_buf_, data_len);
  if (status.IsNoSpace()) {
    SwitchMetaSegment(meta_segments_[cur_segment_].start_);
    status = meta_segments_[cur_segment_].Append(meta_buf_, data_len);
  } else if (!status.ok()) {
    Error(logger_, "Failed to write metadata segment. Segment ID: %u",
          cur_segment_);
    return status;
  }

  return IOStatus::OK();
}

IOStatus TorFS::FlushAppendMetaData(FDPFile* pfile) {
  if (!FDP_META_ENABLED) {
    return IOStatus::OK();
  }

  if (pfile->cur_seg_idx_ == pfile->file_segments_.size()) {
    return IOStatus::OK();
  }
  int allocaSize = sizeof(MetadataHead) + sizeof(FileMeta) +
                   (pfile->file_segments_.size() - pfile->cur_seg_idx_) *
                       sizeof(struct Segment);
  allocaSize = (allocaSize / block_size_ + 1) * block_size_;

  unsigned char* buf = NULL;
  buf = reinterpret_cast<unsigned char*>(io_interface_->AllocBuf(allocaSize));
  if (buf == NULL) {
    Error(logger_, "Failed to allocate metadata buffer");
    return IOStatus::IOError();
  }

  memset(buf, 0, allocaSize);

  // reserved head position
  int data_len = sizeof(MetadataHead);

  int length = pfile->WriteMetaToBuf(buf + data_len, true);
  data_len += length;

  // sector align
  if (data_len % block_size_ != 0) {
    data_len = (data_len / block_size_ + 1) * block_size_;
  }

  MetadataHead metadataHead;
  metadataHead.tag = Update;
  metadataHead.data_length = data_len - sizeof(MetadataHead);

  memcpy(buf, &metadataHead, sizeof(MetadataHead));
  meta_mutex_.Lock();
  IOStatus status = meta_segments_[cur_segment_].Append(buf, data_len);
  if (status.IsNoSpace()) {
    SwitchMetaSegment(meta_segments_[cur_segment_].start_);
    FlushTotalMetaData();
  } else if (!status.ok()) {
    IOStatus::IOError("Failed to write metadata");
  }
  meta_mutex_.Unlock();

  io_interface_->FreeBuf(buf);
  return IOStatus::OK();
}

IOStatus TorFS::FlushDelMetaData(const std::string& fileName) {
  if (!FDP_META_ENABLED) {
    return IOStatus::OK();
  }

  unsigned char* buf =
      reinterpret_cast<unsigned char*>(io_interface_->AllocBuf(block_size_));
  if (buf == NULL) {
    return IOStatus::IOError();
  }

  memset(buf, 0, block_size_);
  // reserved head position
  int dataLen = sizeof(MetadataHead);

  memcpy(buf + dataLen, fileName.c_str(), fileName.length());
  dataLen += FILE_NAME_LEN;

  // sector align
  if (dataLen % block_size_ != 0) {
    dataLen = (dataLen / block_size_ + 1) * block_size_;
  }

  MetadataHead metadataHead;
  metadataHead.tag = Delete;
  metadataHead.data_length = dataLen - sizeof(MetadataHead);

  memcpy(buf, &metadataHead, sizeof(MetadataHead));
  meta_mutex_.Lock();
  IOStatus status = meta_segments_[cur_segment_].Append(buf, dataLen);
  if (status.IsNoSpace()) {
    SwitchMetaSegment(meta_segments_[cur_segment_].start_);
    FlushTotalMetaData();
  } else if (!status.ok()) {
    Error(logger_, "Failed to write metadata segment. Segment ID: %u",
          cur_segment_);
    io_interface_->FreeBuf(buf);
    meta_mutex_.Unlock();
    return status;
  }
  meta_mutex_.Unlock();

  io_interface_->FreeBuf(buf);
  return IOStatus::OK();
}

IOStatus TorFS::FlushRenameMetaData(const std::string& srcName,
                                    const std::string& destName) {
  if (!FDP_META_ENABLED) {
    return IOStatus::OK();
  }

  unsigned char* buf =
      reinterpret_cast<unsigned char*>(io_interface_->AllocBuf(block_size_));
  if (buf == nullptr) {
    Error(logger_, "Failed to allocate buffer");
    return IOStatus::IOError("Failed to flush file name of metadata.");
  }

  memset(buf, 0, block_size_);
  // reserved head position
  int dataLen = sizeof(MetadataHead);

  memcpy(buf + dataLen, srcName.c_str(), srcName.length());
  dataLen += FILE_NAME_LEN;

  memcpy(buf + dataLen, destName.c_str(), destName.length());
  dataLen += FILE_NAME_LEN;

  // sector align
  if (dataLen % block_size_ != 0) {
    dataLen = (dataLen / block_size_ + 1) * block_size_;
  }

  MetadataHead metadataHead;
  metadataHead.tag = Replace;
  metadataHead.data_length = dataLen - sizeof(MetadataHead);

  memcpy(buf, &metadataHead, sizeof(MetadataHead));
  meta_mutex_.Lock();
  IOStatus status = meta_segments_[cur_segment_].Append(buf, dataLen);
  if (status.IsNoSpace()) {
    SwitchMetaSegment(meta_segments_[cur_segment_].start_);
    FlushTotalMetaData();
  } else if (!status.ok()) {
    Error(logger_, "Failed to flush metadata of file name. Segment id [%u]",
          cur_segment_);
  }
  meta_mutex_.Unlock();

  io_interface_->FreeBuf(buf);
  if (!status.ok())
    return IOStatus::IOError("Failed to rename file name of metadata");
  return IOStatus::OK();
}

void TorFS::RecoverFileFromBuf(unsigned char* buf, uint32_t& praseLen,
                               bool replace) {
  praseLen = 0;
  FileMeta fileMetaData = *(reinterpret_cast<FileMeta*>(buf));

  FDPFile* fdpFile = files_[fileMetaData.file_name];
  if (fdpFile && replace) {
    fdpFile->file_segments_.clear();
  }

  if (fdpFile == NULL) {
    fdpFile = new FDPFile(fileMetaData.file_name, -1, 0, io_interface_, logger_,
                          false);
  }

  if (fdpFile == NULL) {
    return;
  }

  fdpFile->size_ = fileMetaData.file_size;
  fdpFile->level_ = fileMetaData.level;

  uint32_t len = sizeof(FileMeta);
  for (int32_t i = 0; i < fileMetaData.nr_segments; i++) {
    struct Segment p = *(reinterpret_cast<struct Segment*>(buf + len));
    fdpFile->file_segments_.push_back(p);
    len += sizeof(struct Segment);
  }

  file_mutex_.Lock();
  files_[fdpFile->name_] = fdpFile;
  file_mutex_.Unlock();

  praseLen = len;
}

void TorFS::ClearMetaData() {
  std::unordered_map<std::string, FDPFile*>::iterator iter;
  for (iter = files_.begin(); iter != files_.end(); ++iter) {
    delete iter->second;
  }
  files_.clear();
}

void TorFS::UpdateSegments() {
  std::priority_queue<Segment> seg_queue;
  std::unordered_map<std::string, FDPFile*>::iterator iter;
  for (iter = files_.begin(); iter != files_.end(); ++iter) {
    FDPFile* fdpFile = iter->second;
    if (fdpFile == nullptr) {
      continue;
    }

    for (uint32_t i = 0; i < fdpFile->file_segments_.size(); i++) {
      seg_queue.push(fdpFile->file_segments_[i]);
    }
  }

  if (all_seg_queue_.empty()) {
    Error(logger_, "The queue stores all segments is empty");
    return;
  }

  Segment baseSeg = all_seg_queue_.top();
  all_seg_queue_.pop();
  while (!seg_queue.empty()) {
    uint64_t base_seg_end = baseSeg.offset + baseSeg.size;
    const Segment& tSeg = seg_queue.top();
    uint64_t tSegEnd = tSeg.offset + tSeg.size;
    if (tSeg.offset >= base_seg_end || tSegEnd <= baseSeg.offset) {
      seg_queue.pop();
      continue;
    }

    if (baseSeg.offset >= tSeg.offset) {
      baseSeg.offset = tSegEnd;
      baseSeg.size = base_seg_end - baseSeg.offset;
    } else if (base_seg_end <= tSegEnd) {
      baseSeg.size -= base_seg_end - tSeg.offset;
    } else {
      Segment rSeg;
      rSeg.offset = baseSeg.offset;
      rSeg.size = tSeg.offset - baseSeg.offset;
      all_seg_queue_.push(rSeg);
      baseSeg.offset = tSegEnd;
      baseSeg.size -= tSegEnd - baseSeg.offset;
    }
    seg_queue.pop();
  }

  if (baseSeg.size > 0) {
    all_seg_queue_.push(baseSeg);
  }
}

IOStatus TorFS::LoadMetaData() {
  if (!FDP_META_ENABLED) {
    return IOStatus::OK();
  }
  meta_buf_ = reinterpret_cast<unsigned char*>(
      io_interface_->AllocBuf(FILE_METADATA_BUF_SIZE));
  if (meta_buf_ == NULL) {
    Error(logger_, "Failed to allocate metadata buffer");
    return IOStatus::IOError("Failed to load metadata");
  }
  memset(meta_buf_, 0, FILE_METADATA_BUF_SIZE);

  int err = 0;
  std::map<uint32_t, uint64_t> seqSlbaMap;
  std::map<uint32_t, std::string> seqPathMap;
  for (uint8_t i = 0; i < MAX_META_SEGMENTS; i++) {
    err = io_interface_->Read(meta_segments_[i].start_, sizeof(SuperBlock),
                              meta_buf_);
    if (err) {
      Error(logger_, "Failed to read superblock");
      return IOStatus::IOError("Failed to load metadata");
    }

    SuperBlock* sblock = reinterpret_cast<SuperBlock*>(meta_buf_);
    if (sblock->info.magic == METADATA_MAGIC) {
      seqSlbaMap[sblock->info.sequence] = meta_segments_[i].start_;
      seqPathMap[sblock->info.sequence] = sblock->info.aux_fs_path;
      if (sblock->info.sequence > sequence_) {
        sequence_ = sblock->info.sequence;
      }
    }
  }
  std::map<uint32_t, uint64_t>::reverse_iterator iter;
  for (iter = seqSlbaMap.rbegin(); iter != seqSlbaMap.rend(); ++iter) {
    ClearMetaData();
    uint64_t read_pos = iter->second + sizeof(SuperBlock);
    while (true) {
      uint32_t read_len = block_size_;
      uint32_t parse_len = 0;
      err = io_interface_->Read(read_pos, read_len, meta_buf_);
      if (err) {
        Error(logger_, "Failed to read metadata head");
        return IOStatus::IOError("Failed to load metadata");
      }

      MetadataHead* metadataHead = reinterpret_cast<MetadataHead*>(meta_buf_);
      if (metadataHead->data_length == 0) {
        SwitchMetaSegment(iter->second);
        if (fs_path_.empty()) {
          fs_path_ = seqPathMap[iter->first];
        }
        sequence_++;
        goto LOAD_END;
      }

      if (metadataHead->data_length + sizeof(MetadataHead) > read_len) {
        err = io_interface_->Read(
            read_pos + read_len,
            metadataHead->data_length + sizeof(MetadataHead) - read_len,
            meta_buf_ + read_len);
        if (err) {
          return IOStatus::IOError("Failed to read metadata head");
        }
      }

      parse_len += sizeof(MetadataHead);
      read_pos += (sizeof(MetadataHead) + metadataHead->data_length);
      switch (metadataHead->tag) {
        case Base: {
          uint32_t file_num =
              *reinterpret_cast<uint32_t*>(meta_buf_ + parse_len);
          parse_len += sizeof(file_num);
          for (uint32_t i = 0; i < file_num; i++) {
            uint32_t file_meta_len = 0;
            RecoverFileFromBuf(meta_buf_ + parse_len, file_meta_len, false);
            parse_len += file_meta_len;
          }
        } break;
        case Update: {
          uint32_t file_meta_len = 0;
          RecoverFileFromBuf(meta_buf_ + parse_len, file_meta_len, false);
        } break;
        case Replace: {
          std::string src_file_name =
              reinterpret_cast<char*>(meta_buf_) + parse_len;
          parse_len += FILE_NAME_LEN;
          std::string dst_file_name =
              reinterpret_cast<char*>(meta_buf_) + parse_len;
          if (files_.find(dst_file_name) != files_.end()) {
            FDPFile* fdpFile = files_[dst_file_name];
            delete fdpFile;
          }
          FDPFile* fdpFile = files_[src_file_name];
          files_.erase(src_file_name);
          if (fdpFile != nullptr) {
            fdpFile->name_ = dst_file_name;
          }
          files_[dst_file_name] = fdpFile;
        } break;
        case Delete: {
          std::string file_name =
              reinterpret_cast<char*>(meta_buf_) + parse_len;
          FDPFile* fdpFile = files_[file_name];
          delete fdpFile;
          files_.erase(file_name);
        } break;
        default:
          break;
      }
    }
  }

LOAD_END:
  UpdateSegments();

  IOStatus status;
  file_mutex_.Lock();
  status = FlushTotalMetaData();
  file_mutex_.Unlock();
  if (status != IOStatus::OK()) {
    return status;
  }
  return IOStatus::OK();
}

IOStatus TorFS::AllocCap(uint64_t size, std::vector<Segment>& segs) {
  off_mutex_.Lock();
  while (size > 0 && !all_seg_queue_.empty()) {
    Segment seg = all_seg_queue_.top();
    all_seg_queue_.pop();
    Segment tSeg = seg;
    tSeg.padding = 0;
    if (seg.size > size) {
      tSeg.size = size;

      seg.size -= size;
      seg.offset += size;
      avail_capacity_ -= size;
      all_seg_queue_.push(seg);
      size = 0;
    } else {
      size -= seg.size;
      avail_capacity_ -= seg.size;
    }

    int vsize = segs.size();
    if (vsize >= 1 &&
        segs[vsize - 1].offset + segs[vsize - 1].size == tSeg.offset) {
      segs[vsize - 1].size += tSeg.size;
    } else {
      segs.push_back(tSeg);
    }
  }

  off_mutex_.Unlock();

  if (size > 0) {
    Error(logger_, "Not enough segments for allocation");
    return IOStatus::NoSpace("Failed to allocate segment");
  }
  return IOStatus::OK();
}

void TorFS::ReclaimSeg(const Segment& seg) {
  off_mutex_.Lock();
  all_seg_queue_.push(seg);
  avail_capacity_ += seg.size;
  off_mutex_.Unlock();
}

void TorFS::SubmitDeallocate(const Segment& seg) {
  deallocate_mutex_.Lock();
  deallocate_queue_.push(seg);
  deallocate_mutex_.Unlock();
}

void TorFS::SwitchMetaSegment(uint64_t start) {
  for (int i = 0; i < MAX_META_SEGMENTS; i++) {
    if (meta_segments_[i].start_ != start) {
      cur_segment_ = i;
      break;
    }
  }

  meta_segments_[cur_segment_].Reset();
  int err = io_interface_->Deallocate(meta_segments_[cur_segment_].start_,
                                      meta_segments_[cur_segment_].capacity_);
  if (err) {
    Error(logger_, "Failed to deallocate metadata segment. Segment ID: %u",
          cur_segment_);
  }
}

void TorFS::DeallocateWorker() {
  while (run_deallocate_worker_) {
    usleep(250);
  REDO:
    deallocate_mutex_.Lock();
    if (deallocate_queue_.empty()) {
      deallocate_mutex_.Unlock();
      continue;
    }
    Segment seg = deallocate_queue_.top();
    deallocate_queue_.pop();
    while (!deallocate_queue_.empty()) {
      const Segment& tmp_seg = deallocate_queue_.top();
      if (seg.offset + seg.size != tmp_seg.offset) {
        break;
      }
      seg.size += tmp_seg.size;
      deallocate_queue_.pop();
    }
    deallocate_mutex_.Unlock();

    if (deallocate_queue_.size() <= 10000) {
      io_interface_->Deallocate(seg.offset, seg.size);
    } else if (deallocate_queue_.size() < 50000) {
      if (seg.size > SMALL_DEAL) {
        io_interface_->Deallocate(seg.offset, seg.size);
      }
    } else {
      if (seg.size > 1048576) {
        io_interface_->Deallocate(seg.offset, seg.size);
      }
    }

    io_interface_->Deallocate(seg.offset, seg.size);

    ReclaimSeg(seg);
    if (deallocate_queue_.size() > 1000) {
      goto REDO;
    }
  }
}

FDPSequentialFile::FDPSequentialFile(const std::string& fname, TorFS* torFS,
                                     const FileOptions& options)
    : filename_(fname),
      use_direct_io_(options.use_direct_reads),
      logical_sector_size_(torFS->block_size_) {
  fs_tor_ = torFS;
  read_off_ = 0;
  fdp_file_ = fs_tor_->files_[fname];
}

FDPSequentialFile::~FDPSequentialFile() {}

bool FDPSequentialFile::use_direct_io() const { return use_direct_io_; }
size_t FDPSequentialFile::GetRequiredBufferAlignment() const {
  return logical_sector_size_;
}

bool FDPRandomAccessFile::use_direct_io() const { return use_direct_io_; }
size_t FDPRandomAccessFile::GetRequiredBufferAlignment() const {
  return logical_sector_size_;
}

FDPWritableFile::FDPWritableFile(const std::string& fname, TorFS* torFS,
                                 const FileOptions& options)
    : FSWritableFile(options),
      filename_(fname),
      use_direct_io_(options.use_direct_writes),
      filesize_(0),
      logical_sector_size_(torFS->block_size_),
      fs_tor_(torFS) {
  fdp_file_ = fs_tor_->files_[fname];
}

FDPWritableFile::~FDPWritableFile() {}

IOStatus FDPWritableFile::PositionedAppend(
    const Slice& data, uint64_t offset, const IOOptions& opts,
    const DataVerificationInfo& /* verification_info */, IODebugContext* dbg) {
  return PositionedAppend(data, offset, opts, dbg);
}

bool FDPWritableFile::IsSyncThreadSafe() const { return true; }
bool FDPWritableFile::use_direct_io() const { return use_direct_io_; }
size_t FDPWritableFile::GetRequiredBufferAlignment() const {
  return logical_sector_size_;
}

bool _file_exist(std::string file_name) {
  std::ifstream f(file_name.c_str());
  if (!f.good()) {
    return false;
  }
  return true;
}

bool _create_file(std::string file_name) {
  std::ofstream f(file_name.c_str());
  if (!f.is_open()) return false;
  return true;
}

bool _async_exist(std::string async_io) {
  for (int i = 0; i < 5; i++) {
    if (strncmp(async_str[i], async_io.c_str(), async_io.size()) == 0)
      return true;
  }
  return false;
}

int ParseTorfsOption(std::string option, TorfsConfig* config) {
  // error code:
  //  -1: error option
  //  -2: error backend
  //  -3: error device
  //  -4: error async IO
  //  -5: error device path
  if (!config || option.find(':') == std::string::npos) {
    return -1;
  }
  int be_pos = option.find(':');
  config->be = option.substr(0, be_pos);
  if (config->be != "xnvme" && config->be != "raw") {
    torfs_perr("The backend: %s is not supported", config->be.c_str());
    return -2;
  }

  // dev type
  if (option.find("/nvme") != std::string::npos) {
    config->dev_type = "block";
  } else if (option.find("/ng") != std::string::npos) {
    config->dev_type = "char";
  } else if (option.find("pci") != std::string::npos) {
    config->dev_type = "spdk";
  } else {
    torfs_perr("The device type is not supported");
    return -3;
  }

  if (option.find('?') != std::string::npos) {
    if (option.find('=') == std::string::npos) {
      torfs_perr(
          "The option is not supported. e.g. "
          "torfs:<backend>:<device>?be=<async_io>");
      return -1;
    }
    int dev_pos = option.find('?');
    config->dev = option.substr(be_pos + 1, dev_pos - be_pos - 1);

    if (config->dev_type != "spdk") {
      if (!_file_exist(config->dev)) {
        torfs_perr("The device: %s does not exist", config->dev.c_str());
        return -5;
      }
    }
    int io_pos = option.find('=');
    if (config->dev_type == "spdk") {
      config->async_io = "nvme";
      config->nsid = static_cast<unsigned int>(
          std::stoi(option.substr(io_pos + 1, option.size() - io_pos)));
    } else {
      config->async_io = option.substr(io_pos + 1, option.size() - io_pos);
      if (!_async_exist(config->async_io)) {
        torfs_perr("The async IO: %s is not supported",
                   config->async_io.c_str());
        return -4;
      }
    }
  } else {  // default backend
    config->dev = option.substr(be_pos + 1, option.size() - be_pos);
    if (config->dev_type == "spdk") {
      config->nsid = 1;
      config->async_io = "nvme";
    } else {
      if (!_file_exist(config->dev)) {
        torfs_perr("The device: %s does not exist", config->dev.c_str());
        return -5;
      }
      config->async_io = "thrpool";
    }
  }
  return 0;
}

/* ### The factory method for creating a FDP Env ### */
IOStatus NewFDPFS(FileSystem** torfs_env, const std::string& dev_name) {
  try {
    std::shared_ptr<Logger> logger;
    Status s;
    std::string torfs_log_path = TORFS_LOG;
    if (!_file_exist(torfs_log_path)) {
      if (!_create_file(torfs_log_path))
        return IOStatus::Corruption("Failed to create TorFS log file");
    }

    s = Env::Default()->NewLogger(torfs_log_path.c_str(), &logger);
    if (!s.ok()) {
      return IOStatus::Corruption("Failed to create TorFS logger");
    } else {
      logger->SetInfoLogLevel(DEBUG_LEVEL);
    }

    TorFS* torFS = new TorFS(FileSystem::Default(), dev_name, logger);

    *torfs_env = torFS;

    torFS->multi_instances_mutex_.Lock();
    if (torFS->env_enabled) {
      torFS->multi_instances_mutex_.Unlock();
      return IOStatus::OK();
    }

    // Load metadata
    IOStatus status = torFS->LoadMetaData();
    if (!status.ok()) {
      torFS->multi_instances_mutex_.Unlock();
      Error(torFS->logger_, "Destroying TorFS Environment");
      return status;
    }
    torFS->multi_instances_mutex_.Unlock();
  } catch (int) {
    return IOStatus::NotSupported("Failed to parse TorFS option");
  }

  return IOStatus::OK();
}

#ifndef ROCKSDB_LITE
extern "C" FactoryFunc<FileSystem> torfs_filesystem_reg;

FactoryFunc<FileSystem> torfs_filesystem_reg =
#if (ROCKSDB_MAJOR < 6) || (ROCKSDB_MAJOR <= 6 && ROCKSDB_MINOR < 28)

    ObjectLibrary::Default()->Register<FileSystem>(
        "torfs:.*", [](const std::string& uri, std::unique_ptr<FileSystem>* f,
                       std::string* errmsg) {
#else

    ObjectLibrary::Default()->AddFactory<FileSystem>(
        ObjectLibrary::PatternEntry("torfs", false).AddSeparator(":"),
        [](const std::string& uri, std::unique_ptr<FileSystem>* f,
           std::string* errmsg) {
#endif
          std::string devID = uri;
          FileSystem* fs = nullptr;
          IOStatus s;

          devID.replace(0, strlen("torfs:"), "");
          s = NewFDPFS(&fs, devID);
          if (!s.ok()) {
            *errmsg = s.ToString();
          }
          f->reset(fs);
          return f->get();
        });
#endif
};  // namespace rocksdb
