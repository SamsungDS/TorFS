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
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "io_torfs.h"
#include "meta_torfs.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/object_registry.h"

#define FDP_DEBUG 0
#define FDP_DEBUG_R (FDP_DEBUG && 1)  /* Read */
#define FDP_DEBUG_W (FDP_DEBUG && 1)  /* Write and Sync */
#define FDP_DEBUG_AF (FDP_DEBUG && 1) /* Append and Flush */
#define FDP_META_DEBUG 0              /* MetaData Flush and Recover */
#define FDP_META_ENABLED 1            /* Enable MetaData 0:Close   1:Open */

#define FDP_PREFETCH 0
#define FDP_PREFETCH_BUF_SZ (1024 * 1024 * 1) /* 1MB */
#define MAX_PID 5
#define MAX_META_SEGMENTS 2
#define NR_DEALLOCATE_THREADS 8
#define FILE_METADATA_BUF_SIZE (1024 * 1024 * 1024)
#define SMALL_DEAL (64 * 1024)

#define FDP_RU_SIZE 13079937024ul
#define META_SEGMENT_CAPACITY (1 * 1024 * 1024 * 1024ul)

namespace rocksdb {

struct Segment {
  uint64_t offset;
  uint64_t size;
  uint32_t padding;

  Segment() {
    offset = 0;
    size = 0;
    padding = 0;
  }

  friend bool operator<(Segment s1, Segment s2) {
    return s1.offset > s2.offset;
  }
};

class FDPFile {
 public:
  std::string name_;
  size_t size_;
  uint64_t uuididx_;
  int level_;
  int pid_;
  std::vector<Segment> file_segments_;
  uint32_t cur_seg_idx_;
  std::shared_ptr<Logger> logger_;
  char* wcache_;
  char* cache_offset_;

  std::mutex file_mutex_;
  std::mutex writer_mtx_;
  std::atomic<int> readers_{0};
  std::shared_ptr<IOInterface> io_interface_;
  FDPFile(const std::string& fname, int lvl, int placementId,
          std::shared_ptr<IOInterface> IOInterface,
          std::shared_ptr<Logger> logger, bool enable_buf);
  ~FDPFile();
  uint32_t GetFileMetaLen();
  uint32_t WriteMetaToBuf(unsigned char* buf, bool append = false);
};

class TorFS : public FileSystemWrapper {
 public:
  port::Mutex file_mutex_;
  std::unordered_map<std::string, FDPFile*> files_;
  std::shared_ptr<Logger> logger_;
  uint64_t sequence_;
  std::string fs_path_;
  port::Mutex multi_instances_mutex_;
  bool env_enabled;
  uint64_t uuididx_;
  unsigned char* meta_buf_;
  port::Mutex seg_queue_mutex_[MAX_PID];
  std::queue<Segment> seg_queue_[MAX_PID];
  uint64_t seg_queue_used_cap_[MAX_PID];
  port::Mutex off_mutex_;
  uint64_t dev_offset_;
  std::priority_queue<Segment> all_seg_queue_;
  uint64_t avail_capacity_;
  uint64_t max_capacity_;
  uint64_t block_size_;
  MetaSegment meta_segments_[MAX_META_SEGMENTS];
  uint8_t cur_segment_;
  port::Mutex meta_mutex_;
  std::unique_ptr<std::thread> deallocate_worker_[NR_DEALLOCATE_THREADS] = {
      nullptr};
  std::priority_queue<Segment> deallocate_queue_;
  port::Mutex deallocate_mutex_;
  std::shared_ptr<IOInterface> io_interface_;

  explicit TorFS(std::shared_ptr<FileSystem> aux_fs, const std::string& option,
                 std::shared_ptr<Logger> logger);
  virtual ~TorFS();
  std::string ToAuxPath(std::string path);
  IOStatus FlushTotalMetaData();
  IOStatus FlushAppendMetaData(FDPFile* pfile);
  IOStatus FlushDelMetaData(const std::string& fileName);
  IOStatus FlushRenameMetaData(const std::string& srcName,
                               const std::string& destName);

  std::string FormatPathLexically(std::filesystem::path filepath);
  void RecoverFileFromBuf(unsigned char* buf, uint32_t& praseLen, bool replace);
  void SwitchMetaSegment(uint64_t start);
  IOStatus LoadMetaData();
  void SetString(std::string aux) { fs_path_ = aux; }
  void ClearMetaData();
  IOStatus NewSequentialFile(const std::string& fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override;
  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override;
  IOStatus NewWritableFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override;
  IOStatus DeleteFile(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override;
  IOStatus GetFileSize(const std::string& f, const IOOptions& options,
                       uint64_t* size, IODebugContext* dbg) override;
  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& options, uint64_t* mtime,
                                   IODebugContext* dbg) override;
  IOStatus RenameFile(const std::string& src, const std::string& dst,
                      const IOOptions& options, IODebugContext* dbg) override;
  IOStatus Truncate(const std::string& /*fname*/, size_t /*size*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("Truncate is not implemented in TorFS");
  }

  IOStatus LinkFile(const std::string& /* file*/, const std::string& /*link*/,
                    const IOOptions& /* options*/,
                    IODebugContext* /* dbg*/) override {
    return IOStatus::NotSupported("LinkFile is not implemented in TorFS");
  }

  static uint64_t gettid();
  IOStatus NewDirectory(const std::string& fname, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override;
  IOStatus FileExists(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override;
  IOStatus GetChildren(const std::string& path, const IOOptions& options,
                       std::vector<std::string>* result,
                       IODebugContext* dbg) override;
  IOStatus CreateDir(const std::string& fname, const IOOptions& options,
                     IODebugContext* dbg) override;
  IOStatus CreateDirIfMissing(const std::string& fname,
                              const IOOptions& options,
                              IODebugContext* dbg) override;
  IOStatus DeleteDir(const std::string& fname, const IOOptions& options,
                     IODebugContext* dbg) override;
  IOStatus LockFile(const std::string& fname, const IOOptions& options,
                    FileLock** lock, IODebugContext* dbg) override;
  IOStatus UnlockFile(FileLock* lock, const IOOptions& options,
                      IODebugContext* dbg) override;
  IOStatus IsDirectory(const std::string& path, const IOOptions& options,
                       bool* is_dir, IODebugContext* dbg) override;
  IOStatus NewLogger(const std::string& fname, const IOOptions& options,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* dbg) override;
  const char* Name() const override;
  IOStatus GetTestDirectory(const IOOptions& options, std::string* path,
                            IODebugContext* dbg) override;
  IOStatus GetAbsolutePath(const std::string& db_path, const IOOptions& options,
                           std::string* output_path,
                           IODebugContext* dbg) override;
  void ReclaimSeg(const Segment& seg);
  IOStatus AllocCap(uint64_t size, std::vector<Segment>& segs);
  void SubmitDeallocate(const Segment& seg);
  void UpdateSegments();

 private:
  bool run_deallocate_worker_ = false;
  void DeallocateWorker();
};

class FDPSequentialFile : public FSSequentialFile {
 private:
  std::string filename_;
  bool use_direct_io_;
  size_t logical_sector_size_;
  FDPFile* fdp_file_;
  TorFS* fs_tor_;
  uint64_t read_off_;

 public:
  FDPSequentialFile(const std::string& fname, TorFS* torFS,
                    const FileOptions& options);
  ~FDPSequentialFile();
  /* ### Implemented at env_fdp_io.cc ### */
  IOStatus ReadOffset(uint64_t offset, size_t n, Slice* result, char* scratch,
                      size_t* read_len) const;

  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override;

  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override;

  IOStatus Skip(uint64_t n) override;
  IOStatus InvalidateCache(size_t offset, size_t length) override;
  bool use_direct_io() const override;
  size_t GetRequiredBufferAlignment() const override;
};

class FDPRandomAccessFile : public FSRandomAccessFile {
 private:
  std::string filename_;
  bool use_direct_io_;
  size_t logical_sector_size_;
  uint64_t uuididx_;

  TorFS* fs_tor_;
  FDPFile* fdp_file_;
#if FDP_PREFETCH
  char* prefetch_;
  size_t prefetch_sz_;
  uint64_t prefetch_off_;
  std::atomic_flag prefetch_lock_ = ATOMIC_FLAG_INIT;
#endif

 public:
  FDPRandomAccessFile(const std::string& fname, TorFS* torFS,
                      const FileOptions& options)
      : filename_(fname),
        use_direct_io_(options.use_direct_reads),
        logical_sector_size_(torFS->block_size_),
        uuididx_(0),
        fs_tor_(torFS) {
#if FDP_PREFETCH
    prefetch_off_ = 0;
#endif
    fs_tor_->file_mutex_.Lock();
    fdp_file_ = fs_tor_->files_[filename_];
    fs_tor_->file_mutex_.Unlock();

#if FDP_PREFETCH
    prefetch_ = reinterpret_cast<char*>(zrocks_alloc(FDP_PREFETCH_BUF_SZ));
    if (!prefetch_) {
      std::cout << " FDP (alloc prefetch_) error." << std::endl;
      prefetch_ = nullptr;
    }
    prefetch_sz_ = 0;
#endif
  }

  virtual ~FDPRandomAccessFile() {}
  /* ### Implemented at env_fdp_io.cc ### */
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& /*options*/,
                Slice* result, char* scratch,
                IODebugContext* /*dbg*/) const override;

  IOStatus Prefetch(uint64_t /*offset*/, size_t /*n*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override;

  size_t GetUniqueId(char* id, size_t max_size) const override;
  IOStatus InvalidateCache(size_t offset, size_t length) override;
  virtual IOStatus ReadOffset(uint64_t offset, size_t n, Slice* result,
                              char* scratch) const;
  bool use_direct_io() const override;
  size_t GetRequiredBufferAlignment() const override;
};

class FDPWritableFile : public FSWritableFile {
 private:
  const std::string filename_;
  const bool use_direct_io_;
  uint64_t filesize_;
  FDPFile* fdp_file_;
  size_t logical_sector_size_;
  TorFS* fs_tor_;

 public:
  explicit FDPWritableFile(const std::string& fname, TorFS* torFS,
                           const FileOptions& options);

  ~FDPWritableFile();
  IOStatus Append(const Slice& data, const IOOptions& options,
                  IODebugContext* dbg) override;

  IOStatus Append(const Slice& data, const IOOptions& opts,
                  const DataVerificationInfo& /* verification_info */,
                  IODebugContext* dbg) override;

  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override;

  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& opts,
                            const DataVerificationInfo& /* verification_info */,
                            IODebugContext* dbg) override;

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus DataSync();
  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus RangeSync(uint64_t offset, uint64_t nbytes, const IOOptions& options,
                     IODebugContext* dbg) override;

  IOStatus Truncate(uint64_t size, const IOOptions& options,
                    IODebugContext* dbg) override;

  IOStatus InvalidateCache(size_t offset, size_t length) override;
  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override;
  size_t GetUniqueId(char* id, size_t max_size) const override;
  bool IsSyncThreadSafe() const override;
  bool use_direct_io() const override;
  size_t GetRequiredBufferAlignment() const override;
};

int ParseTorfsOption(std::string option, TorfsConfig* config);

IOStatus NewFDPFS(FileSystem** fs, const std::string& dev_name);

}  // namespace rocksdb
