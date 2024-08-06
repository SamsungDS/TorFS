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
#include <liburing.h>

#include <memory>
#include <queue>
#include <string>

#include "io_torfs.h"

namespace rocksdb {

class XNvmeBackend : public Backend {
 public:
  XNvmeBackend() {}
  std::queue<struct xnvme_queue*> xnvme_queues_;
  ~XNvmeBackend() { Exit(); }
  std::string dev;
  std::string async_io;
  port::Mutex xnvme_mutex_;
  struct xnvme_dev* dev_ = nullptr;
  const struct xnvme_geo* geo_ = nullptr;
  struct xnvme_queue* queues_[MAX_NR_QUEUE];
  std::queue<void*> big_buf_queue_;
  port::Mutex big_buf_mutex_;
  void* AllocBuf(uint32_t size);
  void FreeBuf(void* buf);
  // for SPDK API
  void FreeBuf(void* buf, uint32_t size);
  int DeallocateImpl(const DeviceGeometry& geo);
  void SetDev(const std::string& name) { dev = name; }
  void SetAsyncType(const std::string& type) { async_io = type; }

 private:
  int Async(const DeviceGeometry& geo, TorfsDIO dio);
  virtual struct xnvme_opts GetOpt() = 0;
  int Init();
  void Exit();
  uint64_t GetNlb();
  uint32_t GetLbaShift();
  static void async_cb(struct xnvme_cmd_ctx* ctx, void* cb_arg);
  int SubmitXNvmeAsyncCmd(struct xnvme_queue* xqueue, const DeviceGeometry& geo,
                          TorfsDIO dio);
  int SubmitDeallocate(const DeviceGeometry& geo);
};

class XNvmeBlock : public XNvmeBackend {
 public:
  XNvmeBlock() {}
  ~XNvmeBlock() {}

 private:
  struct xnvme_opts GetOpt();
};

class XNvmeChar : public XNvmeBackend {
 public:
  XNvmeChar() {}
  ~XNvmeChar() {}

 private:
  struct xnvme_opts GetOpt();
};

class XNvmeSpdk : public XNvmeBackend {
 public:
  XNvmeSpdk() {}
  ~XNvmeSpdk() {}

 private:
  struct xnvme_opts GetOpt();
};

}  // namespace rocksdb
