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
#include <syslog.h>

#include <map>
#include <memory>
#include <queue>
#include <string>

#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace rocksdb {

#define MAX_NR_QUEUE 128
#define THREAD_NUM 4
#define TORFS_MAX_BUF (8 * 1024 * 1024)
#define TORFS_ALIGNMENT 4096
#define MAX_IO_SIZE 65536u

enum TorfsDIO {
  DIO_READ = 0,
  DIO_WRITE,
};

extern void torfs_perr(const char* format, ...);

class TorfsConfig {
 public:
  uint32_t nsid;
  std::string dev;
  std::string dev_type;
  std::string async_io;
  std::string be;

  TorfsConfig() { nsid = 0; }
};

class DeviceGeometry {
 public:
  DeviceGeometry() = default;
  DeviceGeometry& operator=(const DeviceGeometry&) = default;

  explicit DeviceGeometry(uint64_t offset, uint32_t size, uint8_t* buf)
      : offset_{offset}, size_{size}, buf_{buf} {}

  uint64_t offset_;
  uint32_t size_;
  uint8_t* buf_;
  uint8_t dtype_ = 0;
  uint16_t dspec_ = 0;
  void EnableDirective(uint16_t dspec);
};

class Backend {
 public:
  Backend() {}
  virtual ~Backend() = default;
  int NvmeIO(const DeviceGeometry& geo, TorfsDIO dio);
  virtual uint64_t GetNlb() = 0;
  virtual uint32_t GetLbaShift() = 0;
  virtual int DeallocateImpl(const DeviceGeometry& geo) = 0;
  virtual void* AllocBuf(uint32_t size) = 0;
  virtual void FreeBuf(void* buf) = 0;
  virtual void SetDev(const std::string& name) = 0;
  virtual void SetAsyncType(const std::string& type) = 0;
  // for SPDK API
  virtual void FreeBuf(void* buf, uint32_t size) = 0;
  virtual int Init() = 0;
  virtual void Exit() = 0;

 protected:
  virtual int Async(const DeviceGeometry& geo, TorfsDIO dio) = 0;
};

class IOInterface {
 public:
  explicit IOInterface(const TorfsConfig* config);  // hqi:q

  virtual ~IOInterface();
  int Read(uint64_t offset, uint32_t size, void* buf);
  int Write(uint64_t offset, uint32_t size, const void* buf,
            uint16_t placementID);
  int Deallocate(uint64_t offset, uint32_t size);
  uint64_t GetNlba();
  uint64_t GetBlockSize();
  void* AllocBuf(uint32_t size);
  void FreeBuf(void* buf);
  void FreeBuf(void* buf, uint32_t size);
  std::shared_ptr<Backend> be_interface_{};

 private:
  DeviceGeometry PrepIO(uint64_t offset, uint32_t size, const void* buf);
  int DoIO(uint64_t offset, uint32_t size, const void* buf,
           uint16_t placementID, TorfsDIO dio);
};

class BackendFactory {
 public:
  BackendFactory() {}
  virtual ~BackendFactory() = default;
  virtual std::shared_ptr<Backend> CreateInterface(
      const TorfsConfig* config) = 0;
};

class SimpleFactory {
 public:
  SimpleFactory() {}
  ~SimpleFactory() {}
  std::shared_ptr<BackendFactory> CreateFactory(const std::string& be_type);
};

class XNvmeFactory : public BackendFactory {
 public:
  XNvmeFactory() {}
  ~XNvmeFactory() {}
  std::shared_ptr<Backend> CreateInterface(const TorfsConfig* config);
};

}  // namespace rocksdb
