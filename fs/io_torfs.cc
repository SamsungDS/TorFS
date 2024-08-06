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

#include <errno.h>
#include <linux/nvme_ioctl.h>
#include <sys/ioctl.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <iostream>
#include <numeric>

#include "io_torfs.h"
#include "fs_torfs.h"
#include "xnvme_be.h"

namespace rocksdb {

void torfs_perr(const char* format, ...) {
  va_list args;
  va_start(args, format);
  std::cerr << "ERROR ";
  vprintf(format, args);
  std::cerr << std::endl;
  va_end(args);
  fflush(stdout);
}

void DeviceGeometry::EnableDirective(uint16_t dspec) {
  dtype_ = 2;
  dspec_ = dspec;
}

std::shared_ptr<BackendFactory> SimpleFactory::CreateFactory(
    const std::string& be) {
  std::shared_ptr<BackendFactory> be_factory = nullptr;
  if (be == "xnvme") {
    be_factory = std::make_shared<XNvmeFactory>();
  } else if (be == "raw") {
    // TO DO
    return nullptr;
  }
  return be_factory;
}

std::shared_ptr<Backend> XNvmeFactory::CreateInterface(
    const TorfsConfig* config) {
  std::shared_ptr<Backend> be = nullptr;

  if (config->dev_type == "block") {
    be = std::make_shared<XNvmeBlock>();
  } else if (config->dev_type == "char") {
    be = std::make_shared<XNvmeChar>();
  } else if (config->dev_type == "spdk") {
    be = std::make_shared<XNvmeSpdk>();
  }
  be->SetDev(config->dev);
  be->SetAsyncType(config->async_io);
  return be;
}

IOInterface::IOInterface(const struct TorfsConfig* config) {
  std::shared_ptr<SimpleFactory> fac(new SimpleFactory());
  std::shared_ptr<BackendFactory> be_factory = fac->CreateFactory(config->be);
  if (!be_factory) {
    std::string err_msg = "Failed to configure the backend";
    throw(err_msg.c_str());
  }
  be_interface_ = be_factory->CreateInterface(config);
  try {
    if (be_interface_ != nullptr) {
      be_interface_->Init();
    } else {
      std::string err_msg = "Failed to configure the IO interfaces";
      throw(err_msg.c_str());
    }
  } catch (std::invalid_argument& e) {
    std::cout << e.what() << std::endl;
    be_interface_->Exit();
  }
}

IOInterface::~IOInterface() {}

DeviceGeometry IOInterface::PrepIO(uint64_t offset, uint32_t size,
                                   const void* buf) {
  const uint8_t* bufp_ = reinterpret_cast<const uint8_t*>(buf);
  DeviceGeometry geo{offset, size, const_cast<uint8_t*>(bufp_)};
  return geo;
}

int IOInterface::DoIO(uint64_t offset, uint32_t size, const void* buf,
                      uint16_t placementID, TorfsDIO dio) {
  uint32_t remaining_size_ = size;
  uint8_t* bufp_ = (uint8_t*)buf;
  int err;

  while (remaining_size_) {
    uint32_t io_size_ = std::min<size_t>(TORFS_MAX_BUF, remaining_size_);
    DeviceGeometry geo = PrepIO(offset, io_size_, bufp_);
    geo.EnableDirective(placementID);
    err = be_interface_->NvmeIO(geo, dio);
    if (err) {
      torfs_perr("Failed to send NVMe IO command. offset: %lu, size: %u",
                 offset, io_size_);
      break;
    }
    offset += io_size_;
    bufp_ += io_size_;
    remaining_size_ -= io_size_;
  }

  return err;
}

int IOInterface::Read(uint64_t offset, uint32_t size, void* buf) {
  return DoIO(offset, size, buf, 0, DIO_READ);
}

int IOInterface::Write(uint64_t offset, uint32_t size, const void* buf,
                       uint16_t placementID) {
  return DoIO(offset, size, buf, placementID, DIO_WRITE);
}

int IOInterface::Deallocate(uint64_t offset, uint32_t size) {
  DeviceGeometry geo = PrepIO(offset, size, nullptr);
  return be_interface_->DeallocateImpl(geo);
}

uint64_t IOInterface::GetNlba() { return be_interface_->GetNlb(); }

uint64_t IOInterface::GetBlockSize() {
  return 1ULL << be_interface_->GetLbaShift();
}

void* IOInterface::AllocBuf(uint32_t size) {
  return be_interface_->AllocBuf(size);
}

// for SPDK API
void IOInterface::FreeBuf(void* buf, uint32_t size) {
  return be_interface_->FreeBuf(buf, size);
}

void IOInterface::FreeBuf(void* buf) { return be_interface_->FreeBuf(buf); }

}  // namespace rocksdb
