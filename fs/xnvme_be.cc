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
#include <libxnvme.h>
#include <linux/nvme_ioctl.h>
#include <sys/ioctl.h>

#include <algorithm>
#include <cstring>
#include <iostream>
#include <numeric>
#include <queue>
#include <stdexcept>
#include <string>

#include "xnvme_be.h"
#include "io_torfs.h"
#include "port/port.h"

namespace rocksdb {

void* XNvmeBackend::AllocBuf(uint32_t size) {
  void* buf = xnvme_buf_alloc(dev_, size);
  return buf;
}

void XNvmeBackend::FreeBuf(void* buf) { xnvme_buf_free(dev_, buf); }

// for SPDK API
void XNvmeBackend::FreeBuf(void* buf, uint32_t size) {
  (void)size;
  xnvme_buf_free(dev_, buf);
}

int XNvmeBackend::DeallocateImpl(const DeviceGeometry& geo) {
  int err = SubmitDeallocate(geo);
  return (err == 0) ? 0 : err;
}

int XNvmeBackend::Async(const DeviceGeometry& geo, TorfsDIO dio) {
  struct xnvme_queue* xqueue = nullptr;
  xnvme_mutex_.Lock();
  if (!xnvme_queues_.empty()) {
    xqueue = xnvme_queues_.front();
    xnvme_queues_.pop();
  }
  xnvme_mutex_.Unlock();
  if (!xqueue) {
    torfs_perr("xNVMe queue is Empty. Error: %d", -errno);
    return -errno;
  }
  int err = SubmitXNvmeAsyncCmd(xqueue, geo, dio);
  xnvme_mutex_.Lock();
  xnvme_queues_.push(xqueue);
  xnvme_mutex_.Unlock();
  return (err >= 0) ? 0 : err;
}

int XNvmeBackend::Init() {
  int err = 0;
  struct xnvme_opts opts = GetOpt();
  dev_ = xnvme_dev_open(dev.c_str(), &opts);
  if (!dev_) {
    torfs_perr("Cannot open the device: %s", dev.c_str());
    return -1;
  }
  geo_ = xnvme_dev_get_geo(dev_);
  const unsigned int qdepth = MAX_NR_QUEUE;

  for (int i = 0; i < MAX_NR_QUEUE; i++) {
    queues_[i] = nullptr;
    err = xnvme_queue_init(dev_, qdepth, 0, &queues_[i]);
    if (!err) {
      xnvme_queues_.push(queues_[i]);
    } else {
      throw std::invalid_argument("Failed to initialize xNVMe");
    }
  }
  return err;
}

void XNvmeBackend::Exit() {
  int err;
  for (int i = 0; i < MAX_NR_QUEUE; i++) {
    if (queues_[i] == nullptr) {
      break;
    }
    err = xnvme_queue_term(queues_[i]);
    if (err) {
      torfs_perr("The [%d]th queue is failed", i);
    }
  }
  xnvme_dev_close(dev_);
}

uint64_t XNvmeBackend::GetNlb() { return geo_->nsect; }

uint32_t XNvmeBackend::GetLbaShift() { return geo_->ssw; }

void XNvmeBackend::async_cb(struct xnvme_cmd_ctx* ctx, void* cb_arg) {
  struct xnvme_queue* xqueue;
  xqueue = (struct xnvme_queue*)cb_arg;
  if (xnvme_cmd_ctx_cpl_status(ctx)) {
    xnvme_cmd_ctx_pr(ctx, XNVME_PR_DEF);
  }
  xnvme_queue_put_cmd_ctx(xqueue, ctx);
}

int XNvmeBackend::SubmitXNvmeAsyncCmd(struct xnvme_queue* xqueue,
                                      const DeviceGeometry& geo, TorfsDIO dio) {
  struct xnvme_cmd_ctx* xnvme_ctx;
  int err = 0;
  uint32_t remaining_size = geo.size_;
  uint64_t offset = geo.offset_;
  uint8_t* buf = geo.buf_;
  while (remaining_size) {
    uint32_t io_size_ = std::min<size_t>(MAX_IO_SIZE, remaining_size);
    xnvme_ctx = xnvme_queue_get_cmd_ctx(xqueue);
    if (xnvme_ctx == nullptr) {
      throw std::invalid_argument("xNVMe context is NULL");
    }
    xnvme_ctx->async.cb = async_cb;
    xnvme_ctx->async.cb_arg = reinterpret_cast<void*>(xqueue);
    xnvme_ctx->dev = dev_;

    int opcode = 0;
    if (dio == DIO_READ)
      opcode = XNVME_SPEC_NVM_OPC_READ;
    else
      opcode = XNVME_SPEC_NVM_OPC_WRITE;

    uint64_t sLba = offset >> geo_->ssw;
    uint32_t nLb = (io_size_ >> geo_->ssw) - 1;
    xnvme_prep_nvm(xnvme_ctx, opcode, xnvme_dev_get_nsid(dev_), sLba, nLb);

    xnvme_ctx->cmd.nvm.dtype = geo.dtype_;        // 02H
    xnvme_ctx->cmd.nvm.cdw13.dspec = geo.dspec_;  // place_id_
    err = xnvme_cmd_pass(xnvme_ctx, buf, io_size_, nullptr, 0);
    if (err) {
      torfs_perr("Failed to perform xNVMe IO command");
      return err;
    }
    offset += io_size_;
    buf += io_size_;
    remaining_size -= io_size_;
  }
  err = xnvme_queue_drain(xqueue);
  if (err < 0) torfs_perr("Failed to drain xNVMe queue");
  return err;
}

int XNvmeBackend::SubmitDeallocate(const DeviceGeometry& geo) {
  int err;
  uint32_t nsid;
  struct xnvme_spec_dsm_range* dsm_range;
  struct xnvme_cmd_ctx ctx = xnvme_cmd_ctx_from_dev(dev_);
  struct xnvme_ident const* ident = xnvme_dev_get_ident(dev_);
  if (ident->dtype != 1 && ident->dtype != 2)
    return 0;  // we skip non nvme devices
  nsid = xnvme_dev_get_nsid(dev_);
  dsm_range =
      (struct xnvme_spec_dsm_range*)xnvme_buf_alloc(dev_, sizeof(*dsm_range));
  if (!dsm_range) {
    err = -errno;
    torfs_perr("Failed to allocate xNVMe buffer for deallocate command");
    return err;
  }

  dsm_range->cattr = 0;
  dsm_range->slba = geo.offset_ >> geo_->ssw;
  dsm_range->llb = geo.size_ >> geo_->ssw;

  err = xnvme_nvm_dsm(&ctx, nsid, dsm_range, 0, true, false, false);
  if (err) {
    torfs_perr(
        "xnvme_nvm_dsm() [%d] dsm->cattr: [%u] dsm->slba: [%lu] dsm->llb: [%u] "
        "nsid: [%u]",
        err, dsm_range->cattr, dsm_range->slba, dsm_range->llb, nsid);
  }
  xnvme_buf_free(dev_, reinterpret_cast<void*>(dsm_range));
  return err;
}

struct xnvme_opts XNvmeBlock::GetOpt() {
  struct xnvme_opts opts = xnvme_opts_default();
  opts.async = async_io.c_str();
  opts.direct = 1;
  return opts;
}

struct xnvme_opts XNvmeChar::GetOpt() {
  struct xnvme_opts opts = xnvme_opts_default();
  opts.async = async_io.c_str();
  opts.direct = 1;
  return opts;
}

struct xnvme_opts XNvmeSpdk::GetOpt() {
  struct xnvme_opts opts = xnvme_opts_default();
  opts.be = "spdk";
  opts.async = async_io.c_str();
  opts.direct = 1;
  return opts;
}

int Backend::NvmeIO(const DeviceGeometry& geo, TorfsDIO dio) {
  return Async(geo, dio);
}

}  // namespace rocksdb
