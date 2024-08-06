$(info $(MAKEFLAGS))
#torfs_root := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

torfs_SOURCES = fs/fs_torfs.cc \
                fs/fs_torfs_io.cc \
                fs/meta_torfs.cc \
                fs/io_torfs.cc \
                fs/xnvme_be.cc

torfs_HEADERS = fs/fs_torfs.h \
                fs/io_torfs.h \
                fs/xnvme_be.h \
                fs/meta_torfs.h

torfs_LDFLAGS = -Wl,--whole-archive \
                -Wl,--no-as-needed \
                -Wl,--no-whole-archive \
                -Wl,--as-needed \
                -laio -luuid -lnuma -pthread -lcrypto -lxnvme -u torfs_filesystem_reg


