// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "net/batched_output_stream.h"

#include "base/likely.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "ssx/semaphore.h"

#include <seastar/core/future.hh>
#include <seastar/core/scattered_message.hh>

#include <fmt/format.h>

namespace net {

static ss::logger log{"ololo"};

batched_output_stream::batched_output_stream(
  ss::output_stream<char> o, size_t cache)
  : _out(std::move(o))
  , _cache_size(cache)
  , _write_sem(std::make_unique<ssx::semaphore>(1, "net/batch-ostream")) {
    // Size zero reserved for identifying default-initialized
    // instances in stop()
    vassert(_cache_size > 0, "Size must be > 0");
}

[[gnu::cold]] static ss::future<bool>
already_closed_error(ss::scattered_message<char>& msg) {
    return ss::make_exception_future<bool>(
      batched_output_stream_closed(msg.size()));
}

using namespace std::chrono_literals;

ss::future<bool> batched_output_stream::write(ss::scattered_message<char> msg) {
    if (unlikely(_closed)) {
        return already_closed_error(msg);
    }
    vlog(log.info, "acq1");
    auto started = ss::steady_clock_type::now();
    return ss::with_semaphore(
      *_write_sem, 1, [this, v = std::move(msg), started]() mutable {
          vlog(
            log.info,
            "granted1 {}",
            (ss::steady_clock_type::now() - started) / 1ms);
          if (unlikely(_closed)) {
              return already_closed_error(v);
          }
          const size_t vbytes = v.size();
          return _out.write(std::move(v))
            .then([this, vbytes, started] {
                vlog(
                  log.info,
                  "write1 {}",
                  (ss::steady_clock_type::now() - started) / 1ms);
                _unflushed_bytes += vbytes;
                if (
                  _write_sem->waiters() == 0
                  || _unflushed_bytes >= _cache_size) {
                    return do_flush().then([] { return true; });
                }
                return ss::make_ready_future<bool>(false);
            })
            .finally([started] {
                vlog(
                  log.info,
                  "rel1 {}",
                  (ss::steady_clock_type::now() - started) / 1ms);
            });
      });
}

ss::future<> batched_output_stream::do_flush() {
    auto started = ss::steady_clock_type::now();
    if (_unflushed_bytes == 0) {
        return ss::make_ready_future<>();
    }
    _unflushed_bytes = 0;
    return _out.flush().then([started] {
        auto elapsed = ss::steady_clock_type::now() - started;
        if (elapsed / 1ms > 1000) {
            vlog(log.info, "FLUSHED in {} ms", elapsed / 1ms);
        }
    });
}
ss::future<> batched_output_stream::flush() {
    vlog(log.info, "acq2");
    auto started = ss::steady_clock_type::now();
    return ss::with_semaphore(*_write_sem, 1, [this, started] {
        vlog(
          log.info,
          "granted2 {}",
          (ss::steady_clock_type::now() - started) / 1ms);
        return do_flush().finally([started] {
            vlog(
              log.info,
              "rel2 {}",
              (ss::steady_clock_type::now() - started) / 1ms);
        });
    });
}
ss::future<> batched_output_stream::stop() {
    if (_closed) {
        return ss::make_ready_future<>();
    }
    _closed = true;

    if (_cache_size == 0) {
        // A default-initialized batched_output_stream has a default
        // initialized output_stream, which has a default initialized
        // data_sink, which has a null pimpl pointer, and will segfault if
        // any methods (including flush or close) are called on it.
        return ss::make_ready_future();
    }

    vlog(log.info, "acq3");
    auto started = ss::steady_clock_type::now();
    return ss::with_semaphore(*_write_sem, 1, [this, started] {
        vlog(
          log.info,
          "granted3 {}",
          (ss::steady_clock_type::now() - started) / 1ms);
        return do_flush()
          .finally([this] { return _out.close(); })
          .finally([started] {
              vlog(
                log.info,
                "rel3 {}",
                (ss::steady_clock_type::now() - started) / 1ms);
          });
    });
}

} // namespace net
