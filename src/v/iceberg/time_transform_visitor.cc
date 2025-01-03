/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/time_transform_visitor.h"

#include "iceberg/values.h"

#include <seastar/util/variant_utils.hh>

namespace iceberg {

namespace {

static constexpr int64_t micros_per_s = 1'000'000;

int32_t micros_to_hr(int64_t micros) {
    static constexpr int64_t s_per_hr = 3600;
    static constexpr int64_t micros_per_hr = micros_per_s * s_per_hr;
    return static_cast<int32_t>(micros / micros_per_hr);
}

int32_t micros_to_day(int64_t micros) {
    static constexpr int64_t s_per_day = 86400;
    static constexpr int64_t micros_per_day = micros_per_s * s_per_day;
    return static_cast<int32_t>(micros / micros_per_day);
}

} // namespace

int32_t hour_transform_visitor::operator()(const primitive_value& v) {
    if (std::holds_alternative<time_value>(v)) {
        return micros_to_hr(std::get<time_value>(v).val);
    }
    if (std::holds_alternative<timestamp_value>(v)) {
        return micros_to_hr(std::get<timestamp_value>(v).val);
    }
    if (std::holds_alternative<timestamptz_value>(v)) {
        return micros_to_hr(std::get<timestamptz_value>(v).val);
    }
    throw std::invalid_argument(
      fmt::format("hourly_visitor not implemented for primitive value {}", v));
}

int32_t day_transform_visitor::operator()(const primitive_value& v) {
    return ss::visit(
      v,
      [](const timestamp_value& v) { return micros_to_day(v.val); },
      [](const timestamptz_value& v) { return micros_to_day(v.val); },
      [](const auto& v) -> int32_t {
          throw std::invalid_argument(fmt::format(
            "day_transform_visitor not implemented for primitive value {}", v));
      });
}

} // namespace iceberg
