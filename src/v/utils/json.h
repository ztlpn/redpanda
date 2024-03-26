/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "json/json.h"
#include "utils/fragmented_vector.h"

namespace json {

template<typename T, size_t max_fragment_size>
void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const fragmented_vector<T, max_fragment_size>& v) {
    w.StartArray();
    for (const auto& e : v) {
        rjson_serialize(w, e);
    }
    w.EndArray();
}

} // namespace json
