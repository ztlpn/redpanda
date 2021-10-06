// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "config/broker_endpoint.h"
#include "config/convert.h"
#include "config/data_directory_path.h"
#include "config/property.h"
#include "config_store.h"

namespace config {

struct node_config final : public config_store {
public:
    property<data_directory_path> data_directory;
    property<model::node_id> node_id;
    property<std::optional<ss::sstring>> rack;

    // Internal RPC listener
    property<unresolved_address> rpc_server;
    property<tls_config> rpc_server_tls;

    // Kafka RPC listener
    one_or_many_property<model::broker_endpoint> kafka_api;
    one_or_many_property<endpoint_tls_config> kafka_api_tls;

    property<std::optional<ss::sstring>> cloud_storage_cache_directory;

    // build pidfile path: `<data_directory>/pid.lock`
    std::filesystem::path pidfile_path() const {
        return data_directory().path / "pid.lock";
    }

    const std::vector<model::broker_endpoint>& advertised_kafka_api() const {
        if (_advertised_kafka_api().empty()) {
            return kafka_api();
        }
        return _advertised_kafka_api();
    }

    const one_or_many_property<model::broker_endpoint>&
    advertised_kafka_api_property() {
        return _advertised_kafka_api;
    }

    unresolved_address advertised_rpc_api() const {
        return _advertised_rpc_api().value_or(rpc_server());
    }

    node_config() noexcept;
    void load(const YAML::Node& root_node);

private:
    property<std::optional<unresolved_address>> _advertised_rpc_api;
    one_or_many_property<model::broker_endpoint> _advertised_kafka_api;
};

node_config& node();

} // namespace config
