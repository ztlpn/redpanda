#include "cluster/types.h"
#include "storage/kvstore.h"
#include "storage/tests/storage_test_fixture.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/file.hh>

FIXTURE_TEST(kv_read, storage_test_fixture) {
    auto kv_config = storage::kvstore_config(
      8192,
      config::mock_binding(std::chrono::milliseconds(10)),
      "/home/ztlpn/v/incidents/2024-08-1506-arkose-kvstore-surgery/kvstore/var/lib/redpanda/data/",
      storage::make_sanitized_file_config());
    auto kvs = std::make_unique<storage::kvstore>(
      kv_config, 0, resources, feature_table);

    kvs->start().get();

    // kvs
    //   ->for_each(
    //     storage::kvstore::key_space::controller,
    //     [](bytes_view key, const iobuf& val) {
    //         iobuf key_buf;
    //         key_buf.append(key.data(), key.length());
    //         fmt::print("KEY {}\n", key_buf.hexdump(100));
    //     })
    //   .get();
    // return;

    const bytes node_uuid_key = "node_uuid";
    const bytes invariants_key = "configuration_invariants";

    auto uuid_val = (*kvs).get(
      storage::kvstore::key_space::controller, node_uuid_key);
    BOOST_REQUIRE(uuid_val);
    auto uuid = serde::from_iobuf<model::node_uuid>(std::move(*uuid_val));

    fmt::print("UUID {}\n", uuid);

    auto inv_val = (*kvs).get(
      storage::kvstore::key_space::controller, invariants_key);
    if (inv_val) {
        auto inv = reflection::from_iobuf<cluster::configuration_invariants>(
          std::move(*inv_val));

        fmt::print(
          "CONF_INV ni:{} cc:{} v:{}\n",
          inv.node_id,
          inv.core_count,
          inv.version);
    } else {
        fmt::print("NO CONF_INV\n");
    }

    kvs->stop().get();
    kvs.reset();
}

#if 0

FIXTURE_TEST(kv_override, storage_test_fixture) {
    auto kv_config = storage::kvstore_config(
      8192,
      config::mock_binding(std::chrono::milliseconds(10)),
      "/home/ztlpn/v/incidents/2024-08-1506-arkose-kvstore-surgery/kvstore/var/lib/redpanda/data/",
      storage::make_sanitized_file_config());
    auto kvs = std::make_unique<storage::kvstore>(
      kv_config, 0, resources, feature_table);

    kvs->start().get();

    static const bytes node_uuid_key = "node_uuid";
    const bytes invariants_key{"configuration_invariants"};

    // INFO  2023-08-14 07:18:22,628 [shard 0] cluster - members_manager.cc:744
    // - Processing node '20 ({6225c08d-fa8a-4e57-ace6-9cb8192c31c7})'
    // join request (version 7-9)
    //
    // $ python3 -c 'import uuid; print([int(b) for b in uuid.UUID("{4e149898-bf13-4830-b9b2-64c2c4ab052e}").bytes])'
    // [78, 20, 152, 152, 191, 19, 72, 48, 185, 178, 100, 194, 196, 171, 5, 46]

    model::node_uuid node_uuid(std::vector<uint8_t>{
      104, 188, 246, 179, 159, 16, 79, 142, 183, 2, 67, 124, 251, 31, 102, 138});

    BOOST_REQUIRE_EQUAL(
      fmt::format("{}", node_uuid), "68bcf6b3-9f10-4f8e-b702-437cfb1f668a");

    (*kvs)
      .put(
        storage::kvstore::key_space::controller,
        node_uuid_key,
        serde::to_iobuf(node_uuid))
      .get();

    cluster::configuration_invariants invariants(model::node_id(2), 1);

    auto invariants_buffer = reflection::to_iobuf(std::move(invariants));

    (*kvs)
      .put(
        storage::kvstore::key_space::controller,
        invariants_key,
        std::move(invariants_buffer))
      .get();

    kvs->stop().get();
    kvs.reset();
}
#endif
