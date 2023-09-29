// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/mock_property.h"
#include "raft/recovery_scheduler.h"
#include "test_utils/test.h"

#include <seastar/core/manual_clock.hh>

using namespace std::chrono_literals;

using recovery_scheduler = raft::recovery_scheduler_ticker<ss::manual_clock>;

struct mock_consensus {
    model::ntp ntp;
    std::optional<model::node_id> leader_id;
    bool is_learner = false;
    std::optional<raft::follower_recovery_state> frs;

    static ss::lw_shared_ptr<mock_consensus> create(model::ntp ntp) {
        return ss::make_lw_shared<mock_consensus>(std::move(ntp));
    }

    explicit mock_consensus(model::ntp ntp)
      : ntp(std::move(ntp)) {}
};

struct mock_consensus_iface : raft::follower_recovery_state::consensus_iface {
    const model::ntp& ntp() const override { return c->ntp; };
    std::optional<model::node_id> leader_id() const override {
        return c->leader_id;
    };
    bool is_learner() const override { return c->is_learner; };

    explicit mock_consensus_iface(mock_consensus* c)
      : c(c) {}
    mock_consensus* c;
};

TEST_CORO(recovery_scheduler, basic) {
    auto max_active = config::mock_property<size_t>{1};
    auto period = config::mock_property<std::chrono::milliseconds>{100ms};

    auto scheduler = recovery_scheduler{max_active.bind(), period.bind()};

    auto c = mock_consensus::create(model::ntp{
      model::ns{"fff"}, model::topic{"topic"}, model::partition_id{0}});

    c->frs.emplace(
      scheduler,
      std::make_unique<mock_consensus_iface>(c.get()),
      model::offset{0},
      model::offset{100},
      false);

    ss::manual_clock::advance(100ms);
    ASSERT_EQ_CORO(2, 1 + 1);
}
