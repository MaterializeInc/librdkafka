/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2025, Confluent Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "test.h"

static rd_bool_t brokers_add_all_brokers_down_received = rd_false;

/**
 * @brief Error callback for the brokers_add test.
 *        It expects to receive an error indicating that all brokers are down
 *        or a transport error.
 */
static void brokers_add_all_brokers_down_received_error_cb(rd_kafka_t *rk,
                                                           int error,
                                                           const char *reason,
                                                           void *opaque) {
        TEST_SAY("Error callback received %d: %s\n", error, reason);
        TEST_ASSERT(error == RD_KAFKA_RESP_ERR__TRANSPORT ||
                    error == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN);
        if (error == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) {
                brokers_add_all_brokers_down_received = rd_true;
        }
}

/**
 * @brief Verify the case where the client is re-bootstrapped after brokers were
 * added manually.
 */
static void do_test_rebootstrap_brokers_add(rd_kafka_type_t rk_type,
                                            char *bootstrap_servers) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        brokers_add_all_brokers_down_received = rd_false;

        SUB_TEST_QUICK("%s",
                       rk_type == RD_KAFKA_PRODUCER ? "producer" : "consumer");
        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstrap_servers);
        rd_kafka_conf_set_error_cb(
            conf, brokers_add_all_brokers_down_received_error_cb);
        rk = test_create_handle(rk_type, conf);
        rd_kafka_brokers_add(rk, "localhost:9999");

        /* Await a ALL_BROKERS_DOWN error*/
        while (!brokers_add_all_brokers_down_received) {
                rd_kafka_poll(rk, 1000);
        }
        brokers_add_all_brokers_down_received = rd_false;
        rd_kafka_destroy(rk);
        SUB_TEST_PASS();
}

/** Number of re-bootstrap sequences started, counted from the logs. */
static rd_atomic32_t backoff_kept_rebootstrap_sequence_cnt;

/** Number of bootstrap broker objects, re-added by a re-bootstrap sequence,
 *  that inherited the reconnect backoff of the decommissioned one. */
static rd_atomic32_t backoff_kept_backoff_inherited_cnt;

/** `reconnect.backoff.ms` and `reconnect.backoff.max.ms` used by the test,
 *  and the time spent with all brokers down. */
#define BACKOFF_KEPT_BACKOFF_MS     100
#define BACKOFF_KEPT_BACKOFF_MAX_MS 10000
#define BACKOFF_KEPT_OUTAGE_MS      5000

/**
 * @brief Log callback that counts the re-bootstrap sequences started and the
 *        reconnect backoffs inherited by the re-added bootstrap brokers.
 */
static void backoff_kept_log_cb(const rd_kafka_t *rk,
                                int level,
                                const char *fac,
                                const char *buf) {
        if (strstr(buf, "Starting re-bootstrap sequence"))
                rd_atomic32_add(&backoff_kept_rebootstrap_sequence_cnt, 1);
        else if (strstr(buf, "Inheriting reconnect backoff"))
                rd_atomic32_add(&backoff_kept_backoff_inherited_cnt, 1);
}

/**
 * @brief Error callback that only allows the errors of an unreachable
 *        cluster.
 */
static void backoff_kept_error_cb(rd_kafka_t *rk,
                                  int err,
                                  const char *reason,
                                  void *opaque) {
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__TRANSPORT ||
                        err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN,
                    "Unexpected error: %s: %s", rd_kafka_err2name(err),
                    reason);
}

/**
 * @brief Upper bound of the re-bootstrap sequences started by the failed
 *        connection attempts of a bootstrap broker in \p outage_ms , when
 *        the reconnect backoff is kept across them.
 *
 *        While all brokers are down each failed attempt of the only
 *        bootstrap broker starts a sequence, and the attempts are paced by
 *        its reconnect backoff, inherited by the broker object each sequence
 *        re-adds: starting from \p backoff_ms it's doubled when the attempt
 *        is made and again when it fails, with -25%..+50% jitter and capped
 *        at \p backoff_max_ms . The bound takes the -25% jitter at every
 *        step and starts from a broker object that attempts right away.
 */
static int
backoff_kept_max_sequences(int outage_ms, int backoff_ms, int backoff_max_ms) {
        int cnt = 0, t = 0, b = backoff_ms;

        while (t <= outage_ms) {
                /* A failed attempt at `t` starts a sequence. */
                cnt++;
                /* The attempt doubles the backoff, its failure schedules the
                 * next attempt after the jittered backoff and doubles it
                 * again. */
                b = RD_MIN(b * 2, backoff_max_ms);
                t += RD_MIN(b * 3 / 4, backoff_max_ms);
                b = RD_MIN(b * 2, backoff_max_ms);
        }
        return cnt;
}

/**
 * @brief Verify that the reconnect backoff is kept across re-bootstrap
 *        sequences while all brokers are down.
 *
 *        The only bootstrap server is a port nothing listens on, so every
 *        connection attempt is refused right away. Each failure reaches the
 *        "all brokers down" state and starts a re-bootstrap sequence, that
 *        decommissions the bootstrap broker and re-adds it as a new broker
 *        object: it must inherit the reconnect backoff of the decommissioned
 *        one, so that the attempts, and the sequences they start, are paced
 *        by `reconnect.backoff.ms` up to `reconnect.backoff.max.ms` instead
 *        of following each other as soon as a cluster connection is asked
 *        for again, every `sparse_connect_intvl` (50ms here) while the
 *        application keeps requesting metadata: about 100 sequences in 5s
 *        without the inheritance, 4 at most with it.
 */
static void do_test_rebootstrap_all_brokers_down_backoff_kept(
    rd_kafka_type_t rk_type) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        char *unused_bootstraps;
        test_conf_log_interceptor_t *log_interceptor;
        const char *debug_contexts[] = {"generic", "broker", NULL};
        char backoff_ms[16], backoff_max_ms[16];
        int64_t abs_timeout_us;
        int32_t sequence_cnt, inherited_cnt, max_sequence_cnt;

        SUB_TEST_QUICK("%s",
                       rk_type == RD_KAFKA_PRODUCER ? "producer" : "consumer");

        /* A port nothing listens on: borrow the one of a mock broker and
         * destroy the cluster, the kernel then refuses the connections on
         * every platform. */
        mcluster          = test_mock_cluster_new(1, &bootstraps);
        unused_bootstraps = rd_strdup(bootstraps);
        test_mock_cluster_destroy(mcluster);

        rd_atomic32_init(&backoff_kept_rebootstrap_sequence_cnt, 0);
        rd_atomic32_init(&backoff_kept_backoff_inherited_cnt, 0);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", unused_bootstraps);
        /* Avoid them being changed through test.conf */
        rd_snprintf(backoff_ms, sizeof(backoff_ms), "%d",
                    BACKOFF_KEPT_BACKOFF_MS);
        rd_snprintf(backoff_max_ms, sizeof(backoff_max_ms), "%d",
                    BACKOFF_KEPT_BACKOFF_MAX_MS);
        test_conf_set(conf, "reconnect.backoff.ms", backoff_ms);
        test_conf_set(conf, "reconnect.backoff.max.ms", backoff_max_ms);
        rd_kafka_conf_set_error_cb(conf, backoff_kept_error_cb);
        log_interceptor = test_conf_set_log_interceptor(
            conf, backoff_kept_log_cb, debug_contexts);
        rk = test_create_handle(rk_type, conf);

        abs_timeout_us = test_clock() + BACKOFF_KEPT_OUTAGE_MS * 1000LL;
        while (test_clock() < abs_timeout_us) {
                const struct rd_kafka_metadata *md;

                /* Keep asking for a cluster connection, as an application
                 * refreshing its metadata would, so that each re-added
                 * bootstrap broker is selected for connection as soon as
                 * `rd_kafka_connect_any()` allows it. */
                if (rd_kafka_metadata(rk, 0, NULL, &md, 100) ==
                    RD_KAFKA_RESP_ERR_NO_ERROR)
                        rd_kafka_metadata_destroy(md);
                rd_kafka_poll(rk, 0);
        }

        rd_kafka_destroy(rk);
        rd_free(log_interceptor);
        rd_free(unused_bootstraps);

        /* The first attempt fails right away and every later one follows
         * the inherited backoff: at least the first two sequences fit in
         * the outage, 300ms apart at most, and at most the ones computed
         * from the minimum jitter. */
        max_sequence_cnt = backoff_kept_max_sequences(
            BACKOFF_KEPT_OUTAGE_MS, BACKOFF_KEPT_BACKOFF_MS,
            BACKOFF_KEPT_BACKOFF_MAX_MS);
        sequence_cnt = rd_atomic32_get(&backoff_kept_rebootstrap_sequence_cnt);
        TEST_SAY("Found %d re-bootstrap sequences in %dms\n", sequence_cnt,
                 BACKOFF_KEPT_OUTAGE_MS);
        TEST_ASSERT(sequence_cnt >= 2 && sequence_cnt <= max_sequence_cnt,
                    "Expected between 2 and %d re-bootstrap sequences in "
                    "%dms with all brokers down, got %d",
                    max_sequence_cnt, BACKOFF_KEPT_OUTAGE_MS, sequence_cnt);

        /* Every sequence starts while the bootstrap broker is down, so the
         * broker object each one re-adds inherits its reconnect backoff. */
        inherited_cnt = rd_atomic32_get(&backoff_kept_backoff_inherited_cnt);
        TEST_SAY("Found %d inherited reconnect backoff(s)\n", inherited_cnt);
        TEST_ASSERT(inherited_cnt == sequence_cnt,
                    "Expected the re-added bootstrap broker to inherit the "
                    "reconnect backoff on each of the %d re-bootstrap "
                    "sequences, got %d",
                    sequence_cnt, inherited_cnt);

        SUB_TEST_PASS();
}

int main_0152_rebootstrap_local(int argc, char **argv) {
        rd_kafka_type_t rk_type;
        char *bootstrap_servers[] = {NULL, "", "localhost:9998"};
        for (rk_type = RD_KAFKA_PRODUCER; rk_type <= RD_KAFKA_CONSUMER;
             rk_type++) {
                size_t i;
                for (i = 0; i < RD_ARRAY_SIZE(bootstrap_servers); i++) {
                        do_test_rebootstrap_brokers_add(rk_type,
                                                        bootstrap_servers[i]);
                }
                do_test_rebootstrap_all_brokers_down_backoff_kept(rk_type);
        }

        return 0;
}
