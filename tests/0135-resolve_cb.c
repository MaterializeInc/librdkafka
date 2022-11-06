/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2022, Magnus Edenhill
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

#include "rdkafka.h"

/**
 * @name Test a custom address resolution callback.
 *
 * The test sets a bogus metadata.broker.list, and then uses the resolution
 * callback to resolve the correct address. If the resolution callback is not
 * invoked, then the bogus name will be resolved and the test will fail.
 */

typedef struct broker_addr_s {
        const char *node;
        const char *service;
} broker_addr_t;

static int resolve_cb(const char *node,
                      const char *service,
                      const struct addrinfo *hints,
                      struct addrinfo **res,
                      void *opaque) {

        if (strcmp(node, "noexist") == 0) {
                broker_addr_t *addr = (broker_addr_t *)opaque;

                TEST_SAY("Overriding resolution for %s:%s to %s:%s\n", node,
                         service, addr->node, addr->service);

                node    = addr->node;
                service = addr->service;
        }

        return getaddrinfo(node, service, hints, res);
}

int main_0135_resolve_cb(int argc, char **argv) {
        const int partition = 0;
        const int msgcnt    = 1;
        const int msgsize   = 100;

        uint64_t testid   = test_id_generate();
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);

        rd_kafka_conf_t *conf;
        test_conf_init(&conf, NULL, 0);
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);
        rd_kafka_conf_set_resolve_cb(conf, resolve_cb);

        const char *brokers =
            rd_strdup(test_conf_get(conf, "metadata.broker.list"));
        TEST_SAY("Discovered broker list: %s\n", brokers);

        {
                char *n;
                char *port = "9092";

                if ((n = strchr(brokers, ',')))
                        *n = '\0';

                if ((n = strrchr(brokers, ':')) &&
                    ((n == strchr(brokers, ':')) || *(n - 1) == ']')) {
                        *n   = '\0';
                        port = n + 1;
                }

                broker_addr_t *addr = rd_calloc(1, sizeof(broker_addr_t));
                addr->node          = brokers;
                addr->service       = port;

                TEST_SAY("Extracted first broker address as %s:%s\n",
                         addr->node, addr->service);
                rd_kafka_conf_set_opaque(conf, (void *)addr);
        }


        TEST_SAY("Setting bogus broker list\n");
        test_conf_set(conf, "metadata.broker.list", "noexist:1234");

        rd_kafka_t *rk        = test_create_handle(RD_KAFKA_PRODUCER, conf);
        rd_kafka_topic_t *rkt = test_create_producer_topic(rk, topic, NULL);

        test_produce_msgs(rk, rkt, testid, partition, 0, msgcnt, NULL, msgsize);

        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);

        return 0;
}
