#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Example high-level Kafka 0.9 balanced Consumer
#
from confluent_kafka import Consumer, KafkaException
import json
import logging
import sys
from pprint import pformat


def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    print("\nKAFKA Stats: {}\n".format(pformat(stats_json)))


def print_usage_and_exit(program_name):
    sys.stderr.write(
        "Usage: %s [options..] <bootstrap-brokers> <group> <topic1> <topic2> ..\n"
        % program_name
    )
    options = """
 Options:
  -T <intvl>   Enable client statistics at specified interval (ms)
"""
    sys.stderr.write(options)
    sys.exit(1)


if __name__ == "__main__":
    broker = "pi.viole.in:9092"
    group = "example-group"
    topics = ["example-topic"]
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {
        "bootstrap.servers": broker,
        "group.id": group,
        "session.timeout.ms": 6000,
        "auto.offset.reset": "earliest",
    }

    # Create logger for consumer (logs will be emitted when poll() is called)
    logger = logging.getLogger("consumer")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)-15s %(levelname)-8s %(message)s")
    )
    logger.addHandler(handler)

    # Create Consumer instance
    # Hint: try debug='fetch' to generate some log messages
    c = Consumer(conf, logger=logger)

    def print_assignment(consumer, partitions):
        print("Assignment:", partitions)

    # Subscribe to topics
    c.subscribe(topics, on_assign=print_assignment)

    # Read messages from Kafka, print to stdout
    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # Proper message
                sys.stderr.write(
                    "%% %s [%d] at offset %d with key %s:\n"
                    % (msg.topic(), msg.partition(), msg.offset(), str(msg.key()))
                )
                print(msg.value())

    except KeyboardInterrupt:
        sys.stderr.write("%% Aborted by user\n")

    finally:
        # Close down consumer to commit final offsets.
        c.close()
