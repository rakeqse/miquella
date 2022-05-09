#!/usr/bin/env python

from confluent_kafka import Consumer, KafkaException, TopicPartition
import sys
import logging
import cv2 as cv
import argparse
from dotenv import load_dotenv

from serde import decodeResult


if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("partition", type=int)

    arg = parser.parse_args()

    broker = "pi.viole.in:9092"
    group = "stream-opencv-group"
    topics = "result-topic"
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
    c.assign([TopicPartition(topics, arg.partition)])

    # Read messages from Kafka, print to stdout
    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                frame = decodeResult(msg.value())
                if not (frame["img"] is None):
                    cv.imshow(f"frame {arg.partition}", frame["img"])
                    print(frame["result"])
                if cv.waitKey(1) == ord("q"):
                    break
    except KeyboardInterrupt:
        sys.stderr.write("%% Aborted by user\n")

    finally:
        # Close down consumer to commit final offsets.
        c.close()
        cv.destroyAllWindows()
