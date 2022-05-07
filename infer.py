#!/usr/bin/env python

from confluent_kafka import Consumer, KafkaException, Producer
import sys
import logging
import cv2 as cv
import torch

from serde import decodeFromRaw, encodeResult


if __name__ == "__main__":
    model = torch.hub.load("ultralytics/yolov5", "yolov5s")
    broker = "pi.viole.in:9092"
    group = "stream-group"
    topics = ["stream-kafka-proto2"]
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {
        "bootstrap.servers": broker,
        "group.id": group,
        "session.timeout.ms": 6000,
        "auto.offset.reset": "earliest",
    }

    producerConf = {"bootstrap.servers": broker}
    resultTopic = "result-topic"

    p = Producer(**producerConf)

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
                frame = decodeFromRaw(msg.value())
                if not (frame["img"] is None):
                    results = model(frame["img"])
                    results.render()
                    j = (
                        results.pandas()
                        .xyxy[0]
                        .groupby("name")
                        .count()["xmin"]
                        .to_json()
                    )
                    p.produce(
                        resultTopic,
                        encodeResult(
                            {
                                "cameraID": frame["cameraID"],
                                "frame": results.imgs[0],
                                "result": j,
                                "timestamp": frame["timestamp"],
                            }
                        ),
                    )
    except KeyboardInterrupt:
        sys.stderr.write("%% Aborted by user\n")

    finally:
        # Close down consumer to commit final offsets.
        c.close()
        cv.destroyAllWindows()
