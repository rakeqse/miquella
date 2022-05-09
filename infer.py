#!/usr/bin/env python
from base64 import encode
import os
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
import sys
import logging
import cv2 as cv
import torch

from serde import decodeFromRaw, encodeResult
from dotenv import load_dotenv


if __name__ == "__main__":
    model = torch.hub.load("ultralytics/yolov5", "yolov5s")
    load_dotenv()
    broker = os.environ.get("BROKER")
    group = "infer-group"
    topic = os.environ.get("VIDEO_TOPIC")

    partNum = int(os.environ.get("PARTITION"))

    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {
        "bootstrap.servers": broker,
        "group.id": group,
        "session.timeout.ms": 6000,
        "auto.offset.reset": "earliest",
    }

    producerConf = {"bootstrap.servers": broker}
    resultTopic = os.environ.get("RESULT_TOPIC")

    p = Producer(**producerConf)

    # Create logger for consumer (logs will be emitted when poll() is called)
    logger = logging.getLogger("consumer")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)-15s %(levelname)-8s %(message)s")
    )
    logger.addHandler(handler)

    def print_assignment(consumer, partitions):
        print("Assignment:", partitions)

    def poll(consumer):
        msg = consumer.poll(1.0)
        if msg is None:
            return None
        if msg.error():
            raise KafkaException(msg.error())
        else:
            frame = decodeFromRaw(msg.value())
            if frame["img"] is None:
                return None
            else:
                return frame

    def filterNone(consumer):
        return False if consumer is None else True

    def send(msgs, results):
        results.render()
        pandas = results.pandas()
        print(len(msgs))
        for i, (msg, json, img) in enumerate(zip(msgs, pandas.xyxy, results.imgs)):
            p.produce(
                resultTopic,
                encodeResult(
                    {
                        "cameraID": msg["cameraID"],
                        "frame": img,
                        "result": f'{json.groupby("name").count()["xmin"].to_json()}|"cameraID":{msg["cameraID"]}',
                        "timestamp": msg["timestamp"],
                    }
                ),
                partition=int(msg["cameraID"]),
            )

    consGroup = []
    # Create Consumer instance
    # Hint: try debug='fetch' to generate some log messages
    for partition in range(partNum):
        t = Consumer(conf, logger=logger)
        # Subscribe to topics
        t.assign([TopicPartition(topic, partition)])
        consGroup.append(t)

    # Read messages from Kafka, print to stdout
    try:
        while True:
            msgs = list(filter(filterNone, map(poll, consGroup)))
            frames = [msg["img"] for msg in msgs]
            results = model(frames)
            results.print()
            send(msgs, results)
    except KeyboardInterrupt:
        sys.stderr.write("%% Aborted by user\n")

    finally:
        # Close down consumer to commit final offsets.
        for c in consGroup:
            c.close()
        cv.destroyAllWindows()
