#!/usr/bin/env python
import sys

import cv2 as cv
from confluent_kafka import Producer
from serde import encodeToProto, encodeToRaw

if __name__ == "__main__":

    broker = "pi.viole.in:9092"
    topic = "stream-kafka-proto2"

    # Producer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {"bootstrap.servers": broker}

    # Create Producer instance
    p = Producer(**conf)
    cam = cv.VideoCapture("Junction2.avi")  # TODO: consume the video

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write("%% Message failed delivery: %s\n" % err)
        else:
            sys.stderr.write(
                "%% Message delivered to %s [%d] @ %d\n"
                % (msg.topic(), msg.partition(), msg.offset())
            )

    try:
        while True:
            success, frame = cam.read()
            try:
                p.produce(topic, encodeToRaw(frame), callback=delivery_callback)
            except BufferError:
                print("some thing when wrong")
            p.poll(0)
    except:
        print("\nExiting.")
        cam.release()
        p.flush()
        sys.exit(1)
