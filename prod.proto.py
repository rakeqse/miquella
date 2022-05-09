#!/usr/bin/env python
import sys
import os
import cv2 as cv
from confluent_kafka import Producer
from serde import encodeToRaw
import argparse
from dotenv import load_dotenv

if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("partition", type=int)
    parser.add_argument("video", type=str)

    arg = parser.parse_args()

    broker = os.environ.get("BROKER")
    topic = os.environ.get("VIDEO_TOPIC")

    # Producer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {"bootstrap.servers": broker}

    # Create Producer instance
    p = Producer(**conf)
    cam = cv.VideoCapture(arg.video)  # TODO: consume the video

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
                p.produce(
                    topic,
                    encodeToRaw(frame, str(arg.partition)),
                    callback=delivery_callback,
                    partition=arg.partition,
                )
            except BufferError:
                print("some thing when wrong")
            p.poll(0)  # this is for callback purpose
    except:
        print("\nExiting.")
        cam.release()
        p.flush()
        sys.exit(1)
