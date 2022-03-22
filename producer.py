#!/usr/bin/env python
from confluent_kafka import Producer
import cv2 as cv
import sys

from utils import encodeToBytes

if __name__ == '__main__':
    # if len(sys.argv) != 3:
    #     sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
    #     sys.exit(1)

    broker = "localhost:9092"
    topic = "stream-kafka"

    # Producer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {'bootstrap.servers': broker}

    # Create Producer instance
    p = Producer(**conf)
    cam = cv.VideoCapture(0)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))
    try:
        while(True):
            success, frame = cam.read()
            try:
                p.produce(topic, encodeToBytes(frame), callback=delivery_callback)
            except BufferError:
                print('some thing when wrong')
            p.poll(0)
    except:
        print("\nExiting.")
        cam.release()
        p.flush()
        sys.exit(1)