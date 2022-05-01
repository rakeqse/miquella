#!/usr/bin/env python

from base64 import decode
from confluent_kafka import Consumer, KafkaException
import sys
import logging
import cv2 as cv

from utils import decodeFromBytes


if __name__ == '__main__':
    broker = "pi.viole.in:9092"
    group = "stream-group"
    topics = ['stream-kafka']
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {'bootstrap.servers': broker, 'group.id': group, 'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest'}

    # Create logger for consumer (logs will be emitted when poll() is called)
    logger = logging.getLogger('consumer')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    # Create Consumer instance
    # Hint: try debug='fetch' to generate some log messages
    c = Consumer(conf, logger=logger)

    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

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
                frame=decodeFromBytes(msg.value())
                if  not (frame is None):
                    cv.imshow('frame', frame)
                if cv.waitKey(1) == ord('q'):
                    break
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        c.close()
        cv.destroyAllWindows()
