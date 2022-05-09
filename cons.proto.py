#!/usr/bin/env python

from confluent_kafka import Consumer, KafkaException, TopicPartition
import cv2 as cv
import logging
import asyncio
import websockets
import functools
import argparse
from serde import decodeResult


parser = argparse.ArgumentParser()
parser.add_argument("partition", type=int)

arg = parser.parse_args()
broker = "pi.viole.in:9092"
group = "ws-group"
topic = "result-topic"
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
handler.setFormatter(logging.Formatter("%(asctime)-15s %(levelname)-8s %(message)s"))
logger.addHandler(handler)


# Create Consumer instance
# Hint: try debug='fetch' to generate some log messages
c = Consumer(conf, logger=logger)

c.assign([TopicPartition(topic, arg.partition)])


async def run(websocket):
    loop = asyncio.get_running_loop()
    poll = functools.partial(c.poll, 1.0)
    try:
        while True:
            msg = await loop.run_in_executor(None, poll)
            print(msg)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                frame = decodeResult(msg.value())
                if not (frame is None):
                    await websocket.send(frame["result"])
                    # socket.send(results.print()) here
    finally:
        # Close down consumer to commit final offsets.
        c.close()
        cv.destroyAllWindows()


async def main():
    async with websockets.connect("ws://localhost:7731") as websocket:
        await run(websocket)


if __name__ == "__main__":
    asyncio.run(main())
