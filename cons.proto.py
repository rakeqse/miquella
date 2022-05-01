#!/usr/bin/env python

from confluent_kafka import Consumer, KafkaException
import cv2 as cv
import logging
import torch
import asyncio
import websockets
import functools

from decode import decodeFromProto

broker = "pi.viole.in:9092"
group = "stream-group"
topics = ["stream-kafka-proto"]
# Consumer configuration
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
conf = {
    "bootstrap.servers": broker,
    "group.id": group,
    "session.timeout.ms": 6000,
    "auto.offset.reset": "earliest",
}

model = torch.hub.load("ultralytics/yolov5", "yolov5s")

# Create logger for consumer (logs will be emitted when poll() is called)
logger = logging.getLogger("consumer")
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)-15s %(levelname)-8s %(message)s"))
logger.addHandler(handler)


def print_assignment(consumer, partitions):
    print("Assignment:", partitions)


# Create Consumer instance
# Hint: try debug='fetch' to generate some log messages
c = Consumer(conf, logger=logger)

c.subscribe(topics, on_assign=print_assignment)

CONNECTION = set()


async def register(websocket):
    print("new connection initiate")
    CONNECTION.add(websocket)
    try:
        await websocket.wait_closed()
    finally:
        CONNECTION.remove(websocket)


async def run():
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
                frame = decodeFromProto(msg.value())
                if not (frame is None):
                    results = model(frame)
                    results.render()

                    websockets.broadcast(
                        CONNECTION, results.pandas().xyxy[0].to_json(orient="records")
                    )
                    # socket.send(results.print()) here
    finally:
        # Close down consumer to commit final offsets.
        c.close()
        cv.destroyAllWindows()


async def main():
    async with websockets.serve(register, "localhost", "7731"):
        await run()


if __name__ == "__main__":
    asyncio.run(main())
