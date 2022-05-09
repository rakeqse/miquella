from datetime import datetime
import cv2 as cv
import numpy as np

import protobuf.kafka_message_pb2 as kafka_message_pb2

from schema_pb2 import Raw, Result


def encodeToRaw(frame, id):
    m = Raw()
    gray = cv.resize(frame, [640, 480])
    _, buffer = cv.imencode(".jpg", gray)
    m.cameraID = id
    m.frame = buffer.tobytes()
    m.timestamp = str(int(datetime.utcnow().timestamp()))
    return m.SerializeToString()


def decodeFromRaw(buffer):
    m = Raw()
    m.ParseFromString(buffer)
    nparr = np.frombuffer(m.frame, np.uint8)
    # decode image
    img = cv.imdecode(nparr, cv.IMREAD_COLOR)
    return {
        "cameraID": m.cameraID,
        "frame": m.frame,
        "timestamp": m.timestamp,
        "img": img,
    }


def encodeResult(result):
    m = Result()
    m.cameraID = result["cameraID"]
    m.frame = encodeToBytes(result["frame"])
    m.timestamp = result["timestamp"]
    m.result = result["result"]
    return m.SerializeToString()


def decodeResult(buffer):
    m = Result()
    m.ParseFromString(buffer)
    nparr = np.frombuffer(m.frame, np.uint8)
    # decode image
    img = cv.imdecode(nparr, cv.IMREAD_COLOR)
    return {
        "cameraID": m.cameraID,
        "frame": m.frame,
        "timestamp": m.timestamp,
        "img": img,
        "result": m.result,
    }


def encodeToBytes(frame):  # convert to grayscale, resize and return bytes
    _, buffer = cv.imencode(".jpg", frame)
    return buffer.tobytes()


def decodeFromBytes(buffer):
    nparr = np.frombuffer(buffer, np.uint8)
    # decode image
    img = cv.imdecode(nparr, cv.IMREAD_COLOR)
    return img


def encodeToProto(frame):
    message = kafka_message_pb2.KafkaMessage()
    gray = cv.cvtColor(frame, cv.COLOR_BGR2GRAY)
    gray = cv.resize(gray, [800, 600])
    _, buffer = cv.imencode(".jpg", gray)
    message.data = buffer.tobytes()
    message.cameraID = "1"
    return message.SerializeToString()


def decodeFromProto(buffer):
    message = kafka_message_pb2.KafkaMessage()
    message.ParseFromString(buffer)
    nparr = np.frombuffer(message.data, np.uint8)
    # decode image
    img = cv.imdecode(nparr, cv.IMREAD_COLOR)
    return img
