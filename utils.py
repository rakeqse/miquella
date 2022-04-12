import cv2 as cv
import numpy as np

import kafka_message_pb2

def encodeToBytes(frame): # convert to grayscale, resize and return bytes
    gray = cv.cvtColor(frame, cv.COLOR_BGR2GRAY)
    gray = cv.resize(gray, [640, 480])
    _, buffer = cv.imencode('.jpg', gray)
    return buffer.tobytes()

def decodeFromBytes(buffer):
    nparr = np.frombuffer(buffer, np.uint8)
    # decode image
    img = cv.imdecode(nparr, cv.IMREAD_COLOR)
    return img


def encodeToProto(frame):
    message = kafka_message_pb2.KafkaMessage()
    gray = cv.cvtColor(frame, cv.COLOR_BGR2GRAY)
    gray = cv.resize(gray, [640, 480])
    _, buffer = cv.imencode('.jpg', gray)
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
