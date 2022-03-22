import numpy as np
import cv2 as cv

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
