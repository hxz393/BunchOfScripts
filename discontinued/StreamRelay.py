import logging
import os
import queue
import subprocess as sp
import threading
from typing import Tuple

import cv2
import numpy
from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger(__name__)

# Global Variables
直播地址 = 'https://ali-acfun-adaptive.pull.etoote.com/livecloud/kszt_XVdsEVMcWQ4_hd2000.flv?auth_key=1650105523-0-0-80b32cbb1c5c9bcaf9dbb0fd0b09bb4a&oidc=alihb&tsc=origin&kabr_spts=25349886'
推流地址 = 'rtmp://192.168.2.204:1935/live/59952651'
捕获 = cv2.VideoCapture(直播地址)
水印 = cv2.imread(os.path.normpath('E:/Programming/Python/os/logo.png'))
q = queue.Queue()


def capture_video_stream(捕获: cv2.VideoCapture) -> Tuple[int, int, int]:
    """
    Captures video stream and retrieves frames per second, width and height.

    :param 捕获: Capture object.
    :return: Tuple containing fps, width, and height.
    """
    if 捕获.isOpened():
        fps = int(捕获.get(cv2.CAP_PROP_FPS))
        width = int(0.5 * 捕获.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(0.5 * 捕获.get(cv2.CAP_PROP_FRAME_HEIGHT))
        return fps, width, height
    else:
        logger.error("Failed to capture the video stream.")
        return None


def create_ffmpeg_command(fps: int, width: int, height: int, 推流地址: str) -> list:
    """
    Creates and returns the FFMPEG command list.

    :param fps: Frames per second.
    :param width: Width of the frame.
    :param height: Height of the frame.
    :param 推流地址: Streaming address.
    :return: FFMPEG command list.
    """
    return [
        'ffmpeg',
        '-y',
        '-f', 'rawvideo',
        '-vcodec', 'rawvideo',
        '-pix_fmt', 'bgr24',
        '-s', "{}x{}".format(width, height),
        '-r', str(fps),
        '-i', '-',
        '-c:v', 'libx264',
        '-pix_fmt', 'yuv420p',
        '-preset', 'ultrafast',
        '-f', 'flv',
        推流地址
    ]


def que_put(q: queue.Queue, 捕获: cv2.VideoCapture):
    while True:
        q.put(捕获.read()[1])
        if q.qsize() > 100:
            q.get()


def add_image(q: queue.Queue, 管道: sp.Popen, 水印: numpy.ndarray):
    while True:
        if not q.empty():
            frame = q.get()
            frame = img_add_text(frame, 水印, "City Battle of Kings", 15, 60, (255, 255, 255), 50)
            frame = cv2.resize(frame, (0, 0), fx=0.5, fy=0.5, interpolation=cv2.INTER_NEAREST)
            管道.stdin.write(frame.tostring())


def img_add_text(img: numpy.ndarray, watermark: numpy.ndarray, text: str, left: int, top: int, textColor=(0, 255, 0), textSize=20):
    if isinstance(img, numpy.ndarray):
        img = Image.fromarray(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))
        wmk = Image.fromarray(cv2.cvtColor(watermark, cv2.COLOR_BGR2RGB))
    layer = Image.new('RGBA', img.size, (0, 0, 0, 0))
    layer.paste(wmk, (img.size[0] - 150, img.size[1] - 60))
    marked_img = Image.composite(layer, img, layer)
    draw = ImageDraw.Draw(marked_img)
    fontText = ImageFont.truetype("font/simsun.ttc", textSize, encoding="utf-8")
    draw.text((left, top), text, textColor, font=fontText)
    return cv2.cvtColor(numpy.asarray(marked_img), cv2.COLOR_RGB2BGR)


def main():
    fps, width, height = capture_video_stream(捕获)
    command = create_ffmpeg_command(fps, width, height, 推流地址)
    管道 = sp.Popen(command, stdin=sp.PIPE)

    threads = [threading.Thread(target=que_put, args=(q, 捕获)),
               threading.Thread(target=add_image, args=(q, 管道, 水印))]
    for t in threads:
        t.start()


if __name__ == '__main__':
    main()
