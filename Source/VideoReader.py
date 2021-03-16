from datetime import timedelta
from pathlib import Path
import csv
import time

from imutils.object_detection import non_max_suppression
import numpy as np
import cv2
import pytesseract

ROOT_DIR = Path(__file__).resolve().parents[1]
SOURCE_DIR = Path(__file__).resolve().parents[0]


class BoundingBox:

    def __init__(self, startX: int, startY: int, endX: int, endY: int):
        self.startX = startX
        self.startY = startY
        self.endX = endX
        self.endY = endY

    def center(self):
        x = (self.startX + self.endX) / 2
        y = (self.startY + self.endY) / 2
        return (x, y)

    def draw(self, image):
        cv2.rectangle(image,
                      (self.startX, self.startY),
                      (self.endX, self.endY),
                      (0, 255, 0),
                      2)

    def size(self):
        height = self.endY - self.startY
        width = self.endX - self.startX
        return height * width


class Frame:

    east_net = None

    def __init__(self, image, video_name: str, timestamp: timedelta):
        self.image = image
        self.video_name = video_name
        self.timestamp = timestamp

    def find_text(self, min_confidence: float = 0.8, save_boxes: bool = False):
        # https://www.pyimagesearch.com/2018/08/20/opencv-text-detection-east-text-detector/
        # load the input image and grab the image dimensions
        image = self.image.copy()
        (H, W) = image.shape[:2]

        # set the new width and height and then determine the ratio in change
        # for both the width and height
        (newH, newW) = (H - H % 32, W - W % 32)
        rW = W / float(newW)
        rH = H / float(newH)

        # resize the image and grab the new image dimensions
        image = cv2.resize(image, (newW, newH))
        (H, W) = image.shape[:2]

        # define the two output layer names for the EAST detector model that
        # we are interested -- the first is the output probabilities and the
        # second can be used to derive the bounding box coordinates of text
        layerNames = [
    	   "feature_fusion/Conv_7/Sigmoid",
	       "feature_fusion/concat_3"]

        # load the pre-trained EAST text detector
        if not Frame.east_net:
            print("[INFO] loading EAST text detector...", end='\r')
            net_path = Path(SOURCE_DIR, 'frozen_east_text_detection.pb')
            Frame.east_net = cv2.dnn.readNet(str(net_path))

        # construct a blob from the image and then perform a forward pass of
        # the model to obtain the two output layer sets
        blob = cv2.dnn.blobFromImage(image, 1.0, (W, H),
        	(123.68, 116.78, 103.94), swapRB=True, crop=False)
        start = time.time()
        Frame.east_net.setInput(blob)
        (scores, geometry) = Frame.east_net.forward(layerNames)
        end = time.time()

        # show timing information on text prediction
        print("[INFO] text detection took {:.6f} seconds".format(end - start))

        # grab the number of rows and columns from the scores volume, then
        # initialize our set of bounding box rectangles and corresponding
        # confidence scores
        (numRows, numCols) = scores.shape[2:4]
        rects = []
        confidences = []

        # loop over the number of rows
        for y in range(0, numRows):
            # extract the scores (probabilities), followed by the geometrical
            # data used to derive potential bounding box coordinates that
            # surround text
            scoresData = scores[0, 0, y]
            xData0 = geometry[0, 0, y]
            xData1 = geometry[0, 1, y]
            xData2 = geometry[0, 2, y]
            xData3 = geometry[0, 3, y]
            anglesData = geometry[0, 4, y]

            # loop over the number of columns
            for x in range(0, numCols):
                # if our score does not have sufficient probability, ignore it
                if scoresData[x] < min_confidence:
                    continue
                # compute the offset factor as our resulting feature maps will
                # be 4x smaller than the input image
                (offsetX, offsetY) = (x * 4.0, y * 4.0)
                # extract the rotation angle for the prediction and then
                # compute the sin and cosine
                angle = anglesData[x]
                cos = np.cos(angle)
                sin = np.sin(angle)
                # use the geometry volume to derive the width and height of
                # the bounding box
                h = xData0[x] + xData2[x]
                w = xData1[x] + xData3[x]
                # compute both the starting and ending (x, y)-coordinates for
                # the text prediction bounding box
                endX = int(offsetX + (cos * xData1[x]) + (sin * xData2[x]))
                endY = int(offsetY - (sin * xData1[x]) + (cos * xData2[x]))
                startX = int(endX - w)
                startY = int(endY - h)
                # add the bounding box coordinates and probability score to
                # our respective lists
                rects.append((startX, startY, endX, endY))
                confidences.append(scoresData[x])

        # apply non-maxima suppression to suppress weak, overlapping bounding
        # boxes
        boxes = non_max_suppression(np.array(rects), probs=confidences)

        # rescale bounding boxes and return
        results = []
        for (startX, startY, endX, endY) in boxes:
            # scale the bounding box coordinates based on the respective
            # ratios
            startX = int(startX * rW)
            startY = int(startY * rH)
            endX = int(endX * rW)
            endY = int(endY * rH)
            bb = BoundingBox(startX, startY, endX, endY)
            results.append(bb)

        if save_boxes:
            for bb in results:
                bb.draw(self.image)
            self.save(replace=True)

        return (results, confidences)

    def text(self):
        # preprocess to enhance accuracy
        # convert to grayscale:
        i = cv2.cvtColor(self.image, cv2.COLOR_BGR2GRAY)
        # dilate
        i = cv2.dilate(self.image, np.ones((5, 5)), iterations=1)
        # denoise
        i = cv2.GaussianBlur(self.image, )
        # threshold
        i = cv2.threshold(self.image, 0, 255,
                              cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
        return pytesseract.image_to_string(i, lang='eng')

    def save(self, replace: bool = False):
        dest_path = Path(ROOT_DIR, 'Frames', self.video_name,
                         '%s.png' % str(self.timestamp))
        if replace or not dest_path.exists():
            dest_path.parents[0].mkdir(parents=True, exist_ok=True)
            cv2.imwrite(str(dest_path), self.image)
            return True
        return False


class FrameExtractor:

    tuning_threshold_default = timedelta(seconds=4) # for personal machine

    def __init__(self, video_path: Path):
        self.path = video_path
        self.video_name = video_path.parts[-2]
        self.video = cv2.VideoCapture(str(video_path))

        self.tuning_threshold = FrameExtractor.tuning_threshold_default
        bench_test = Path(SOURCE_DIR, 'Tests', 'FrameExtractor',
                         'scrubbing_by_time_vs_grab.csv')
        if bench_test.exists():
            with bench_test.open('r') as infile:
                reader = csv.DictReader(infile)
                results = [row for row in reader]
            for item in results:
                ts_runtime = float(item['timestamp_runtime'])
                gr_runtime = float(item['grab_runtime'])
                if ts_runtime < gr_runtime:
                    tune_step = float(item['test_step'])
                    self.tuning_threshold = timedelta(seconds=tune_step)
                    break

    def frames(self,
               start: timedelta = timedelta(),
               stop: timedelta = None,
               step: timedelta = timedelta(seconds=10)):
        # Scrubbing by timestamp outperforms grabbing and discarding frames at
        # some critical value.  On my personal computer, this threshold comes
        # at a step size of 4 seconds between frames (the default).
        # Running FrameExtractor.benchmark() on your machine can determine
        # this value for your test environment.  When you do so, the result is
        # stored in Source/Tests in csv form, and once a test has been run,
        # FrameExtractor will intelligently load the derived threshold value
        # during initialization.
        if step < self.tuning_threshold:
            generator = self.frames_by_grab(start, stop, step)
        else:
            generator = self.frames_by_time(start, stop, step)
        for frame in generator:
            yield frame

    def frames_by_time(self,
                       start: timedelta = timedelta(),
                       stop: timedelta = None,
                       step: timedelta = timedelta(seconds=10)):
        print(self.video_name)
        self.video.set(cv2.CAP_PROP_POS_MSEC, start.seconds * 1000)
        timestamp = start
        (capture_success, frame) = self.video.read()
        while capture_success:
            if stop and timestamp > stop:
                break
            print(timestamp, end='\r')
            yield Frame(frame, self.video_name, timestamp)
            timestamp += step
            self.video.set(cv2.CAP_PROP_POS_MSEC, timestamp.seconds * 1000)
            (capture_success, frame) = self.video.read()

    def frames_by_grab(self,
                       start: timedelta = timedelta(),
                       stop: timedelta = None,
                       step: timedelta = timedelta(seconds=1)):
        print(self.video_name)
        self.video.set(cv2.CAP_PROP_POS_MSEC, start.microseconds / 1000)
        next_time = start
        while True:
            if self.video.grab():
                msecs = self.video.get(cv2.CAP_PROP_POS_MSEC)
                curr_time = timedelta(milliseconds=msecs)
                if stop and curr_time > stop:
                    break
                if curr_time >= next_time:
                    print(next_time, end='\r')
                    (capture_success, frame) = self.video.retrieve()
                    yield Frame(frame, self.video_name, next_time)
                    next_time = curr_time + step
            else:
                break

    def benchmark(self,
                  step_low: timedelta = timedelta(seconds=1),
                  step_high: timedelta = timedelta(seconds=10)):
        def by_time(step):
            start = time.time()
            for frame in self.frames_by_time(step=step):
                pass
            end = time.time()
            return end - start

        def by_grab(step):
            start = time.time()
            for frame in self.frames_by_grab(step=step):
                pass
            end = time.time()
            return end - start

        results = []
        fieldnames = ['test_step', 'timestamp_runtime', 'grab_runtime']
        for test_step in range(step_low.seconds, step_high.seconds):
            t = timedelta(seconds=test_step)
            results.append({
                fieldnames[0] : test_step,
                fieldnames[1] : by_time(t),
                fieldnames[2] : by_grab(t)
            })

        test_path = Path(SOURCE_DIR, 'Tests', 'FrameExtractor',
                          'scrubbing_by_time_vs_grab.csv')
        test_path.parents[0].mkdir(parents=True, exist_ok=True)
        with test_path.open('w') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            for item in results:
                writer.writerow(item)

        self.tuning_threshold = FrameExtractor.tuning_threshold_default
        for item in results:
            if item['timestamp_runtime'] < item['grab_runtime']:
                s = float(item['test_step'])
                self.tuning_threshold = timedelta(seconds=s)
                break

        return results


if __name__ == '__main__':
    video = Path(ROOT_DIR, 'Videos', 'Politics', 'Tim Pool',
                      '[2021-03-04] Ebay Just NUKED Dr. Seuss Books As OFFENSIVE, RSBN Gets Nuked By Youtube As Censorship Escalates',
                      '[video] amAHRY2neAg.mp4')
    g = FrameExtractor(video)
    start = time.time()
    for frame in g.frames(step=timedelta(seconds=10)):
        frame.find_text(save_boxes=True)
    end = time.time()
    print(end - start)
