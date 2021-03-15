from pathlib import Path
import csv
import time

from imutils.object_detection import non_max_suppression
import numpy as np
import cv2
import pytesseract


class Frame:

    def __init__(self, image_matrix):
        self.image = image_matrix

    def find_text(self, width, height):
        # https://www.pyimagesearch.com/2018/08/20/opencv-text-detection-east-text-detector/
        orig = self.image.copy()
        (h, w) = image.shape[:2]
        (new_h, new_w) = (height, width)

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


class FrameExtractor:

    tuning_threshold_default = 4 # for personal machine

    def __init__(self, video_path: Path):
        self.path = video_path
        self.video = cv2.VideoCapture(str(video_path))

        self.tuning_threshold = FrameExtractor.tuning_threshold_default
        source_dir = Path(__file__).parents[0]
        bench_test = Path(source_dir, 'Tests', 'FrameExtractor',
                         'scrubbing_by_time_vs_grab.csv')
        if bench_test.exists():
            with bench_test.open('r') as infile:
                reader = csv.DictReader(infile)
                results = [row for row in reader]
            for item in results:
                if item['timestamp_runtime'] < item['grab_runtime']:
                    self.tuning_threshold = int(item['test_step'])
                    break

    def frames(self, start: int = 0, end: int = None, step: int = 10,
               save: bool = False):
        # Scrubbing by timestamp outperforms grabbing and discarding frames at
        # some critical value.  On my personal computer, this threshold comes
        # at a step size of 4 seconds between frames (the default).
        # Running FrameExtractor.benchmark() on your machine can determine
        # this value for your test environment.  When you do so, the result is
        # stored in Source/Tests in csv form, and once a test has been run,
        # FrameExtractor will intelligently load the derived threshold value
        # during initialization.
        if step < self.tuning_threshold:
            for frame in self.frames_by_grab(start, end, step, save):
                yield frame
        else:
            for frame in self.frames_by_time(start, end, step, save):
                yield frame

    def frames_by_time(self, start: int = 0, end: int = None, step: int = 10,
               save: bool = False):
        print(self.path.parts[-2])
        self.video.set(cv2.CAP_PROP_POS_MSEC, start)
        time = start
        (capture_success, frame) = self.video.read()
        while capture_success:
            if end and time > end * 1000:
                break
            hour = int(time / 1000 // 60 // 60)
            minute = int(time / 1000 // 60 % 60)
            second = int(time / 1000 % 60)
            print('\t%02d:%02d:%02d' % (hour, minute, second), end='\r')
            if save:
                video_title = self.path.parts[-2]
                root_dir = Path(__file__).resolve().parents[1]
                frame_dir = Path(root_dir, 'Frames', video_title)
                frame_dir.mkdir(parents=True, exists_ok=True)
                frame_title = '%02d-%02d-%02d' % (hour, minute, second)
                frame_path = Path(frame_dir, frame_title)
                if not frame_path.exists():
                    cv2.imwrite(str(frame_path), frame)
            yield Frame(frame)
            time += step * 1000
            self.video.set(cv2.CAP_PROP_POS_MSEC, time)
            (capture_success, frame) = self.video.read()

    def frames_by_grab(self, start: int = 0, end: int = None, step: int = 10,
                    save: bool = False):
        print(self.path.parts[-2])
        self.video.set(cv2.CAP_PROP_POS_MSEC, start)
        prev_time = start
        while True:
            if self.video.grab():
                time = self.video.get(cv2.CAP_PROP_POS_MSEC) // (step * 1000)
                if end and time > (end / step):
                    break
                hour = int(time * step // 60 // 60)
                minute = int(time * step // 60 % 60)
                second = int(time * step % 60)
                print('\t%02d:%02d:%02d' % (hour, minute, second), end='\r')
                if time > prev_time:
                    (capture_success, frame) = self.video.retrieve()
                    if save:
                        root_dir = Path(__file__).resolve().parents[1]
                        video_title = self.path.parts[-2]
                        frame_dir = Path(root_dir, 'Frames', video_title)
                        frame_dir.mkdir(parents=True, exists_ok=True)
                        frame_title = '%02d-%02d-%02d' % (hour, minute, second)
                        frame_path = Path(frame_dir, frame_title)
                        if not frame_path.exists():
                            cv2.imwrite(str(frame_path), frame)
                    yield Frame(frame)
                prev_time = time
            else:
                break

    def benchmark(self, step_low: int = 1, step_high: int = 10):
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
        for test_step in range(step_low, step_high):
            results.append({
                fieldnames[0] : test_step,
                fieldnames[1] : by_time(test_step),
                fieldnames[2] : by_grab(test_step)
            })

        source_dir = Path(__file__).parents[0]
        test_path = Path(source_dir, 'Tests', 'FrameExtractor',
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
                self.tuning_threshold = int(item['test_step'])
                break

        return results


if __name__ == '__main__':
    root_dir = Path(__file__).resolve().parents[1]
    video = Path(root_dir, 'Videos', 'Politics', 'Tim Pool',
                      '[2021-03-04] Ebay Just NUKED Dr. Seuss Books As OFFENSIVE, RSBN Gets Nuked By Youtube As Censorship Escalates',
                      '[video] amAHRY2neAg.mp4')
    g = FrameExtractor(video)
    start = time.time()
    for frame in g.frames(step=5):
        pass
    end = time.time()
    print(end - start)
