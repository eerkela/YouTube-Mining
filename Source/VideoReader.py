import time
from pathlib import Path

import cv2
import pytesseract


class FrameGrabber:

    def __init__(self, video_path: Path):
        self.path = video_path
        self.video = cv2.VideoCapture(str(video_path))

    def frames(self, start: int = 0, end: int = None, step: int = 10,
               save: bool = False):
        # scrubbing by timestamp outperforms grabbing and discarding at a step
        # size of 4 or greater.  This can be empirically derived by running
        # the unit test in main
        if step < 4:
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
                root_dir = Path(__file__).resolve().parents[1]
                video_title = self.path.parts[-2]
                frame_dir = Path(root_dir, 'Frames', video_title)
                frame_dir.mkdir(parents=True, exists_ok=True)
                frame_title = '%02d-%02d-%02d' % (hour, minute, second)
                frame_path = Path(frame_dir, frame_title)
                if not frame_path.exists():
                    cv2.imwrite(str(frame_path), frame)
            yield (frame)
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
                    yield (frame)
                prev_time = time
            else:
                break

    def tune(self, test_video):
        pass

class VideoReader:

    def __init__(self, video_path: Path):
        self.frame_generator = FrameGenerator(video_path)

    def text(self, start: int=0, end: int=None, rate: int=10):
        frame_generator = self.frames(start, end, rate)
        for frame in frame_generator:
            # preprocess to enhance accuracy
            # convert to grayscale:
            frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            # dilate
            frame = cv2.dilate(frame, np.ones((5, 5)), iterations=1)
            # denoise
            frame = cv2.GaussianBlur(frame, )
            # threshold
            frame = cv2.threshold(frame, 0, 255,
                                  cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
            yield pytesseract.image_to_string(frame, lang='eng')

    def corpus(self, start: int=0, end: int=None, rate: int=10):
        pass


if __name__ == '__main__':
    import json

    def test_scrubbing(test_video):
        g = FrameGrabber(test_video)

        def by_time(step):
            start = time.time()
            for frame in g.frames_by_time(step=step):
                pass
            end = time.time()
            return end - start

        def by_grab(step):
            start = time.time()
            for frame in g.frames_by_grab(step=step):
                pass
            end = time.time()
            return end - start

        results = {}
        for test_step in range(1, 10):
            results[test_step] = {
                'time' : by_time(test_step),
                'grab' : by_grab(test_step)
            }
        dest = Path('Unit Tests', 'scrubbing_by_time_vs_grab_opencv.json')
        dest.parents[0].mkdir(exist_ok=True)
        with dest.open('w') as outfile:
            json.dump(results, outfile)
        return results


    root_dir = Path(__file__).resolve().parents[1]
    video = Path(root_dir, 'Videos', 'Politics', 'Tim Pool',
                      '[2021-03-04] Ebay Just NUKED Dr. Seuss Books As OFFENSIVE, RSBN Gets Nuked By Youtube As Censorship Escalates',
                      '[video] amAHRY2neAg.mp4')
    #print(json.dumps(test_scrubbing(video), indent=4))
    g = FrameGrabber(video)
    start = time.time()
    for frame in g.frames(step=10):
        pass
    end = time.time()
    print(end - start)
