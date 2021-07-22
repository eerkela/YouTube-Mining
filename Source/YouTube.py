# built-ins
import csv
import itertools
import json
import os
import re
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

# dependencies
import ffmpeg
import pytube
from dotenv import load_dotenv, find_dotenv
from tqdm import tqdm

# internal
# from DBUtils import format_filename, duration, stitch


load_dotenv(find_dotenv())
API_KEY = os.getenv('YOUTUBE_API_KEY')
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'
ROOT_DIR = Path(__file__).resolve().parents[1]
with Path(ROOT_DIR, "Lists", "channels.json").open() as f:
    CHANNELS = json.load(f)


def duration(input_file: Path):
    cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
           '-of', 'default=noprint_wrappers=1:nokey=1', input_file]
    result = subprocess.run(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    try:
        return float(result.stdout)
    except:
        print("Error parsing duration for path: %s" % input_file)
        raise

def format_filename(string, remove = "[^-_(),' a-zA-Z0-9]", char_lim = 200):
    name = re.sub(remove, "", string)
    name = re.sub("\s+", " ", name).strip("-_, ")
    return name[:char_lim]

def get_local_channels():
    parent_dir = Path(ROOT_DIR, "Videos", "Politics")
    for channel in parent_dir.iterdir():
        info = [_ for _ in channel.glob("info.json")]
        if channel.is_dir() and len(info) > 0:
            yield Channel(channel)


class Channel:

    def __init__(self, id):
        if not (isinstance(id, Path) or isinstance(id, str)):
            raise Exception("id cannot be parsed: %s" % id)

        self._is_local = isinstance(id, Path)
        if self._is_local:
            if not id.exists():
                raise Exception("Channel directory not found: %s" % id)
            if not Path(id, "info.json").exists():
                raise Exception("Channel has no info.json file: %s" % id)
            self.target_dir = id
            self.channel_info = Path(self.target_dir, "info.json")
            with self.channel_info.open("r") as f:
                sav = json.load(f)
                self.fetched_at = datetime.fromisoformat(sav["fetched_at"])
                self.name = sav["name"]
                self.formatted_name = sav["formatted_name"]
                self.id = sav["id"]
                self.about_html = sav["about_html"]
                self.community_html = sav["community_html"]
                self.featured_channels_html = sav["featured_channels_html"]
                self.videos_html = sav["videos_html"]
            paths = []
            for title in self.target_dir.iterdir():
                if title.is_dir():
                    for stream in title.iterdir():
                        info = [_ for _ in stream.glob("info.json")]
                        if stream.is_dir() and len(info) > 0:
                            paths.append(stream)
            self.videos = VideoGenerator(paths)
        else:
            self.id = id
            c = pytube.Channel("https://www.youtube.com/channel/%s" % self.id)
            self.fetched_at = datetime.now()
            self.name = c.channel_name
            self.formatted_name = format_filename(self.name)
            self.about_html = c.about_html
            self.community_html = c.community_html
            self.featured_channels_html = c.featured_channels_html
            self.videos_html = c.html

            # def get_target_dir():
            #     # recursively search CHANNELS for relevant category(ies)
            #     q = [([], CHANNELS)]
            #     while q:
            #         (keys, this_v) = q.pop()
            #         if this_v == self.id:
            #             return Path(ROOT_DIR, "Videos", *keys[:-1],
            #                         self.formatted_name)
            #         if isinstance(this_v, dict):
            #             for (k, v) in this_v.items():
            #                 q.append((keys + [k], v))
            #     return Path(ROOT_DIR, "Videos", self.formatted_name)

            self.target_dir = Path(ROOT_DIR, "Videos", "Politics",
                                   self.formatted_name)
            self.channel_info = Path(self.target_dir, "info.json")
            urls = [url for url in tqdm(c.url_generator(), leave = False)]
            self.videos = VideoGenerator(urls)
            self.target_dir.mkdir(parents = True, exist_ok = True)
            with self.channel_info.open("w") as f:
                json.dump(self.flatten(), f)

    def flatten(self):
        flat = {
            "fetched_at" : self.fetched_at.isoformat(),
            "name" : self.name,
            "formatted_name" : self.formatted_name,
            "id" : self.id,
            "about_html" : self.about_html,
            "community_html" : self.community_html,
            "featured_channels_html" : self.featured_channels_html,
            "videos_html" : self.videos_html,
            "total_videos" : len(self.videos)
        }
        return flat


class Video:

    def __init__(self, id):
        self._is_local = isinstance(id, Path)
        self._is_downloaded = None

        if self._is_local:
            if not id.exists() or not id.is_dir():
                raise Exception("Directory does not exist: %s" % id)
            if len([_ for _ in id.glob("info.json")]) == 0:
                raise Exception("Directory has no info.json file: %s" % id)
            self.stream_dir = id
            self.target_dir = self.stream_dir.parent
            with Path(self.stream_dir, "info.json").open("r") as f:
                s = json.load(f)
                self.url = s["url"]
                self.video_id = s["id"]
                self.fetched_at = datetime.fromisoformat(s["fetched_at"])
                self.title = s["title"]
                self.formatted_title = s["formatted_title"]
                self.publish_date = datetime.fromisoformat(s["publish_date"])
                self.length = timedelta(seconds = s["length"])
                self.channel = s["channel"]
                self.description = s["description"]
                self.keywords = s["keywords"]
                self.thumbnail_url = s["thumbnail_url"]
                self.views = s["views"]
                self.rating = s["rating"]
        else:
            pytube_video = pytube.YouTube(id)
            self.url = id
            self.video_id = re.split("v=", self.url)[-1]
            self.fetched_at = datetime.now()
            self.title = pytube_video.title
            self.formatted_title = format_filename(self.title)
            self.publish_date = pytube_video.publish_date
            self.length = timedelta(seconds = pytube_video.length)
            self.channel = {
                "name" : pytube_video.author,
                "formatted_name" : format_filename(pytube_video.author),
                "id" : pytube_video.channel_id,
                "url" : pytube_video.channel_url
            }
            self.description = pytube_video.description
            self.keywords = pytube_video.keywords
            self.thumbnail_url = pytube_video.thumbnail_url
            self.views = pytube_video.views
            self.rating = pytube_video.rating

            # pytube objects
            self.captions = pytube_video.captions
            self.streams = pytube_video.streams

            # def get_target_dir():
            #     # recursively search CHANNELS for relevant category(ies)
            #     slug = "[%s] %s" % (self.publish_date.date(),
            #                         self.formatted_title)
            #     q = [([], CHANNELS)]
            #     while q:
            #         (keys, this_v) = q.pop()
            #         if this_v == self.channel["id"]:
            #             return Path(ROOT_DIR, "Videos", *keys[:-1],
            #                         self.channel["formatted_name"], slug)
            #         if isinstance(this_v, dict):
            #             for (k, v) in this_v.items():
            #                 q.append((keys + [k], v))
            #     return Path(ROOT_DIR, "Videos",
            #                 self.channel["formatted_name"], slug)

            slug = "[%s] %s" % (self.publish_date.date(), self.formatted_title)
            self.target_dir = Path(ROOT_DIR, "Videos", "Politics",
                                   self.channel["formatted_name"], slug)
            self.stream_dir = Path(self.target_dir, self.video_id)

        self.info_path = Path(self.stream_dir, "info.json")
        self.stats_path = Path(self.stream_dir, "stats.csv")
        self.video_path = Path(self.stream_dir, "video.mp4")
        self.audio_path = Path(self.stream_dir, "audio.mp4")
        self.combined_path = Path(self.stream_dir, "combined.mp4")
        self.captions_path = Path(self.target_dir, "captions.srt")

    def convert(self):
        if not self.is_converted():
            if not self.is_downloaded():
                raise Exception("Cannot convert: Video not or only partially \
                                 downloaded (%s)" % self.target_dir)
            self.combined_path.unlink(missing_ok=True)
            video = ffmpeg.input(self.video_path)
            audio = ffmpeg.input(self.audio_path)
            out = ffmpeg.output(video, audio, self.combined_path)
            out.run()
            self.video_path.unlink()
            self.audio_path.unlink()
            return True
        return False

    def download(self, dry_run = False, verbose = True):
        if self._is_local:
            raise Exception("Can't download a local video")
        if verbose:
            print("[%s] %s" % (self.publish_date.date(), self.formatted_title))

        # save video metadata
        self.stream_dir.mkdir(parents = True, exist_ok = True)
        with self.info_path.open(mode = 'w') as outfile:
            json.dump(self.flatten(), outfile)

        # save view statistics
        row = {
            "timestamp" : self.fetched_at.isoformat(),
            "views" : self.views,
            "rating" : self.rating
        }
        if not self.stats_path.exists():
            with self.stats_path.open(mode = 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames = list(row.keys()))
                writer.writeheader()
                writer.writerow(row)
        else:
            with self.stats_path.open(mode = 'a') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames = list(row.keys()))
                writer.writerow(row)

        # download video
        if not self.is_downloaded() and not dry_run:
            self.streams.filter(adaptive = True, mime_type = 'video/mp4') \
                        .order_by('resolution') \
                        .desc() \
                        .first() \
                        .download(output_path = self.target_dir,
                                  filename = self.video_path.stem)
            self.streams.filter(adaptive = True, mime_type = 'audio/mp4') \
                        .order_by('abr') \
                        .desc() \
                        .first() \
                        .download(output_path = self.target_dir,
                                  filename = self.audio_path.stem)
            if "en" in self.captions.keys():
                self.captions["en"].download(output_path = self.target_dir,
                                   title = self.captions_path.stem,
                                   srt=True)

    def flatten(self):
        flat = {
            "url" : self.url,
            "id" : self.video_id,
            "fetched_at" : self.fetched_at.isoformat(),
            "title" : self.title,
            "formatted_title" : self.formatted_title,
            "publish_date" : self.publish_date.isoformat(),
            "length" : self.length.total_seconds(),
            "channel" : self.channel,
            "description" : self.description,
            "keywords" : self.keywords,
            "thumbnail_url" : self.thumbnail_url,
            "views" : self.views,
            "rating" : self.rating
        }
        return flat

    def is_converted(self, tolerance = 3):
        def validate(path):
            if not path.exists():
                return False
            dur1 = duration(path)
            dur2 = self.length.total_seconds()
            return abs(dur1 - dur2) < tolerance

        return validate(self.combined_path)

    def is_downloaded(self, tolerance = 3):
        def validate(path):
            if not path.exists():
                return False
            dur1 = duration(path)
            dur2 = self.length.total_seconds()
            return abs(dur1 - dur2) <= tolerance

        if self._is_downloaded is not None:
            return self._is_downloaded
        cond1 = validate(self.combined_path)
        cond2 = validate(self.video_path) and validate(self.audio_path)
        self._is_downloaded = cond1 or cond2
        return self._is_downloaded

    def __str__(self):
        return "[%s] %s: %s" % (self.publish_date.date(),
                                self.channel["name"],
                                self.title)


class VideoGenerator:

    def __init__(self, ids):
        self.ids = ids
        self.length = len(self.ids)

    def __iter__(self):
        for id in self.ids:
            try:
                yield Video(id)
            except pytube.exceptions.VideoUnavailable:
                continue

    def __len__(self):
        return self.length


if __name__ == "__main__":

    test_channel_id = Path(ROOT_DIR, "Videos", "Politics", "Tim Pool")
    c = Channel(test_channel_id)
    for v in tqdm(c.videos, leave = False):
        print(v.title)

    for c in get_local_channels():
        print(c.name)
