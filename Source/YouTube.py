# built-ins
import csv
import json
import os
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from socket import gaierror
from urllib.error import URLError

# dependencies
import ffmpeg
import pytube
from tqdm import tqdm

# internal
#from decorators import *


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
        print(f"Error parsing duration for path: {input_file}")
        raise

def format_filename(string, remove = "[^-_(),' a-zA-Z0-9]", char_lim = 200):
    name = re.sub(remove, "", string)
    name = re.sub("\s+", " ", name).strip("-_, ")
    return name[:char_lim]

def get_local_channels():
    parent_dir = Path(ROOT_DIR, "Videos")
    for channel in parent_dir.iterdir():
        info = [_ for _ in channel.glob("info.json")]
        if channel.is_dir() and len(info) > 0:
            yield Channel(channel)


class Channel:

    def __init__(self, config, write_info=False):
        self.fetched_at = config["fetched_at"]
        self.name = config["name"]
        self.formatted_name = config["formatted_name"]
        self.id = config["id"]
        self.about_html = config["about_html"]
        self.community_html = config["community_html"]
        self.featured_channels_html = config["featured_channels_html"]
        self.videos_html = config["videos_html"]
        self.videos = config["videos"]
        self.total_videos = config["total_videos"]

        self.target_dir = Path(ROOT_DIR, "Videos", self.formatted_name)
        if write_info:
            self.target_dir.mkdir(parents=True, exist_ok=True)
            with Path(self.target_dir, "info.json").open("w") as outfile:
                json.dump(self.flatten(), outfile)

    @classmethod
    def from_local(cls, path):
        """Factory method. Returns a Channel object from the data cached
        locally in the given path's info.json file.

        Args:
            path (Path-like): path to construct video from.  Should contain
                an info.json file.

        Raises:
            ValueError: path does not exist or has no info.json file
        """
        if not path.exists():
            raise ValueError(f"Channel directory not found: {path}")
        if not Path(path, "info.json").exists():
            raise ValueError(f"Channel has no info.json file: {path}")

        video_paths = []
        for title_dir in path.iterdir():
            if title_dir.is_dir():
                for stream_dir in title_dir.iterdir():
                    info = [p for p in stream_dir.glob("info.json")]
                    if stream_dir.is_dir() and len(info) > 0:
                        video_paths.append(stream_dir)
        video_generator = VideoGenerator(video_paths, local=True)

        with Path(path, "info.json").open("r") as infile:
            saved = json.load(infile)
            config_dict = {
                "fetched_at": datetime.fromisoformat(saved["fetched_at"]),
                "name": saved["name"],
                "formatted_name": saved["formatted_name"],
                "id": saved["id"],
                "about_html": saved["about_html"],
                "community_html": saved["community_html"],
                "featured_channels_html": saved["featured_channels_html"],
                "videos_html": saved["videos_html"],
                "videos": video_generator,
                "total_videos": len(video_generator)
            }
        return cls(config_dict, write_info=False)

    @classmethod
    def from_pytube(cls, id):
        """Factory method. Returns a Channel object from a pytube query.

        Args:
            id (str): id of channel. This usually fits the formula
                'https://www.youtube.com/channel/{id}', but it can be found
                easily with third party tools if it's not immediately obvious.
        """
        c = pytube.Channel(f"https://www.youtube.com/channel/{id}")
        video_urls = [url for url in tqdm(c.url_generator(), leave=False)]
        video_generator = VideoGenerator(video_urls, local=False)

        config_dict = {
            "_is_local": False,
            "fetched_at": datetime.now(),
            "name": c.channel_name,
            "formatted_name": format_filename(c.channel_name),
            "id": id,
            "about_html": c.about_html,
            "community_html": c.community_html,
            "featured_channels_html": c.featured_channels_html,
            "videos_html": c.html,
            "videos": video_generator,
            "total_videos": len(video_generator)
        }
        return cls(config_dict, write_info=True)

    def download(self):
        with ThreadPoolExecutor(max_workers=None) as exec:
            try:
                for video in self.videos:
                    exec.submit(video.download)
            except (KeyboardInterrupt, SystemExit):
                exec.shutdown()
                return

    def flatten(self):
        """Returns a json serializable dictionary encapsulating the class"""
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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_traceback):
        return None

    def __len__(self):
        return self.total_videos

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return (
            f"{self.name}\n"
            f"{self.fetched_at}\n"
            f"{self.total_videos}"
        )


class Video:


    def __init__(self, config):
        self._is_downloaded = None

        self.fetched_at = config["fetched_at"]
        self.url = config["url"]
        self.id = config["id"]
        self.title = config["title"]
        self.formatted_title = config["formatted_title"]
        self.publish_date = config["publish_date"]
        self.length = config["length"]
        self.channel = config["channel"]
        self.description = config["description"]
        self.keywords = config["keywords"]
        self.thumbnail_url = config["thumbnail_url"]
        self.views = config["views"]
        self.rating = config["rating"]
        self.captions = config["captions"]
        self.streams = config["streams"]

        slug = f"[{self.publish_date.date()}] {self.formatted_title}"
        self.target_dir = Path(ROOT_DIR, "Videos",
                               self.channel["formatted_name"], slug, self.id)

    @classmethod
    def from_local(cls, path):
        if not path.exists() or not path.is_dir():
            raise Exception(f"Directory does not exist: {path}")
        if not Path(path, "info.json").exists():
            raise Exception(f"Directory has no info.json file: {path}")

        with Path(path, "info.json").open("r") as infile:
            saved = json.load(infile)
            config_dict = {
                "fetched_at": datetime.fromisoformat(saved["fetched_at"]),
                "url": saved["url"],
                "id": saved["id"],
                "title": saved["title"],
                "formatted_title": saved["formatted_title"],
                "publish_date": datetime.fromisoformat(saved["publish_date"]),
                "length": timedelta(seconds=saved["length"]),
                "channel": {
                    "name" : saved["channel"]["name"],
                    "formatted_name": saved["channel"]["formatted_name"],
                    "id": saved["channel"]["id"],
                    "url": saved["channel"]["url"]
                },
                "description": saved["description"],
                "keywords": saved["keywords"],
                "thumbnail_url": saved["thumbnail_url"],
                "views": saved["views"],
                "rating": saved["rating"],
                "captions": None,
                "streams": None
            }
        return cls(config_dict)

    @classmethod
    def from_pytube(cls, url):
        v = pytube.YouTube(url)
        config_dict = {
            "fetched_at": datetime.now(),
            "url": url,
            "id": re.split("v=", url)[-1],
            "title": v.title,
            "formatted_title": format_filename(v.title),
            "publish_date": v.publish_date,
            "length": timedelta(seconds=v.length),
            "channel": {
                "name": v.author,
                "formatted_name": format_filename(v.author),
                "id": v.channel_id,
                "url": v.channel_url
            },
            "description": v.description,
            "keywords": v.keywords,
            "thumbnail_url": v.thumbnail_url,
            "views": v.views,
            "rating": v.rating,
            "captions": v.captions,
            "streams": v.streams
        }
        return cls(config_dict)

    def convert(self):
        """Merges audio and video into a single file via ffmpeg encoding."""
        audio_path = Path(self.target_dir, "audio.mp4")
        video_path = Path(self.target_dir, "video.mp4")
        combined_path = Path(self.target_dir, "combined.mp4")
        if not self.is_converted():
            if not self.is_downloaded():
                raise RuntimeError((f"Cannot convert video: download "
                                    f"incomplete ({self.target_dir})"))
            combined_path.unlink(missing_ok=True)
            video = ffmpeg.input(video_path)
            audio = ffmpeg.input(audio_path)
            out = ffmpeg.output(video, audio, combined_path)
            out.run()
            video_path.unlink()
            audio_path.unlink()
            return True
        return False

    def download(self, dry_run=False, verbose=True):
        """Downloads the highest quality audio/video streams to local storage

        Args:
            dry_run (bool): Perform full download?
            verbose (bool): Print video information to console?

        Raises:
            AttributeError: video is local (no data to download)
        """
        if self.streams is None:
            raise RuntimeError(("No valid streams to download. "
                                "Check if video is online or local"))
        if verbose:
            print(f"[{self.publish_date.date()}] {self.formatted_title}")

        self.target_dir.mkdir(parents=True, exist_ok=True)
        info_path = Path(self.target_dir, "info.json")
        stats_path = Path(self.target_dir, "stats.csv")
        audio_path = Path(self.target_dir, "audio.mp4")
        video_path = Path(self.target_dir, "video.mp4")
        captions_path = Path(self.target_dir, "captions.srt")

        # save video metadata
        with Path(self.target_dir, "info.json").open(mode="w") as outfile:
            json.dump(self.flatten(), outfile)

        # save view statistics
        row = {
            "timestamp": self.fetched_at.isoformat(),
            "views": self.views,
            "rating": self.rating
        }
        if not stats_path.exists():
            with stats_path.open(mode="w") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=list(row.keys()))
                writer.writeheader()
                writer.writerow(row)
        else:
            with stats_path.open(mode="a") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=list(row.keys()))
                writer.writerow(row)

        # download video
        if not self.is_downloaded() and not dry_run:
            self.streams.filter(adaptive=True, mime_type="video/mp4") \
                        .order_by("resolution") \
                        .desc() \
                        .first() \
                        .download(output_path=video_path.parent,
                                  filename=video_path.name)
            self.streams.filter(adaptive=True, mime_type="audio/mp4") \
                        .order_by("abr") \
                        .desc() \
                        .first() \
                        .download(output_path=audio_path.parent,
                                  filename=audio_path.name)
            if "en" in self.captions.keys():
                self.captions["en"].download(output_path=captions_path.parent,
                                             title=captions_path.name,
                                             srt=True)

    def flatten(self):
        flat = {
            "url" : self.url,
            "id" : self.id,
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

    def is_converted(self, tolerance=3):
        def validate(path):
            if not path.exists():
                return False
            dur1 = duration(path)
            dur2 = self.length.total_seconds()
            return abs(dur1 - dur2) < tolerance

        return validate(Path(self.target_dir, "combined.mp4"))

    def is_downloaded(self, tolerance=3):
        def validate(path):
            if not path.exists():
                return False
            dur1 = duration(path)
            dur2 = self.length.total_seconds()
            return abs(dur1 - dur2) <= tolerance

        audio_path = Path(self.target_dir, "audio.mp4")
        video_path = Path(self.target_dir, "video.mp4")
        combined_path = Path(self.target_dir, "combined.mp4")
        if self._is_downloaded is not None:
            return self._is_downloaded
        cond1 = validate(combined_path)
        cond2 = validate(video_path) and validate(audio_path)
        self._is_downloaded = cond1 or cond2
        return self._is_downloaded

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_traceback):
        return None

    def __len__(self):
        """Returns duration of video in seconds"""
        return self.length.total_seconds()

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return (
            f"[{self.publish_date.date()}] "
            f"{self.channel['name']}: "
            f"{self.title}"
        )


class VideoGenerator:

    def __init__(self, objs, local=False):
        self.objs = objs
        self.length = len(self.objs)
        self.local = local

    def __iter__(self):
        for obj in self.objs:
            try:
                if self.local:
                    yield Video.from_local(obj)
                else:
                    yield Video.from_pytube(obj)
            except (pytube.exceptions.VideoUnavailable, URLError, gaierror):
                continue

    def __len__(self):
        return self.length



if __name__ == "__main__":

    test_id = "UCzJXNzqz6VMHSNInQt_7q6w"
    with Channel.from_pytube(test_id) as c2:
        c2.download()

    # test_channel_id = "UCG749Dj4V2fKa143f8sE60Q"
    # c = Channel(test_channel_id)
    # for v in tqdm(c.videos, leave=False):
    #     v.download(dry_run=True)

    # for c in get_local_channels():
    #     print(c.name)
