import csv
import datetime
import isodate
import json
import os
import subprocess
from pathlib import Path

import ffmpeg
from dotenv import load_dotenv, find_dotenv
from googleapiclient.discovery import build
from pytube import YouTube, Playlist


load_dotenv(find_dotenv())
API_KEY = os.getenv('YOUTUBE_API_KEY')
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'

def format_filename(string):
    char_map = {
        '<' : '',
        '>' : '',
        ':' : '',
        '\"' : '\'',
        '/' : '-',
        '\\' : '-',
        '|' : '-',
        '?' : '',
        '*' : '',
    }
    output = ''
    for char in string:
        if char in char_map.keys():
            output += char_map[char]
        else:
            output += char
    return output

def get_duration(input_file: Path):
    cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
           '-of', 'default=noprint_wrappers=1:nokey=1', input_file]
    result = subprocess.run(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    return float(result.stdout)

def stitch(video_path: Path, audio_path: Path, output_path: Path):
    if not all([p.exists() for p in [video_path, audio_path]]):
        raise Exception('no file(s) to convert for: %s' % output_path)

    output_path.unlink(missing_ok=True)
    video = ffmpeg.input(video_path)
    audio = ffmpeg.input(audio_path)
    out = ffmpeg.output(video, audio, output_path)
    out.run()
    video_path.unlink()
    audio_path.unlink()
    return output_path


class Video:

    def __init__(self, api_response, category):
        self.fetched_at = datetime.datetime.now()
        self.id = api_response['id']
        self.url = 'https://www.youtube.com/watch?v=%s' % self.id
        self.category = category

        # parse snippet
        snippet = api_response['snippet']
        self.title = snippet['title']
        self.created_at = isodate.parse_datetime(snippet['publishedAt'])
        self.channel = {
            'name' : snippet['channelTitle'],
            'id' : snippet['channelId']
        }
        self.description = snippet['description']
        try:
            self.category_id = snippet['categoryId']
        except KeyError:
            self.category_id = None
        try:
            self.tags = snippet['tags']
        except KeyError:
            self.tags = None

        def format_thumbnail(thumbnail_dict):
            if 'maxres' in thumbnail_dict.keys():
                return thumbnail_dict['maxres']['url']
            if 'standard' in thumbnail_dict.keys():
                return thumbnail_dict['standard']['url']
            elif 'high' in thumbnail_dict.keys():
                return thumbnail_dict['high']['url']
            elif 'medium' in thumbnail_dict.keys():
                return thumbnail_dict['medium']['url']
            return thumbnail_dict['default']['url']
        self.thumbnail = format_thumbnail(snippet['thumbnails'])

        #parse contentDetails
        contentDetails = api_response['contentDetails']
        self.duration = isodate.parse_duration(contentDetails['duration'])
        try:
            self.captions_available = contentDetails['caption'] == 'true'
        except KeyError:
            self.captions_available = False

        # parse stats
        def format_statistics(stats_dict):
            try:
                views = int(stats_dict['viewCount'])
            except KeyError:
                views = None
            try:
                likes = int(stats_dict['likeCount'])
            except KeyError:
                likes = None
            try:
                dislikes = int(stats_dict['dislikeCount'])
            except KeyError:
                dislikes = None
            try:
                favorites = int(stats_dict['favoriteCount'])
            except KeyError:
                favorites = None
            return (views, likes, dislikes, favorites)
        (v, l, d, f) = format_statistics(api_response['statistics'])
        self.stats = {
            'views' : v,
            'likes' : l,
            'dislikes' : d,
            'favorites' : f
        }

        # get target directory
        root_dir = Path(__file__).resolve().parents[1]
        local_title = '[%s] %s' % (str(self.created_at.date()), self.title)
        local_title = format_filename(local_title)
        self.target_dir = Path(root_dir, 'Videos', self.category,
                               self.channel['name'], local_title)

    def download(self, convert=True):
        print(self.__str__())
        self.save_info()
        self.save_stats()

        if not self.is_downloaded():
            yt = YouTube(self.url)

            # Download video
            self.target_dir.mkdir(parents=True, exist_ok=True)
            video_path = Path(yt.streams \
                .filter(adaptive=True, mime_type='video/mp4') \
                .order_by('resolution') \
                .desc() \
                .first() \
                .download(output_path = self.target_dir,
                          filename = '[video] %s' % self.id))

            # Download audio
            audio_path = Path(yt.streams \
                .filter(adaptive=True, mime_type='audio/mp4') \
                .order_by('abr') \
                .desc() \
                .first() \
                .download(output_path = self.target_dir,
                          filename = '[audio] %s' % self.id))

            # Download captions
            if self.captions_available:
                if ('en' in yt.captions.keys()):
                    track = yt.captions['en']
                    track.download(output_path = self.target_dir,
                                   title = '[captions] %s' % self.id,
                                   srt=True)

            # concatenate audio/video:
            if convert:
                output_path = Path(self.target_dir, '%s.mp4' % self.id)
                stitch(video_path, audio_path, output_path)

    def info(self):
        info = {
            'title' : self.title,
            'url' : self.url,
            'created_at' : str(self.created_at),
            'channel' : self.channel,
            'description' : self.description,
            'category_id' : self.category_id,
            'tags' : self.tags,
            'thumbnail' : self.thumbnail,
            'duration' : str(self.duration),
            'stats' : self.stats
        }
        return info

    def is_downloaded(self, tolerance=4):
        def validate(path):
            if not path.exists():
                return False
            dur1 = get_duration(path)
            dur2 = self.duration.total_seconds()
            return abs(dur1 - dur2) < tolerance

        final_path = Path(self.target_dir, '%s.mp4' % self.id)
        if validate(final_path):
            return True
        video_path = Path(self.target_dir, '[video] %s.mp4' % self.id)
        audio_path = Path(self.target_dir, '[audio] %s.mp4' % self.id)
        return validate(video_path) and validate(audio_path)

    def save_info(self):
        self.target_dir.mkdir(parents=True, exist_ok=True)
        info_path = Path(self.target_dir, 'info.json')
        with info_path.open(mode='w') as outfile:
            json.dump(self.info(), outfile)

    def save_stats(self):
        self.target_dir.mkdir(parents=True, exist_ok=True)
        fieldnames = ['timestamp', 'views', 'likes', 'dislikes', 'favorites']
        row = {
            'timestamp' : self.fetched_at.isoformat(),
            'views' : self.stats['views'],
            'likes' : self.stats['likes'],
            'dislikes' : self.stats['dislikes'],
            'favorites' : self.stats['favorites']
        }
        stats_path = Path(self.target_dir, 'stats.csv')
        if not stats_path.exists():
            with stats_path.open(mode='w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(row)
        else:
            with stats_path.open(mode='a') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerow(row)

    def __str__(self):
        return '[%s] %s' % (str(self.created_at.date()), self.title)


class Channel:

    client = None

    def __init__(self, id, category):
        self.category = category

        if not Channel.client:
            Channel.client = build(API_SERVICE_NAME, API_VERSION,
                                developerKey=API_KEY,
                                cache_discovery=False)

        channel_request = Channel.client.channels().list(
            part = 'snippet,contentDetails',
            id = id
        ).execute()

        # parse snippet
        snippet = channel_request['items'][0]['snippet']
        self.name = snippet['title']
        self.description = snippet['description']
        self.created_at = snippet['publishedAt']

        # parse contentDetails
        contentDetails = channel_request['items'][0]['contentDetails']
        self.upload_playlist = contentDetails['relatedPlaylists']['uploads']

        # get contents
        self.videos = None
        self.complete = False

    def uploads(self, depth=None):
        # handle caching
        if self.videos:
            if depth and len(self.videos) >= depth:
                return self.videos[:depth]
            elif self.complete:
                return self.videos

        # get channel contents from api
        responses = []
        next_page_token = None
        while True:
            playlist_request = Channel.client.playlistItems().list(
                playlistId = self.upload_playlist,
                part = 'snippet,contentDetails',
                maxResults = 50,
                pageToken = next_page_token
            ).execute()

            ids = ','.join(map(lambda x: x['snippet']['resourceId']['videoId'],
                               playlist_request['items']))
            video_request = Channel.client.videos().list(
                id = ids,
                part = 'snippet,statistics,contentDetails'
            ).execute()
            responses.extend(video_request['items'])
            if depth and len(responses) >= depth:
                responses = responses[:depth]
                break

            next_page_token = playlist_request.get('nextPageToken')
            if next_page_token is None:
                break

        # assign cache
        self.videos = [Video(r, self.category) for r in responses]
        if not depth:
            self.complete = True
        return self.videos

    def info(self):
        info = {
            'name' : self.name,
            'description' : self.description,
            'created_at' : self.created_at
        }
        return info

    def undownloaded(self, depth=None):
        if self.videos:
            if depth and len(self.videos) >= depth:
                return [v for v in self.videos[:depth]
                                if not v.is_downloaded()]
            elif self.complete:
                return [v for v in self.videos if not v.is_downloaded()]

        videos = self.uploads(depth=depth)
        return [v for v in videos if not v.is_downloaded()]
