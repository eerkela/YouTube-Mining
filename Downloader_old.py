import csv
import datetime
import isodate
import json
import logging
import os
import subprocess
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

import ffmpeg
from dotenv import load_dotenv
from googleapiclient.discovery import build
from pytube import YouTube, Playlist


load_dotenv()
API_KEY = os.getenv('YOUTUBE_API_KEY')
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'

def setup_logging():
    todays_date = datetime.datetime.today().strftime('%Y-%m-%d')
    log_dir = os.path.join('Logs', todays_date)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logging.basicConfig(
        filename = os.path.join(log_dir, 'Downloader.log'),
        level = logging.WARNING,
        format = '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
        datefmt = '%H:%M:%S'
    )

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

def get_duration(input_file):
    cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
           '-of', 'default=noprint_wrappers=1:nokey=1', input_file]
    result = subprocess.run(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    return float(result.stdout)

def stitch(video_path, audio_path, output_path):
    if os.path.exists(output_path):
        os.remove(output_path)
    video = ffmpeg.input(video_path)
    audio = ffmpeg.input(audio_path)
    out = ffmpeg.output(video, audio, output_path)
    out.run()
    os.remove(video_path)
    os.remove(audio_path)
    return output_path


class Video:

    def __init__(self, response, category, save=True):
        def format_thumbnail(thumbnail_dict):
            if 'standard' in thumbnail_dict.keys():
                return thumbnail_dict['standard']['url']
            elif 'high' in thumbnail_dict.keys():
                return thumbnail_dict['high']['url']
            elif 'medium' in thumbnail_dict.keys():
                return thumbnail_dict['medium']['url']
            return thumbnail_dict['default']['url']

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

        self.fetched_at = datetime.datetime.now()
        self.category = category

        self.id = response['id']
        self.title = response['snippet']['title']
        self.description = response['snippet']['description']
        self.url = 'https://www.youtube.com/watch?v=%s' % response['id']
        self.channel = {
            'name' : response['snippet']['channelTitle'],
            'id' : response['snippet']['channelId']
        }
        self.duration = isodate.parse_duration(
            response['contentDetails']['duration'])
        self.created_at = isodate.parse_datetime(
            response['snippet']['publishedAt'])
        self.thumbnail = format_thumbnail(response['snippet']['thumbnails'])
        (v, l, d, f) = format_statistics(response['statistics'])
        self.statistics = {
            'views' : v,
            'likes' : l,
            'dislikes' : d,
            'favorites' : f
        }
        try:
            self.tags = response['snippet']['tags']
        except KeyError:
            self.tags = []

        # Update metadata
        if save:
            download_dir = self.get_download_dir()
            metadata_path = os.path.join(download_dir, 'metadata.json')
            if not os.path.exists(metadata_path):
                self.save_metadata()
            else:   # temporary overwrite
                self.save_metadata()

            # update statistics:
            self.save_statistics()

    def get_download_dir(self):
        local_title = '[%s] %s' % (str(self.created_at.date()), self.title)
        local_title = format_filename(local_title)
        download_dir = os.path.join(self.category,
                                  self.channel['name'],
                                  local_title)
        return download_dir

    def download(self, convert=True):
        print('\t[%s] %s' % (str(self.created_at.date()), self.title))
        download_dir = self.get_download_dir()
        if not self.is_converted():
            if not self.is_downloaded():
                if not os.path.exists(download_dir):
                    os.makedirs(download_dir)
                try:
                    yt = YouTube(self.url)

                    # Download video
                    yt.streams \
                        .filter(adaptive=True, mime_type='video/mp4') \
                        .order_by('resolution') \
                        .desc() \
                        .first() \
                        .download(output_path=download_dir,
                                  filename=self.id,
                                  filename_prefix='[video] ')

                    # Download audio
                    yt.streams \
                        .filter(adaptive=True, mime_type='audio/mp4') \
                        .order_by('abr') \
                        .desc() \
                        .first() \
                        .download(output_path=download_dir,
                                  filename=self.id,
                                  filename_prefix='[audio] ')

                    # Download captions
                    if ('en' in yt.captions.keys()):
                        track = yt.captions['en']
                        track.download(output_path=download_dir,
                                       title=self.id,
                                       filename_prefix='[captions] ',
                                       srt=True)
                except Exception as e:
                    logging.error(traceback.format_exc())
                    pass

            # concatenate audio/video:
            if convert:
                try:
                    self.stitch()
                except Exception as e:
                    logging.error(traceback.format_exc())
                    pass

    def stitch(self):
        download_dir = self.get_download_dir()
        video_path = os.path.join(download_dir, '[video] %s.mp4' % self.id)
        audio_path = os.path.join(download_dir, '[audio] %s.mp4' % self.id)
        output_path = os.path.join(download_dir, '%s.mp4' % self.id)
        stitch(video_path, audio_path, output_path)

    def is_converted(self, tolerance=4):
        parent_dir = self.get_download_dir()
        output_path = os.path.join(parent_dir, '%s.mp4' % self.id)
        if not os.path.exists(output_path):
            return False

        try:
            o_dur = get_duration(output_path)
            converted = abs(self.duration.total_seconds() - o_dur) < tolerance
            return converted
        except Exception as e:
            os.remove(output_path)
            return False

    def is_downloaded(self, tolerance=4):
        parent_dir = self.get_download_dir()
        video_path = os.path.join(parent_dir, '[video] %s.mp4' % self.id)
        audio_path = os.path.join(parent_dir, '[audio] %s.mp4' % self.id)
        if not os.path.exists(video_path) or not os.path.exists(audio_path):
            return False

        try:
            v_dur = get_duration(video_path)
            a_dur = get_duration(audio_path)
            downloaded = (
                abs(self.duration.total_seconds() - v_dur) < tolerance
                and
                abs(self.duration.total_seconds() - a_dur) < tolerance
            )
            return downloaded
        except Exception as e:
            os.remove(video_path)
            os.remove(audio_path)
            return False

    def get_metadata(self):
        metadata = {
            'id' : self.id,
            'title' : self.title,
            'description' : self.description,
            'url' : self.url,
            'channel' : self.channel,
            'duration' : str(self.duration),
            'created at' : str(self.created_at),
            'thumbnail' : self.thumbnail,
            'tags' : self.tags
        }
        return metadata

    def get_statistics(self):
        stats = {}
        stats_path = os.path.join(self.get_download_dir(), 'stats.csv')
        with open(stats_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                timestamp = isodate.parse_datetime(row['timestamp'])
                stats[str(timestamp)] = {
                    'views' : row['views'],
                    'likes' : row['likes'],
                    'dislikes' : row['dislikes'],
                    'favorites' : row['favorites']
                }
        stats[str(self.fetched_at)] = {
            'views' : self.statistics['views'],
            'likes' : self.statistics['likes'],
            'dislikes' : self.statistics['dislikes'],
            'favorites' : self.statistics['favorites']
        }
        return stats

    def save_metadata(self):
        download_dir = self.get_download_dir()
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
        metadata = self.get_metadata()
        metadata_path = os.path.join(download_dir, 'metadata.json')
        with open(metadata_path, 'w') as outfile:
            json.dump(metadata, outfile)

    def save_statistics(self):
        download_dir = self.get_download_dir()
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
        fieldnames = ['timestamp', 'views', 'likes', 'dislikes', 'favorites']
        row = {
            'timestamp' : self.fetched_at.isoformat(),
            'views' : self.statistics['views'],
            'likes' : self.statistics['likes'],
            'dislikes' : self.statistics['dislikes'],
            'favorites' : self.statistics['favorites']
        }
        stats_path = os.path.join(download_dir, 'stats.csv')
        if not os.path.exists(stats_path):
            with open(stats_path, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(row)
        else:
            with open(stats_path, 'a') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerow(row)

    def __str__(self):
        return '[%s] %s' % (str(self.created_at.date()), self.title)


class Client:

    def __init__(self, api_key):
        self.client = build(API_SERVICE_NAME, API_VERSION,
                            developerKey=api_key,
                            cache_discovery=False)

    def get_uploads(self, channel_id, category, depth=None, save=True):
        request1 = self.client.channels().list(
            part = 'contentDetails',
            id = channel_id
        ).execute()
        upload_playlist = request1['items'][0]['contentDetails'] \
                                  ['relatedPlaylists']['uploads']

        responses = []
        next_page_token = None
        while True:
            request2 = self.client.playlistItems().list(
                playlistId = upload_playlist,
                part = 'snippet,contentDetails',
                maxResults = 50,
                pageToken = next_page_token
            ).execute()

            ids = ','.join(map(lambda x: x['snippet']['resourceId']['videoId'],
                               request2['items']))
            request3 = self.client.videos().list(
                id = ids,
                part = 'snippet, statistics, contentDetails'
            ).execute()
            responses.extend(request3['items'])
            if depth and len(responses) > depth:
                responses = responses[:depth]
                break

            next_page_token = request2.get('nextPageToken')
            if next_page_token is None:
                break

        uploads = []
        for r in responses:
            uploads.append(Video(r, category, save=save))
        return uploads

    def download_channel(self, channel_id, category, convert=True, depth=None):
        uploads = self.get_uploads(channel_id, category, depth=depth)
        for video in uploads:
            video.download(convert)


if __name__ == '__main__':
    setup_logging()

    ## Sources gathered from the Alternative Influence Report (in parent
    ## folder) + transparency.tube + personal curation.

    politics = {
        # anti-woke
        'Aaron Mate (The Grayzone)' : 'UCEXR8pRTkE2vFeJePNe9UcQ',
        'Andrew Klavan' : 'UCyhEZKz-LOwgktptEOh6_Iw',
        'Andy Ngo' : 'UCmmfVSRVwMSfHdRf6v3lPxQ',
        'Aydin Paladin' : 'UCUowFWIWGw6Pv2JqfEj8njQ',
        'Ben Shapiro' : 'UCnQC_G5Xsjhp9fEJKuIcrSw',
        'BG On The Scene' : 'UC_tuX7_FhcVvrLzlEEKNhCw',
        'Blaire White' : 'UCDmCBKaKOtOrEqgsL4-3C8Q',
        'Bret Weinstein' : 'UCi5N_uAqApEUIlg32QzkPlg',
        'Brandon Straka (WalkAway)' : 'UCeyQ0fdrXTBxK-iOq1cBrbg',
        'Carl Benjamin (Sargon of Akkad)' : 'UC-yewGHQbNFpDrGM0diZOLA',
        'Carl Benjamin (Sargon of Akkad Live)' : 'UC6cMYsKMx6XicFcFm7mTsmA',
        'Carl Benjamin (Akkad Daily)' : 'UCitU2-w3XE8ujvUZjcAnhIg',
        'Carl Benjamin (The Thinkery)' : 'UCpiCH7qvGVlzMOqy3dncA5Q',
        'Carl Benjamin (Lotus Eaters)' : 'UCnw5I-wliudW8YO1vr5O4IQ',
        'Carl Benjamin (Lotus Eaters Podcast)' : 'UC7edjYPNhTm5LYJMT7UMt0Q',
        'Campus Reform' : 'UCA8sK_Bba0Eb-k33bD0ldhw',
        'Candace Owens' : 'UCL0u5uz7KZ9q-pe-VC8TY-w',
        'Count Dankula' : 'UC7SeFWZYFmsm1tqWxfuOTPQ',
        'Count Dankula 2: Electric Boogaloo' : 'UCRLO8HU2LWaMH6mjbQ1falQ',
        'Dave Cullen (Computing Forever)' : 'UCT9D87j5W7PtE7NHOR5DUOQ',
        'Dave Rubin (Rubin Report)' : 'UCJdKr0Bgd_5saZYqLCa9mng',
        'Doctor Layman' : 'UCe9g_wsVXN17qmw8kPk4hRw',
        'Donald Trump' : 'UCAql2DyGU2un1Ei2nMYsqOA',
        'Eric Weinstein' : 'UCR85PW_B_7_Aisx5vNS7Gjw',
        'Fleccas Talks' : 'UCIpwPuJsrboNnf200oV8cWQ',
        'Gad Saad' : 'UCLH7qUqM0PLieCVaHA7RegA',
        'Glenn Beck' : 'UCvqtzdcURSqNjY9RQEK4XmQ',
        'Jimmy Dore' : 'UC3M7l8ved_rYQ45AVzS0RGA',
        'Jordan Peterson' : 'UCL_f53ZEJxp8TtlOkHwMV9Q',
        'Kaitlyn Bennett (Liberty Hangout)' : 'UCQMb7c66tJ7Si8IrWHOgAPg',
        'Karen Straughan' : 'UCcmnLu5cGUGeLy744WS-fsg',
        'laowhy86' : 'UChvithwOECK5g_19TjldMKw',
        'Lauren Chen' : 'UCLUrVTVTA3PnUFpYvpfMcpg',
        'Lauren Southern' : 'UCla6APLHX6W3FeNLc8PYuvg',
        'Liberal Hivemind' : 'UCIr4vkCsn0tdTW2xZ1jRG1g',
        'Liberty Hangout' : 'UCQMb7c66tJ7Si8IrWHOgAPg',
        'Lives Matter Show' : 'UCk-qmvpU4mOmKBOyCznE-VA',
        'Matt Christiansen' : 'UCxeY-wRrb65Jt37QHa5xMog',
        'Megyn Kelly' : 'UCzJXNzqz6VMHSNInQt_7q6w',
        'Michael Knowles' : 'UCr4kgAUTFkGIwlWSodg43QA',
        'Mike Cernovich' : 'UC87YBeLMwXhgaw5tcCxsXgQ',
        'Milo Yiannopoulos' : 'UC0aVoboXBUx2-tVIWHc3W2Q',
        'News Junkie\'s Cartoons' : 'CUa8s1GPOGlfwUeiO8zzLnQ',
        'Paul Joseph Watson' : 'UCittVh8imKanO_5KohzDbpg',
        'Project Veritas' : 'UCL9PlYkRD3Q-RZca6CCnPKw',
        'Project Veritas Action' : 'UCEE8w-v6Gg4j3ze3oX-urEw',
        'Ruptly' : 'UC5aeU5hk31cLzq_sAExLVWg',
        'Sam Harris' : 'UCNAxrHudMfdzNi6NxruKPLw',
        'SCNR' : 'UCLMSv1UJp9sfoHyo-9s6sdw',
        'serpentza' : 'UCl7mAGnY4jh4Ps8rhhh8XZg',
        'ShortFatOtaku' : 'UCVt7ujK-9TT9KByzL9g_2QQ',
        'Steven Crowder' : 'UCIveFvW-ARp_B_RckhweNJw',
        'Styxhexenhammer666' : 'UC0rZoXAD5lxgBHMsjrGwWWQ',
        'Thunderf00t' : 'UCmb8hO2ilV9vRa8cilis88A',
        'Tim Pool' : 'UCG749Dj4V2fKa143f8sE60Q',
        'Timcast' : 'UCe02lGcO-ahAURWuxAJnjdA',
        'Timcast IRL' : 'UCLwNTXWEjVd2qIHLcXxQWxA',
        'WalkAway Campaign' : 'UCDb4InP9mRZR9oogD1b2dOQ',
        'WeAreChange' : 'UChwwoeOZ3EJPobW83dgQfAg',

        # woke
        'Contrapoints' : 'UCNvsIonJdJ5E4EXMa65VYpA',
        'David Pakman' : 'UCvixJtaXuNdMPUGdOPcY8Ag',
        'Feminist Frequency' : 'UC7Edgk9RxP7Fm7vjQ1d-cDA',
        'June (Shoe0nHead)' : 'UC0aanx5rpr7D1M7KCFYzrLQ',
        'June (Brainlet)' : 'UC7UiChjgT_LDKcr_8NEEbMA',
        'Keith Olbermann (former MSNBC)' : 'UCeAACGXB76rAOON6aaY72-Q',
        'NowThis News' : 'UCn4sPeUomNGIr26bElVdDYg',
        'Secular Talk' : 'UCldfgbzNILYZA4dmDt4Cd6A',
        'Vaush' : 'UC1E-JS8L0j1Ei70D9VEFrPQ',
        'The Damage Report' : 'UCl9roQQwv4o4OuBj3FhQdDQ',
        'The Humanist Report' : 'UC7Q4rvzJDbHeBHYk5rnvZeA',
        'The Young Turks' : 'UC1yBKRuGpC1tSM73A0ZjYjQ',
        'Three Arrows' : 'UCCT8a7d6S6RJUivBgNRsiYg',
        'Vox' : 'UCLXo7UDZvByw2ixzpQCufnA'
    }

    removed = {
        'Alexandria Ocasio-Cortez' : 'UCElqfal0wzzpLsHlRuqZjaA',
        'Jacobin Magazine' : 'UCzGUT9PjV3SMBwjWXUYh4HA',
        'Russel Brand' : 'UCswH8ovgUp5Bdg-0_JTYFNw',

        'Brittany Pettibone' : 'UCesrUK_dMDBZAf7cnjQPdgQ',
        'Millenial Woes' : 'UCLfhh63n0fWn0gXXKQ5NWvw',
        'Officer Tatum' : 'UCaYw_yJ_YLPEv6zR2c7hgHA',
        'Rebel News' : 'UCGy6uV7yqGWDeUWTZzT3ZEg',
    }

    def download_channel_synchronously(id, category, convert=False, depth=None):
        try:
            yt.download_channel(id, category, convert, depth)
        except Exception as e:
            logging.error(traceback.format_exc())
            raise

    yt = Client(API_KEY)
    with ProcessPoolExecutor() as exec:
        for id in politics.values():
            exec.submit(download_channel_synchronously,
                            id, 'Politics', False, None)
        for id in politics.values():
            exec.submit(download_channel_synchronously,
                            id, 'Politics', True, None)
        exec.shutdown()
