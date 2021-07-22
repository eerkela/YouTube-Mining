import shutil

from YouTube import *


def rename_files(channel_path):
    new_path = Path(channel_path.parent, format_filename(channel_path.name))
    channel_path = channel_path.rename(new_path)
    for video_path in channel_path.iterdir():
        if video_path.is_dir():
            date = video_path.name[1:11]
            try:
                with Path(video_path, "info.json").open("r") as f:
                    info = json.load(f)
            except:
                continue
            title = format_filename(info["title"])
            new_path = Path(video_path.parent, "[%s] %s" % (date, title))
            try:
                video_path = video_path.rename(new_path)
            except OSError: # path already exists (multiple streams)
                video_path = new_path
            id = re.split("v=", info["url"])[-1]
            stream_path = Path(video_path, id)
            stream_path.mkdir(exist_ok = True)
            for file in video_path.iterdir():
                if file.suffix == ".mp4":
                    if re.match("^\[audio\]", file.name):
                        file.replace(Path(stream_path, "audio.mp4"))
                    elif re.match("^\[video\]", file.name):
                        file.replace(Path(stream_path, "video.mp4"))
                    else:
                        file.replace(Path(stream_path, "combined.mp4"))
                elif not file.is_dir():
                    file.replace(Path(stream_path,
                                     "%s_old%s" % (file.stem, file.suffix)))

    return channel_path

def fix_off_by_one_day(channel_path: Path):
    titles = []
    dates = []
    paths = []
    for video in channel_path.iterdir():
        if video.is_dir():
            date = datetime.strptime(video.name[1:11], "%Y-%m-%d")
            title = re.sub("^\[[0-9-]+\] ", "", video.name)
            if title in titles:
                index = titles.index(title)
                if abs(date - dates[index]).days <= 1:
                    t1 = datetime.fromtimestamp(video.stat().st_mtime)
                    t2 = datetime.fromtimestamp(paths[index].stat().st_mtime)
                    if t1 > t2: # this path is more recent, move other here
                        shutil.copytree(paths[index], video,
                                        dirs_exist_ok = True)
                        shutil.rmtree(paths[index])
                    else: # do the reverse
                        shutil.copytree(video, paths[index],
                                        dirs_exist_ok = True)
                        shutil.rmtree(video)
            titles.append(title)
            dates.append(date)
            paths.append(video)


def fix(channel_path, channel_id):
    if not channel_path.exists():
        channel_path.mkdir()
    starting_videos = len([p for p in channel_path.iterdir() if p.is_dir()])
    print("Starting with %s videos..." % starting_videos)

    channel_path = rename_files(channel_path)
    c = Channel(channel_id)
    for v in tqdm(c.videos, leave = False):
        v.download(dry_run = True, verbose = False)
    fix_off_by_one_day(channel_path)

    ending_videos = len([p for p in channel_path.iterdir() if p.is_dir()])
    print("Ending with %s videos..." % ending_videos)
    if ending_videos != starting_videos:
        print("%s differences" % abs(ending_videos - starting_videos))

    downloaded = [v.is_downloaded() for v in c.videos]
    denominator = len(downloaded) if len(downloaded) > 0 else 1
    percent_downloaded = sum(downloaded) / denominator * 100
    print("%2.1f%% downloaded" % percent_downloaded)

def find_corrupt_videos(channel: Path):
    results = []
    for video in channel.iterdir():
        if video.is_dir():
            for file in video.rglob("*.mp4"):
                try:
                    duration(file)
                except:
                    results.append(file)
    return results


if __name__ == "__main__":
    for (k, v) in list(CHANNELS["Politics"].items())[72:]:
        c = pytube.Channel("https://youtube.com/channel/%s" % v)
        print(c.channel_name)
        if k == "#WalkAway Campaign":
            p = Path(ROOT_DIR, "Videos", "Politics", "WalkAway Campaign")
        else:
            try:
                p = Path(ROOT_DIR, "Videos", "Politics", c.channel_name)
            except:
                continue
        fix(p, v)
        print()

    # test_channel_path = Path(ROOT_DIR, "Videos", "Politics", "Shoe0nHead")
    # test_channel_id = "UC0aanx5rpr7D1M7KCFYzrLQ"
    # fix(test_channel_path, test_channel_id)

    # politics = Path(ROOT_DIR, "Videos", "Politics")
    # for channel in politics.iterdir():
    #     if channel.is_dir():
    #         for f in find_corrupt_videos(channel):
    #             f.unlink()
