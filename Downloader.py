import datetime
import json
import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path

from Source.yt import Channel

def setup_logging():
    todays_date = datetime.datetime.today().strftime('%Y-%m-%d')
    parent_dir = Path(__file__).resolve().parent
    log_dir = Path(parent_dir, 'Logs', todays_date)
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=Path(log_dir, 'Downloader.log'),
        level=logging.WARNING,
        format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )


if __name__ == '__main__':
    setup_logging()

    ## Sources gathered from the Alternative Influence Report (in parent
    ## folder) + transparency.tube + personal curation.

    def download_channel(id, category, convert=False, range=None):
        c = Channel(id, category=category)
        for v in c.uploads(range=range):
            try:
                v.download(convert=convert)
            except:
                logging.error(traceback.format_exc())

    politics_path = Path('Lists', 'politics.json')
    with politics_path.open(mode='r') as in_file:
        politics = json.load(in_file)

    with ProcessPoolExecutor(max_workers=None) as exec:
        for id in politics.values():
            exec.submit(download_channel, id, 'Politics', False, (0, 64))
        for id in politics.values():
            exec.submit(download_channel, id, 'Politics', True, (0, 64))
