import datetime
import json
import logging
import traceback
from concurrent.futures import ProcessPoolExecutor
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

def split_list(to_split, n=1):
    '''Splits list into n sublists of equal length'''
    length = len(to_split)
    return [to_split[i*length//n : (i+1)*length//n] for i in range(n)]


if __name__ == '__main__':
    setup_logging()

    def download_channel(id, category, convert=False, depth=None):
        try:
            c = Channel(id, category=category)
            for v in c.uploads(depth=depth):
                try:
                    v.download(convert=convert)
                except:
                    logging.error(traceback.format_exc())
        except:
            logging.error(traceback.format_exc())

    politics_path = Path('Lists', 'politics.json')
    with politics_path.open(mode='r') as in_file:
        politics = json.load(in_file)

    blocks = split_list(list(politics.values()), 5)
    block = blocks[4]
    test_id = list(politics.values())[0]
    with ProcessPoolExecutor(max_workers=None) as exec:
        for id in politics.values():
            exec.submit(download_channel, id, 'Politics', False, 50)
        for id in politics.values():
            exec.submit(download_channel, id, 'Politics', False, None)
        for id in politics.values():
            exec.submit(download_channel, id, 'Politics', True, None)
