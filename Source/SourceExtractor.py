import requests
from multiprocessing import Pool

import cv2
from bs4 import BeautifulSoup
import pytesseract


def dhash(image, hash_size=8):
    resized = cv2.resize(image, hash_size + 1, hash_size)
    diff = resized[:, 1:] > resized[:, :-1]
    return sum([2 ** i for (i, v) in enumerate(diff.flatten()) if v])


class SourceExtractor:

    def __init__(self, path_to_video):
        self.path = path_to_video
        self.video = cv2.VideoCapture(path_to_video)

    def get_longest_string(self, strings):
        return max(strings, key=lambda s: len(s)).strip()

    def extract_text_by_time(self, x=None, w=None, y=None, h=None, rate=10):
        def get_frame(time):
            self.video.set(cv2.CAP_PROP_POS_MSEC, time)
            (capture_success, frame) = self.video.read()
            return (capture_success, frame)

        ignore_rows = True
        ignore_columns = True
        if y is not None and h is not None:
            ignore_rows = False
        if x is not None and w is not None:
            ignore columns = False

        contents = []
        time = 0
        (capture_success, frame) = get_frame(time)
        while capture_success:
            search = frame
            if not ignore_rows and not ignore_columns:
                search = frame[y:y+h, x:x+w]
            elif not ignore_rows and ignore_columns:
                search = frame[y:y+h, :]
            elif ignore_rows and not ignore_columns:
                search = frame[: , x:x+w]

            #cv2.imwrite('frame%.png' % (time // 1000), search)
            image_to_text = pytesseract.image_to_string(search, lang='eng')
            contents.append(image_to_text)
        return contents

    def extract_text_by_grab(self, x=None, w=None, y=None, h=None, rate=10):
        ignore_rows = True
        ignore_columns = True
        if y is not None and h is not None:
            ignore_rows = False
        if x is not None and w is not None:
            ignore columns = False

        contents = []
        prev_time = -1
        while True:
            grabbed = self.video.grab()
            if grabbed:
                time_s = self.video.get(cv2.CAP_PROP_POS_MSEC) // (rate * 1000)
                if time_s > prev_time:
                    (capture_success, frame) = video.retrieve()
                    search = frame
                    if not ignore_rows and not ignore_columns:
                        search = frame[y:y+h, x:x+w]
                    elif not ignore_rows and ignore_columns:
                        search = frame[y:y+h, :]
                    elif ignore_rows and not ignore_columns:
                        search = frame[: , x:x+w]

                    image_to_text = pytesseract.image_to_string(search,
                                                                lang='eng')
                    contents.append(image_to_text)
                prev_time = time_s
            else:
                break
        return contents

    def extract_sources(self, rate=10):
        snapshots = self.extract_text_by_time(y=0, h=32, rate=rate)
        tried = []
        output = []
        for text in snapshots:
            search = self.get_longest_string(text.split(' '))
            if not search.startswith('http'):
                search = 'http://%s' % text
            if search not in tried:
                tried.append(search)
                try:
                    request = requests.head(search)
                    if request.status_code < 400:
                        output.append(search)
                except:
                    pass
        return output

    def save_url(self, url):
        #https://programminghistorian.org/en/lessons/working-with-web-pages
        #https://docs.python.org/3/library/http.client.html#http.client.HTTPResponse.read
        #https://requests.readthedocs.io/en/latest/api/#requests.Response.content
        (download_dir, _) = os.path.split(self.path)
        response = requests.get(url)
        #title = BeautifulSoup.find('.//title').text
        #if not title:
        #    title = url
        #path = os.path.join(download_dir, title)
        path = os.path.join(download_dir, url)
        with open(path, 'wb') as f:
            f.write(response.content)

    def crawl(pages, depth=None):
        indexed_url = [] # a list for the main and sub-HTML websites in the main website
        for i in range(depth):
            for page in pages:
                if page not in indexed_url:
                    indexed_url.append(page)
                    try:
                        c = requests.get(page)
                    except:
                        print( "Could not open %s" % page)
                        continue
                    soup = BeautifulSoup(c.read())
                    links = soup('a') # finding all the sub_links
                    for link in links:
                        if 'href' in dict(link.attrs):
                            url = urljoin(page, link['href'])
                            if url.find("'") != -1:
                                    continue
                            url = url.split('#')[0]
                            if url[0:4] == 'http':
                                    indexed_url.append(url)
            pages = indexed_url
        return indexed_url


if __name__ == '__main__':
    #https://stackoverflow.com/questions/31205497/how-download-a-full-webpage-with-a-python-script
    #https://stackoverflow.com/questions/52655841/opencv-python-multithreading-seeking-within-a-videocapture-object


    path = os.path.join('Videos',
                        'Tim Pool',
                        '2020-09-04',
                        'Democrats PANICKING Over Riots Get Saved By Media, Latest Smear Of Trump So INSANE It Can\'t Be Real',
                        'MDJgz0pa0Eo.mp4')
    s = SourceExtractor(path)
    print(s.extract_sources())


    '''
    video_paths = []
    channel_path = os.path.join('Videos', 'Tim Pool')
    for (dirpath, dirnames, filenames) in os.walk(channel_path):
        for file in filenames:
            if file.endswith('.mp4') and not file.startswith('['):
                path = os.path.join(channel_path, dirpath, file)
                video_paths.append(path)

    def extract_async(path):
        t = TextExtractor(path)
        sources = t.extract_sources()
        for url in sources:
            t.save_url(url)

    pool = Pool()
    pool.map(extract_async, video_paths)
    pool.close()
    pool.join()
    '''

    '''
    path = os.path.join('Videos',
                        'Tim Pool',
                        '2020-08-30',
                        'Trump Takes MAJOR Polling Lead Against Biden, Democrats PANIC, Shift Message As Riots BACKFIRE',
                        'QUiGL0nQdMs.mp4')

    import time
    start = time.time()
    print(extract_sources(path))
    end = time.time()
    print(end - start)
    '''
