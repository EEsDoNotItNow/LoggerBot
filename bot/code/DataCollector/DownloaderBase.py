
from pathlib import Path
import requests
import re

from ..Log import Log
from ..SQL import SQL


class DownloaderBase:

    base_dest = Path("~/Pictures/LoggerBot/").expanduser()
    url_file_regex = r"https?:\/\/.*\/(?P<fn>.*\.(?:.*))$"

    def __init__(self, *args, **kwargs):
        self.saved = False
        self.url = None
        self.dest = None
        self.http_status = None
        self.file_name = None
        self.sql = SQL()
        self.log = Log()


    async def save(self, saved):
        raise NotImplementedError()


    async def process(self):
        raise NotImplementedError()


    async def DownloadFile(self):
        log = Log()
        log.info(f"Attempt to download {self.url} to {self.dest}")

        self.dest.mkdir(parents=True, exist_ok=True)

        try:
            r = requests.get(self.url)
        except requests.exceptions.ConnectionError:
            self.http_status = 599
            raise

        self.http_status = r.status_code

        try:
            r.raise_for_status()
        except Exception:
            self.log.error("Bad status on file, we are done here!")
            raise
        
        # Check to see if we need to modify our file_name
        while Path(self.dest,self.file_name).is_file():
            self.file_name = self.file_name.stem + "_copy" + self.file_name.suffix
        _file = Path(self.dest,self.file_name)
        with open(_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024): 
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
        self.saved = True


    async def ScrapeUrl(url):
        self.log.info(f"Attempt to scrape url {self.url} to {self.dest}")

        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

        response = requests.get(url, headers=headers)

        response.raise_for_status()

        # Handle e621.net links
        match_obj = re.search(r"<a href=\"(?P<url>[0-9a-zA-Z_$\-\.\+\!\*\'\(\)\,]*)\">Download</a>", response.text)
        if match_obj:
            self.url = match_obj.group('url')
            match_obj = re.search(self.url_file_regex, self.url, re.IGNORECASE)
            if match_obj:
                self.log.info("Found a valid match for e621 links, save it!")
                self.file_name = match_obj.group("fn")
                self.file_name = Path(f"{self.message.id}_{self.file_name}")
                await DownloadFile()
                return

        self.log.error(f"No match found for {url}")
        dest.mkdir(parents=True, exist_ok=True)
        _file = Path(dest, f"{url.replace('/','')}.html")
        self.log.info(f"Write to file {_file}")
        with open(_file, 'w') as f:
            f.write(f'<meta http-equiv="refresh" content="0; URL=\'{url}\'" />')
