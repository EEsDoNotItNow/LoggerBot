
from pathlib import Path
import requests
import re
import time

from ..Log import Log
from ..SQL import SQL


class DownloaderBase:

    base_dest = Path("~/Pictures/LoggerBot/").expanduser()
    url_file_regex = r"https?:\/\/.*\/(?P<fn>.*\.[^\/\n]*)$"
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

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
            r = requests.get(self.url, headers=self.headers)
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


    async def ScrapeUrl(self):
        self.log.info("Currently not attempting to scape URLS, just return")
        return

        self.log.info(f"Attempt to scrape url {self.url} to {self.dest}")

        response = requests.get(self.url, headers=self.headers)

        response.raise_for_status()

        # Handle e621.net links
        match_e621 = re.search(r'<a href="(?P<url>.*)">Download<\/a>', response.text)
        if match_e621 and False:
            self.url = match_e621.group('url')
            match_obj = re.search(self.url_file_regex, self.url, re.IGNORECASE)
            if match_obj:
                self.log.info("Found a valid match for e621 links, save it!")
                self.file_name = match_obj.group("fn")
                self.file_name = f"{self.message.id}_{self.file_name}"
                await self.DownloadFile()
                return
        else:
            self.log.warning(f"Unable to scrape URL: {self.url}")
