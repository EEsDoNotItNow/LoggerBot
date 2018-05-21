
import asyncio
import requests
import re
import datetime

from ..Log import Log

class e621:

    headers_base = {'User-agent': 'LoggerBot/1.0 (my EEsDoNotItNow on e621)'}
    last_request = datetime.datetime.now()

    def __init__(self, url):
        match_post = re.match(r"https://e621.net/post/show/\d+", url)
        match_pool = re.match(r"https://e621.net/pool/show/\d+", url)

        if match_post:
            self.url = match_post.group(0)+ ".json" if not url.endswith(".json") else ""
            self.type = "post"
        elif match_pool:
            self.url = match_pool.group(0)+ ".json" if not url.endswith(".json") else ""
            self.type = "pool"
        else:
            raise ValueError(f"Invalid url: {url}")
        self.urls = []


    def __str__(self):
        return self.url


    async def process(self):
        """Run me, then check self.urls for your urls to download!
        """
        headers = self.headers_base
        while datetime.datetime.now() - e621.last_request < datetime.timedelta(seconds=1):
            self.log.warning("Exceeding API limits, delaying.")
            await asyncio.sleep(1)
        e621.last_request = datetime.datetime.now()
        r = requests.get(self.url, headers=headers)
        r.raise_for_status()
        data = r.json()

        if self.type == 'post':
            self.urls.append(data['file_url'])
        elif self.type == 'pool':
            for post in data['posts']:
                self.urls.append(post['file_url'])
        else:
            raise ValueError(f"Unknown type: {self.type}")

        return

