from collections import defaultdict
from pathlib import Path
import asyncio
import re
import requests
import time

from ..Log import Log
from ..SQL import SQL

from .DownloaderBase import DownloaderBase

class Embed(DownloaderBase):

    def __init__(self, embed, message):
        super().__init__()
        self.embed = defaultdict(lambda: None)
        self.embed.update(embed)
        self.message = message
        self.channel_name = message.channel.name if message.channel else message.author.name
        self.server_name = message.server.name if message.server else "PrivateMessages"
        self.dest = Path(self.base_dest, self.server_name, self.channel_name)
        self.file_name = None
        self.proxy_url = None
        self.provider_url = None
        self.provider_name = None
        self.type = None
        self.description = None
        self.title = None


    async def save(self):
        """Save data to SQL
        """
        message_id = self.message.id
        url = self.url
        file_name = self.file_name
        saved = self.saved
        http_status = self.http_status
        dest = str(self.dest)
        proxy_url = self.proxy_url
        provider_url = self.provider_url
        provider_name = self.provider_name
        _type = self.type
        description = self.description
        title = self.title

        cmd = """
            INSERT INTO embeds 
            (
                message_id,
                url,
                proxy_url,
                provider_url,
                provider_name,
                type,
                description,
                title,
                file_name,
                saved,
                http_status,
                dest
            ) VALUES (
                :message_id,
                :url,
                :proxy_url,
                :provider_url,
                :provider_name,
                :_type,
                :description,
                :title,
                :file_name,
                :saved,
                :http_status,
                :dest
            )
            """
        self.sql.cur.execute(cmd, locals())
        await self.sql.commit()


    async def process(self):
        # Attempt to save the image
        self.log.info("Processing embed")
        self.type = self.embed['type']

        if self.embed['type'] == 'link':
            self.url = self.embed['url']
            self.provider_name = self.embed['provider']['name'] if self.embed['provider'] else None
            self.provider_url = self.embed['provider']['url'] if self.embed['provider'] else None
            self.description = self.embed['description']
            self.title = self.embed['title']
            await self.save()
            return
        elif self.embed['type'] == 'image':
            self.url = self.embed['url']
            match_obj = re.search(self.url_file_regex, self.url, re.IGNORECASE)

            if match_obj is not None:
                self.file_name = match_obj.group("fn")

            try:
                if match_obj is not None:
                    await self.DownloadFile()
                else:
                    await self.ScrapeUrl()
            except requests.exceptions.HTTPError as e:
                self.log.warning(f"Got http_status error on download: {e}")
            except Exception:
                self.log.error("Failed to save file")
                self.log.error(f"URL: {self.url}")
                self.log.error(f"File Name: {self.file_name}")
                self.log.exception("Didn't save file")
            await self.save()
            return
        elif self.embed['type'] == 'video':
            self.url = self.embed['url']

            self.provider_name = self.embed['provider']['name'] if self.embed['provider'] else None
            self.provider_url = self.embed['provider']['url'] if self.embed['provider'] else None
            self.description = self.embed['description']
            self.title = self.embed['title']

            self.log.warning("Didn't save file (Not sure what to do with videos for now)")
            await self.save()
            return
        elif self.embed['type'] == 'article':
            self.url = self.embed['url']

            self.provider_name = self.embed['provider']['name'] if self.embed['provider'] else None
            self.provider_url = self.embed['provider']['url'] if self.embed['provider'] else None
            self.description = self.embed['description']
            self.title = self.embed['title']

            self.log.warning("Didn't save file (Not sure what to do with articles for now)")
            await self.save()
            return
        self.log.critical(embed)
        self.log.critical("Unknown embed type was encountered!")
        await self.save()
        pass
