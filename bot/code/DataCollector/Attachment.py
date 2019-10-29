from collections import defaultdict
from pathlib import Path
import asyncio
import re
import requests
import time

from ..Log import Log
from ..SQL import SQL

from .DownloaderBase import DownloaderBase

class Attachment(DownloaderBase):

    def __init__(self, attachment, message):
        super().__init__()
        self.attachment = defaultdict(lambda: None)
        self.attachment.update(attachment)
        self.message = message
        self.channel_name = message.channel.name if message.channel else message.author.name
        self.guild_name = message.guild.name if message.guild else "PrivateMessages"
        self.dest = Path(self.base_dest, self.guild_name, self.channel_name)


    async def save(self):
        """Save data to SQL
        """
        message_id = self.message.id
        attachment_id = self.attachment['id']
        file_name = self.attachment['filename']
        height = self.attachment['height']
        proxy_url = self.attachment['proxy_url']
        saved = self.saved
        size = self.attachment['size']
        url = self.attachment['url']
        width = self.attachment['width']

        cmd = """
            INSERT INTO attachments 
            (
                message_id,
                attachment_id,
                file_name,
                height,
                proxy_url,
                saved,
                size,
                url,
                width
            ) VALUES (
                :message_id,
                :attachment_id,
                :file_name,
                :height,
                :proxy_url,
                :saved,
                :size,
                :url,
                :width
            )
            """
        self.sql.cur.execute(cmd, locals())
        await self.sql.commit()


    async def process(self):
        # Attempt to save the image
        self.log.info("Processing attachment")

        self.url = self.attachment['url']
        self.file_name = Path(f"{self.message.id}_{self.attachment['filename']}")
        try:
            await self.DownloadFile()
        except requests.exceptions.HTTPError as e:
            self.log.warning(f"Got http_status error on download: {e}")
        except Exception:
            self.log.error("Failed to save file")
            self.log.info(self.attachment)
            self.log.error(f"URL: {self.url}")
            self.log.error(f"File Name: {self.file_name}")
            self.log.exception("Didn't save file")
        await self.save()

