
from pathlib import Path
import requests
import re


from .DownloaderBase import DownloaderBase

class Link(DownloaderBase):

    def __init__(self, url, message):
        super().__init__()
        self.url = url
        self.message = message
        self.channel_name = message.channel.name if message.channel else message.author.name
        self.guild_name = message.guild.name if message.guild else "PrivateMessages"
        self.dest = Path(self.base_dest, self.guild_name, self.channel_name)
        self.file_name = None


    async def save(self):
        """Save data to SQL
        """
        message_id = self.message.id
        url = self.url
        file_name = self.file_name
        saved = self.saved
        http_status = self.http_status
        dest = str(self.dest)

        cmd = """
            INSERT INTO links 
            (
                message_id,
                url,
                file_name,
                saved,
                http_status,
                dest
            ) VALUES (
                :message_id,
                :url,
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
        while 1:
            # Does this URL end with a file extension?
            match_obj = re.search(self.url_file_regex, self.url, re.IGNORECASE)
            if match_obj:
                self.log.info("File has valid extension, grab it directly")
                self.file_name = match_obj.group("fn")
                self.file_name = f"{self.message.id}_{self.file_name}"
                try:
                    await self.DownloadFile()
                except requests.exceptions.HTTPError as e:
                    self.log.warning(f"Got http_status error on download: {e}")
                    self.log.error(f"I coulnd't find a way to save this url: {self.url}")
                except Exception:
                    self.log.error("Failed to save file")
                    self.log.error(f"URL: {self.url}")
                    self.log.error(f"File Name: {self.file_name}")
                    self.log.exception("Didn't save file")
                break

            # All else fails, scrap that site!
            try:
                await self.ScrapeUrl()
            except requests.exceptions.HTTPError as e:
                self.log.warning(f"Got http_status error on scrape: {e}")
                self.log.error(f"I coulnd't find a way to save this url: {self.url}")
            except Exception:
                self.log.error("Failed to save file")
                self.log.error(f"URL: {self.url}")
                self.log.error(f"File Name: {self.file_name}")
                self.log.exception("Didn't save file")
            break

        await self.save()
