
import discord
import asyncio
from pathlib import Path
import requests
import re
from html.parser import HTMLParser
import numpy as np

from ..Log import Log
from ..Client import Client
from ..SQL import SQL

class DataCollector:

    def __init__(self):
        self.log = Log()
        self.client = Client()
        self.sql = SQL()
        self.inbox_dir = Path("~/Pictures/FlashBot/").expanduser()
        pass


    async def on_ready(self):
        self.log.info("DataCollector, ready to receive commands!")
        asyncio.ensure_future(self.catchup_old_logs())


    async def on_message(self, message):
        self.log.info(f"{message.author.name}: {message.content}")
        await self.process_message(message)


    async def catchup_old_logs(self):
        self.log.info("Catchup to missing messages!")
        for server in self.client.servers:
            self.log.info(f"Catchup on {server}")
            for channel in server.channels:
                self.log.info(f"Catchup on {channel}")
                messages = self.client.logs_from(channel, limit=1000)
                try:
                    async for message in messages:
                        parsed = await self.process_message(message)
                        if parsed:
                            await asyncio.sleep(np.random.rand()*60)
                            self.log.info("Wake up and scan")
                except discord.errors.Forbidden:
                    continue
        self.log.info("Catchup completed")
        pass

    async def process_message(self, message):

        found_files = False

        message_id = message.id
        channel_id = message.channel.id if message.channel else None
        author_id = message.author.id
        created_at = message.timestamp.timestamp()
        content = message.content
        clean_content = message.clean_content

        # Make sure we havn't already processed this one
        values = self.sql.cur.execute("SELECT * FROM messages WHERE message_id=:message_id",locals()).fetchone()
        if values:
            return False

        save_location = Path(self.inbox_dir, message.channel.name)

        if message.attachments:
            for attachment in message.attachments:
                self.log.info("Found an attachment, attempt to download")
                asyncio.ensure_future(DownloadFile(attachment['url'], save_location, message))
                found_files = True


        if message.embeds:
            for embed in message.embeds:
                if embed['type'] == 'image':
                    self.log.info("Found an embed, attempt to download")
                    asyncio.ensure_future(DownloadFile(embed['url'], save_location, message))
                    found_files = True
                elif embed['type'] == 'link':
                    asyncio.ensure_future(ScrapeUrl(embed['url'], save_location, message))
                    found_files = True
                else:
                    self.log.warning("Unsure what to do with embed:")
                    self.log.warning(embed)

        if not message.embeds or message.attachments:
            urls = re.findall('(https?://[^ ><]+)', message.content)
            for url in urls:
                self.log.info(f"Found URL: {url}")
                asyncio.ensure_future(ScrapeUrl(url, save_location, message))
                found_files = True


        cmd = """
            INSERT INTO messages 
            (
                message_id,
                channel_id,
                author_id,
                created_at,
                content,
                clean_content
            ) VALUES (
                :message_id,
                :channel_id,
                :author_id,
                :created_at,
                :content,
                :clean_content
            )
            """
        self.sql.cur.execute(cmd, locals())
        await self.sql.commit()
        return found_files

    async def save_images(self, message):
        pass


    async def on_channel_create(self, channel):
        pass


    async def on_channel_delete(self, channel):
        pass


    async def on_channel_update(self, before, after):
        pass


    async def on_error(self, event, *args, **kwargs):
        pass


    async def on_group_join(self, channel, user):
        pass


    async def on_group_remove(self, channel, user):
        pass


    async def on_member_ban(self, member):
        pass


    async def on_member_join(self, member):
        pass


    async def on_member_remove(self, member):
        pass


    async def on_member_unban(self, server, user):
        pass


    async def on_member_update(self, before, after):
        pass


    async def on_message_delete(self, message):
        pass


    async def on_message_edit(self, before, after):
        pass


    async def on_reaction_add(self, reaction, user):
        pass


    async def on_reaction_clear(self, message, reactions):
        pass


    async def on_reaction_remove(self, reaction, user):
        pass


    async def on_resumed(self, ):
        pass


    async def on_server_available(self, server):
        pass


    async def on_server_emojis_update(self, before, after):
        pass


    async def on_server_join(self, server):
        pass


    async def on_server_remove(self, server):
        pass


    async def on_server_role_create(self, role):
        pass


    async def on_server_role_delete(self, role):
        pass


    async def on_server_role_update(self, before, after):
        pass


    async def on_server_unavailable(self, server):
        pass


    async def on_server_update(self, before, after):
        pass


    async def on_socket_raw_receive(self, msg):
        pass


    async def on_socket_raw_send(self, payload):
        pass


    async def on_typing(self, channel, user, when):
        pass


    async def on_voice_state_update(self, before, after):
        pass

async def DownloadFile(url, dest, message):
    log = Log()
    log.info(f"Attempt to download {url} to {dest}")

    dest.mkdir(parents=True, exist_ok=True)

    local_filename = url.split('/')[-1]

    r = requests.get(url)

    r.raise_for_status()

    _file = Path(dest,local_filename)
    if _file.is_file():
        _file = Path(f"{_file}_{message.id}")

    with open(_file, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)

async def ScrapeUrl(url, dest, message):
    log = Log()
    log.info(f"Attempt to scrape url {url} to {dest}")

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    match_obj = re.search(".*\.(jpg|jpeg|png)$", url)
    if match_obj:
        await DownloadFile(url, dest, message)
        return

    response = requests.get(url, headers=headers)

    log.info(response)
    log.info(response.encoding)
    # log.info(response.text)
    match_obj = re.search("<a href=\"(?P<file>.*)\">Download</a>", response.text)
    if match_obj:
        log.info("Attempt to download the image!")
        await DownloadFile(match_obj.group("file"), dest, message)
        return

    log.error(f"No match found for {url}")
    dest.mkdir(parents=True, exist_ok=True)
    _file = Path(dest, f"{url.replace('/','')}.html")
    log.info(f"Write to file {_file}")
    with open(_file, 'w') as f:
        f.write(f'<meta http-equiv="refresh" content="0; URL=\'{url}\'" />')
