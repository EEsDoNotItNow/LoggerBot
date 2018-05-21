
from html.parser import HTMLParser
from pathlib import Path
import asyncio
import discord
import numpy as np
import re
import requests

from ..Client import Client
from ..Log import Log
from ..SQL import SQL

from .Attachment import Attachment
from .e621 import e621
from .Embed import Embed
from .Link import Link

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
            channels = sorted(server.channels, key=lambda x: x.position)
            for channel in channels:
                self.log.info(f"Catchup on {channel}")
                messages = self.client.logs_from(channel, limit=10000000)
                count = 0
                try:
                    async for message in messages:
                        count += 1
                        try:
                            parsed = await self.process_message(message)
                        except Exception:
                            self.log.exception("Failed to parse a message")
                            continue
                        if parsed:
                            await asyncio.sleep(np.random.rand()*5)
                            self.log.info("Wake up and scan")
                except discord.errors.Forbidden:
                    continue
                self.log.info(f"Channel {channel} had {count:,.0f} messages")
        self.log.info("Catchup completed")
        pass

    async def process_message(self, message):
        """New way to handle a message
        """
        found_files = 0

        # Don't reprocess messages, ever!
        message_id = message.id
        values = self.sql.cur.execute("SELECT * FROM messages WHERE message_id=:message_id",locals()).fetchone()
        if values:
            return 0

        await self.log_message(message)

        # Check for links, download each of them
        urls = re.findall('(https?://[^ ><\n]+)', message.content)
        for url in urls:
            await asyncio.sleep(np.random.rand()*found_files)

            # Check to see if we have an e621
            is_e621 = re.match(r"https?://e621.net", url)
            if is_e621:
                self.log.info("Found a valid e621 link!")
                url_getter = e621(url)
                await url_getter.process()
                for url in url_getter.urls:
                    await asyncio.sleep(np.random.rand()*found_files)
                    self.log.info(f"Found URL: {url}")
                    link = Link(url, message)
                    await link.process()
                    found_files += 1 if link.saved else 0
                continue

            # Link wasn't a special case, just run it
            self.log.info(f"Found URL: {url}")
            link = Link(url, message)
            await link.process()
            found_files += 1 if link.saved else 0
            if not link.saved:
                self.log.info(f"Failed to use Link on {url}")

        if found_files:
            return found_files

        # If no links, check for embeds, download each of them
        for embed in message.embeds:
            await asyncio.sleep(np.random.rand()*found_files)
            self.log.info(f"Found Embed")
            embed = Embed(embed, message)
            await embed.process()
            found_files += 1

        if found_files:
            return found_files

        # If no embeds, check for attachments, download each of them
        for attachment in message.attachments:
            await asyncio.sleep(np.random.rand()*found_files)
            self.log.info(f"Found Attachment")
            attachment = Attachment(attachment, message)
            await attachment.process()
            found_files += 1
            
        return found_files


    async def log_message(self, message):

        message_id = message.id
        channel_id = message.channel.id if message.channel else None
        server_id = message.server.id if message.channel else None
        author_id = message.author.id
        created_at = message.timestamp.timestamp()
        content = message.content
        clean_content = message.clean_content
        embed_count = len(message.embeds)
        user_mention_count = len(message.mentions)
        channel_mention_count = len(message.channel_mentions)
        attachment_count = len(message.attachments)
        reaction_count = len(message.reactions)

        # Log message
        cmd = """
            INSERT INTO messages 
            (
                message_id,
                channel_id,
                server_id,
                author_id,
                created_at,
                content,
                clean_content,
                embed_count,
                user_mention_count,
                channel_mention_count,
                attachment_count,
                reaction_count
            ) VALUES (
                :message_id,
                :channel_id,
                :server_id,
                :author_id,
                :created_at,
                :content,
                :clean_content,
                :embed_count,
                :user_mention_count,
                :channel_mention_count,
                :attachment_count,
                :reaction_count
            )
            """
        self.sql.cur.execute(cmd, locals())
        await self.sql.commit()


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

