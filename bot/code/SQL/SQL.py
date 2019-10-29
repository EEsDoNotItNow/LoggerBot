
import asyncio
import sqlite3
import pathlib
import time

from ..Singleton import Singleton
from ..Log import Log
from ..Client import Client



class SQL(metaclass=Singleton):
    """Manage SQL connection, as well as basic user information
    """

    def __init__(self, db_name):

        db_path = pathlib.Path(db_name)
        self.log = Log()
        if not db_path.is_file():
            self.create_db(db_name)

        self.conn = sqlite3.connect(db_name)
        self.conn.row_factory = self.dict_factory
        self.client = Client()
        self._commit_in_progress = False
        self.log.info("SQL init completed")


    def create_db(self, db_name):
        self.log.warning("New DB file")
        conn = sqlite3.connect(db_name)
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        conn.commit()
        cur.execute("PRAGMA synchronous=1")
        conn.commit()
        conn.close()
        self.log.warning("Finished new DB file creation")


    @property
    def cur(self):
        return self.conn.cursor()


    async def on_ready(self):
        await self.table_setup()

        self.log.info("SQL registered to receive commands!")


    async def on_message(self, message):
        self.log.debug(f"Got message: {message.content}")
        self.log.debug(f"       From: {message.author.name} ({message.author.id})")
        if message.guild:
            self.log.debug(f"         On: {message.guild} ({message.guild.id})")

        data = {}
        data['name'] = message.author.name
        data['display_name'] = message.author.display_name
        data['user_id'] = message.author.id
        data['discriminator'] = message.author.discriminator
        data['avatar'] = message.author.avatar
        data['bot'] = message.author.bot
        data['avatar_url'] = str(message.author.avatar_url)
        data['default_avatar_url'] = str(message.author.default_avatar_url)
        data['mention'] = message.author.mention
        data['created_at'] = message.author.created_at

        cmd = """
            INSERT OR REPLACE INTO users 
            (
                name,
                display_name,
                user_id,
                discriminator,
                avatar,
                bot,
                avatar_url,
                default_avatar_url,
                mention,
                created_at
            ) VALUES (
                :name,
                :display_name,
                :user_id,
                :discriminator,
                :avatar,
                :bot,
                :avatar_url,
                :default_avatar_url,
                :mention,
                :created_at
            )
            """
        self.cur.execute(cmd, data)
        await self.commit()


    async def commit(self, now=True):
        # Schedule a commit in the future
        # Get loop from the client, schedule a call to _commit and return
        if now:
            self.conn.commit()
        else:
            asyncio.ensure_future(self._commit(now))


    async def _commit(self, now=True):
        self.log.debug("Start a _commit()")
        if self._commit_in_progress and not now:
            self.log.debug("Skipped a _commit()")
            return
        self._commit_in_progress = True
        if not now:
            await asyncio.sleep(5)
        # Commit SQL
        self.conn.commit()
        self._commit_in_progress = False
        self.log.info("Finished a _commit()")



    async def table_exists(self, table_name):
        cmd = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        if self.cur.execute(cmd).fetchone():
            return True
        return False


    @staticmethod
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d


    async def table_setup(self):
        """Setup any SQL tables needed for this class
        """
        self.log = Log()


        self.log.info("Check to see if users exists.")
        if not await self.table_exists("users"):
            self.log.info("Create users table")
            cur = self.cur
            cmd = """
                CREATE TABLE IF NOT EXISTS users
                (
                    name TEXT NOT NULL,
                    display_name TEXT,
                    user_id TEXT NOT NULL UNIQUE,
                    discriminator TEXT,
                    avatar TEXT,
                    bot BOOLEAN,
                    avatar_url TEXT,
                    default_avatar TEXT,
                    default_avatar_url TEXT,
                    mention TEXT,
                    created_at INTEGER
                )"""
            cur.execute(cmd)
            await self.commit()


        self.log.info("Check to see if channels exists.")
        if not await self.table_exists("channels"):
            self.log.info("Create channels table")
            cur = self.cur
            cmd = """
                CREATE TABLE IF NOT EXISTS channels
                (
                    name TEXT NOT NULL,
                    channel_id TEXT NOT NULL UNIQUE,
                    server_id TEXT,
                    topic TEXT,
                    position INTEGER,
                    type TEXT,
                    mention TEXT
                )"""
            cur.execute(cmd)
            await self.commit()


        self.log.info("Check to see if messages exists.")
        if not await self.table_exists("messages"):
            self.log.info("Create messages table")
            cur = self.cur
            cmd = """
                CREATE TABLE messages
                (
                    message_id TEXT NOT NULL UNIQUE,
                    channel_id TEXT,
                    server_id TEXT,
                    author_id TEXT NOT NULL,
                    created_at INTEGER,
                    content TEXT,
                    clean_content TEXT,
                    embed_count INTEGER,
                    user_mention_count INTEGER,
                    channel_mention_count INTEGER,
                    attachment_count INTEGER,
                    reaction_count INTEGER
                )
            """
            cur.execute(cmd)
            await self.commit()


        self.log.info("Check to see if links exists.")
        if not await self.table_exists("links"):
            self.log.info("Create links table")
            cur = self.cur
            cmd = """
                CREATE TABLE links
                (
                    message_id TEXT NOT NULL,
                    url TEXT,
                    file_name TEXT,
                    saved BOOLEAN,
                    alt_saved BOOLEAN DEFAULT 0,
                    http_status INTEGER,
                    dest TEXT
                )
            """
            cur.execute(cmd)
            await self.commit()


        self.log.info("Check to see if embeds exists.")
        if not await self.table_exists("embeds"):
            self.log.info("Create embeds table")
            cur = self.cur
            cmd = """
                CREATE TABLE embeds
                (
                    message_id TEXT NOT NULL,
                    url TEXT,
                    proxy_url TEXT,
                    provider_url TEXT,
                    provider_name TEXT,
                    type TEXT,
                    description TEXT,
                    title TEXT,
                    file_name TEXT,
                    saved BOOLEAN,
                    http_status INTEGER,
                    dest TEXT,
                    author_url TEXT,
                    author_name TEXT,
                    video TEXT
                )
            """
            cur.execute(cmd)
            await self.commit()


        self.log.info("Check to see if attachments exists.")
        if not await self.table_exists("attachments"):
            self.log.info("Create attachments table")
            cur = self.cur
            cmd = """
                CREATE TABLE attachments
                (
                    message_id TEXT NOT NULL,
                    attachment_id TEXT, --Unique?
                    file_name TEXT, 
                    height INTEGER,
                    proxy_url TEXT,
                    saved BOOLEAN,
                    size INTEGER,
                    url TEXT,
                    width INTEGER
                )
            """
            cur.execute(cmd)
            await self.commit()


        # self.log.info("Check to see if images exists.")
        # if not await self.table_exists("images"):
        #     self.log.info("Create images table")
        #     cur = self.cur
        #     cmd = """
        #         CREATE TABLE images
        #         (
        #             file_name TEXT,
        #             dest TEXT,
        #             ahash TEXT,
        #             phash TEXT,
        #             dhash TEXT,
        #             whash TEXT
        #         )
        #     """
        #     cur.execute(cmd)
        #     await self.commit()


"""
Neat trick for ranks
select  p1.*
,       (
        select  count(*)
        from    People as p2
        where   p2.age > p1.age
        ) as AgeRank
from    People as p1
where   p1.Name = 'Juju bear'
"""
