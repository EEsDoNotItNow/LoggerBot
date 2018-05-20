
import asyncio
import sqlite3
import pathlib

from ..Singleton import Singleton
from ..Log import Log
from ..Client import Client



class SQL(metaclass=Singleton):
    """Manage SQL connection, as well as basic user information
    """

    def __init__(self, db_name):

        db_path = pathlib.Path(db_name)
        if not db_path.is_file():
            self.create_db(db_name)

        self.conn = sqlite3.connect(db_name)
        self.conn.row_factory = self.dict_factory
        self.log = Log()
        self.client = Client()
        self._commit_in_progress = False
        self.log.info("SQL init completed")


    def create_db(self, db_name):
        conn = sqlite3.connect(db_name)
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        conn.commit()
        cur.execute("PRAGMA synchronous=1")
        conn.commit()
        conn.close()


    @property
    def cur(self):
        return self.conn.cursor()


    async def on_ready(self):
        await self.table_setup()

        self.log.info("SQL registered to receive commands!")


    async def on_message(self, message):
        self.log.debug(f"Got message: {message.content}")
        self.log.debug(f"       From: {message.author.name} ({message.author.id})")
        if message.server:
            self.log.debug(f"         On: {message.server} ({message.server.id})")

        data = {}
        data['name'] = message.author.name
        data['display_name'] = message.author.display_name
        data['user_id'] = message.author.id
        data['discriminator'] = message.author.discriminator
        data['avatar'] = message.author.avatar
        data['bot'] = message.author.bot
        data['avatar_url'] = message.author.avatar_url
        data['default_avatar_url'] = message.author.default_avatar_url
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
                    user_id TEXT NOT NULL,
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


        self.log.info("Check to see if messages exists.")
        if not await self.table_exists("messages"):
            self.log.info("Create messages table")
            cur = self.cur
            cmd = """
                CREATE TABLE messages
                (
                    message_id TEXT NOT NULL UNIQUE,
                    channel_id TEXT,
                    author_id TEXT NOT NULL,
                    created_at INTEGER,
                    content TEXT,
                    clean_content TEXT
                )
            """
            cur.execute(cmd)
            await self.commit()


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
