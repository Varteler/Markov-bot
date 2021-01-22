from discord.ext import commands
import asyncio
import asyncpg
import random
import re
import psycopg2
import random
import random
from discord import utils
from discord import TextChannel
from psycopg2 import pool


TOKEN = ''


def fetch_messages(connection, messages):
    cursor = connection.cursor()
    query = "SELECT * FROM messages"
    cursor.execute(query)
    records = cursor.fetchall()
    for row in records:
        messages.append(row[0])


def insert_messages(connection, values):
    cursor = connection.cursor()
    query = "INSERT INTO messages (message, date) VALUES (%s, %s)"
    cursor.executemany(query, values)
    connection.commit()
    print(cursor.rowcount, "Record inserted")


class ChatCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.is_responsive = True

    @commands.Cog.listener()
    async def on_message(self, message):
        if not self.bot.is_ready():
            return
        if message.author.bot:
            return
        if random.randint(1, 20) == 5:
            output = self.generate_sequence()
            await message.channel.send(" ".join(output))

    @commands.command()
    async def gadaj(self, ctx):
        if not self.bot.is_ready():
            return

        message = ctx.message
        if message.author.bot:
            return
        if not self.is_responsive:
            return
        if message.content.lower().startswith("acodin"):
            await message.channel.send("Siema")
            return

        output = self.generate_sequence()
        await message.channel.send(" ".join(output))

    @commands.command()
    async def stop(self, ctx):
        self.is_responsive = False

    @commands.command()
    async def start(self, ctx):
        self.is_responsive = True

    def generate_sequence(self):
        current_key = random.choice(list(self.bot.msg_dict))
        i = 0
        output = [current_key]
        while len(self.bot.msg_dict[current_key]) != 0:
            word = random.choice(self.bot.msg_dict[current_key])
            current_key = " ".join([current_key.split()[1], word])
            output.append(word)
            i += 1

        return output


class DiscordBot(commands.Bot):
    def __init__(self, command_prefix, help_command=None, description=None, **options):
        super().__init__(command_prefix, help_command=help_command,
                         description=description, **options)

        self.msg_dict = {}

    async def on_ready(self):
        self.set_connection_pool()

        size_of_chunk = 500
        chunk = 0
        fetch_after = None

        messages = []

        # for ch in channels:
        #     await fetch_from_channel(ch, messages, size_of_chunk, chunk, fetch_after)
        casual = await self.fetch_channel(543121513717694525)
        info_channel = await self.fetch_channel(800089621001666581)

        connection = self.connection_pool.getconn()
        cursor = connection.cursor()
        cursor.execute(
            "SELECT date FROM messages ORDER BY date DESC LIMIT 1")
        record = cursor.fetchone()
        if record is not None:
            fetch_after = record[0]

        await self.fetch_from_channel(casual, info_channel, messages, size_of_chunk, chunk, fetch_after)
        fetch_messages(self.get_connection_from_pool(), messages)

        self.create_word_dict(messages)

        print("Bot ready")
        await info_channel.send("Żubry załadowane")

    def create_word_dict(self, messages_list):
        messages = list(map(lambda msg: list(map(lambda str: str.lower(), list(filter(
            lambda word: word not in ",.?!():;\"' ", re.split("(?=[.,?!():;\"' ])|(?<=[.,?!():;\"' ])", msg))))), messages_list))
        for sentence in messages:
            length = len(sentence)
            for i in range(length - 2):
                entry = " ".join([sentence[i], sentence[i+1]])
                if entry not in self.msg_dict.keys():
                    self.msg_dict[entry] = []
                self.msg_dict[entry].append(sentence[i+2])
            if len(sentence) > 0 and " ".join([sentence[length-2], sentence[length-1]]) not in self.msg_dict.keys():
                self.msg_dict[" ".join(
                    [sentence[length-2], sentence[length-1]])] = []

    async def fetch_from_channel(self, channel, info_channel, messages, size_of_chunk, chunk, fetch_after):
        while True:
            fetched_messages = await channel.history(limit=size_of_chunk, after=fetch_after, oldest_first=True).flatten()
            if len(fetched_messages) == 0:
                break

            fetch_after = fetched_messages[len(
                fetched_messages) - 1].created_at

            insert_messages(self.get_connection_from_pool(),
                            list(map(lambda m: (m.content, m.created_at), fetched_messages)))

            if chunk % 10 == 0:
                await info_channel.send("Ładuję żubry... " + str(chunk))

            chunk += 1
            print(chunk)

    def set_connection_pool(self):
        pool = psycopg2.pool.SimpleConnectionPool(1, 20, user="postgres",
                                                  password="Aleksander",
                                                  host="127.0.0.1",
                                                  port="5432",
                                                  database="mydb")

        if pool:
            print("Connection pool created successfully")
            self.connection_pool = pool

    def get_connection_from_pool(self):
        connection = self.connection_pool.getconn()
        if connection:
            return connection
        else:
            raise Exception("Couldn't receive connection from pool")


client = DiscordBot("!")
client.add_cog(ChatCog(client))
client.run(TOKEN)
