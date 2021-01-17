from discord.ext import commands
import asyncio
import asyncpg
import random
import re
import psycopg2
import random
from discord import utils
from discord import TextChannel


TOKEN = ''


def insert_messages(values):
    import psycopg2
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="mydb")

        cursor = connection.cursor()
        query = "INSERT INTO messages (message, date) VALUES (%s, %s)"
        cursor.executemany(query, values)
        connection.commit()
        print(cursor.rowcount, "Record inserted")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


async def fetch_from_channel(channel, messages, size_of_chunk, chunk, fetch_after):
    while True:
        fetched_messages = await channel.history(limit=size_of_chunk, after=fetch_after, oldest_first=True).flatten()
        if len(fetched_messages) == 0:
            break

        fetch_after = fetched_messages[len(
            fetched_messages) - 1].created_at
        messages.extend(list(
            map(lambda m: m.content, fetched_messages)))

        chunk += 1
        print(chunk)


class ChatCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.is_responsive = True

    @commands.command()
    async def gadaj(self, ctx):
        message = ctx.message
        if message.author.bot:
            return
        if not self.is_responsive:
            return
        if message.content.lower().startswith("acodin"):
            await message.channel.send("Siema")
            return

        current_key = random.choice(list(self.bot.msg_dict))
        i = 0
        output = [current_key]
        while len(self.bot.msg_dict[current_key]) != 0:
            word = random.choice(self.bot.msg_dict[current_key])
            current_key = " ".join([current_key.split()[1], word])
            output.append(word)
            i += 1

        await message.channel.send(" ".join(output))

    @commands.command()
    async def stop(self, ctx):
        self.is_responsive = False

    @commands.command()
    async def start(self, ctx):
        self.is_responsive = True


class DiscordBot(commands.Bot):
    def __init__(self, command_prefix, help_command=None, description=None, **options):
        super().__init__(command_prefix, help_command=help_command,
                         description=description, **options)

        self.msg_dict = {}

    async def on_ready(self):
        guild = self.guilds[0]
        channels = list(filter(lambda channel: issubclass(
            channel.__class__, TextChannel), guild.channels))

        print(channels)

        size_of_chunk = 500
        chunk = 0
        fetch_after = None

        messages = []

        # for ch in channels:
        #     await fetch_from_channel(ch, messages, size_of_chunk, chunk, fetch_after)
        casual = await self.fetch_channel(543121513717694525)
        await fetch_from_channel(casual, messages, size_of_chunk, chunk, fetch_after)

        # try:
        #     connection = psycopg2.connect(user="postgres",
        #                                   password="Aleksander",
        #                                   host="127.0.0.1",
        #                                   port="5432",
        #                                   database="mydb")

        #     cursor = connection.cursor()

        #     cursor.execute(
        #         "SELECT date FROM messages ORDER BY date DESC LIMIT 1")
        #     record = cursor.fetchone()
        #     if record is not None:
        #         fetch_after = record[0]

        # except (Exception, psycopg2.Error) as error:
        #     print("Error while connecting to PostgreSQL", error)
        # finally:
        #     if(connection):
        #         cursor.close()
        #         connection.close()
        #         print("PostgreSQL connection is closed")

        messages = list(map(lambda msg: list(map(lambda str: str.lower(), list(filter(
            lambda word: word not in ",.?!() ", re.split("(?=[.,?!() ])|(?<=[.,?!() ])", msg))))), messages))
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

        print("Bot ready")


client = DiscordBot("!")
client.add_cog(ChatCog(client))
client.run(TOKEN)
