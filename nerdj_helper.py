"""
Copyright (c) 2019 Valentin B.
A simple music bot written in discord.py using youtube-dl.
Though it's a simple example, music bots are complex and require much time and knowledge until they work perfectly.
Use this as an example or a base for your own bot and extend it as you want. If there are any bugs, please let me know.
Requirements:
Python 3.5+
pip install -U discord.py pynacl youtube-dl
You also need FFmpeg in your PATH environment variable or the FFmpeg.exe binary in your bot's directory on Windows.
"""

import asyncio
import logging
import os
import sys

import discord
from discord.ext import commands
from kafka import KafkaConsumer

import kafka

producer = kafka.KafkaProducer(bootstrap_servers='localhost:9094')
token = open("token2.txt", "r").read()
handler = logging.StreamHandler()

async def kafka_consume():
    consumer = KafkaConsumer('nerdj_play',bootstrap_servers='localhost:9094',client_id="nerd-j", group_id='nerd-e')
    print("message received")
    while True:
        for message in consumer:
            print(message.value.decode('utf-8'))
            #cena.true_play(cena.bot.get_context(msg),search = message.value.decode('utf-8'))
            channel = bot.get_channel(712710066653888563)
            await channel.send(f"?play {message.value.decode('utf-8')}")
            #cena.true_play(ctx,search = message.value.decode('utf-8'))

class Companion(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    def cog_unload(self):
        for state in self.voice_states.values():
            self.bot.loop.create_task(state.stop())

    def cog_check(self, ctx: commands.Context):
        if not ctx.guild:
            raise commands.NoPrivateMessage('This command can\'t be used in DM channels.')

        return True

    async def cog_before_invoke(self, ctx: commands.Context):
        ctx.voice_state = self.get_voice_state(ctx)

    async def cog_command_error(self, ctx: commands.Context, error: commands.CommandError):
        await ctx.send('An error occurred: {}'.format(str(error)))

  


async def main():
    intents = discord.Intents.default()
    intents.members = True
    intents.presences = True
    intents.messages = True
    intents.typing = True
    intents.message_content = True
    global bot
    bot =  commands.Bot(command_prefix='&', case_insensitive=True, description="The Superior Bot",intents=intents)
    

    

    @bot.event
    async def on_ready():
        print('Logged in as:\n{0.user.name}\n{0.user.id}'.format(bot))
        asyncio.get_event_loop().create_task(kafka_consume())

    @bot.command(name='fix', aliases=['repair'])
    @commands.is_owner()
    async def fixbot(ctx):
        print('Fixing...')
        await ctx.send('Fixing myself...')
        os.execv(sys.executable, ['python3'] + sys.argv) 
        return
    
    async with bot:
        await bot.add_cog(Companion(bot))
        discord.utils.setup_logging(level=logging.INFO, root=False,handler=handler)
        await bot.start(token)


        

    

import asyncio

asyncio.run(main())