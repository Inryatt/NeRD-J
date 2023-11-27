import asyncio
import logging
import os
import sys
import threading

import discord
from discord.ext import commands
from kafka import KafkaConsumer

import kafka
consumer = KafkaConsumer('nerdj_play',bootstrap_servers='localhost:9094',client_id="nerd-jh", group_id='nerd-e')

producer = kafka.KafkaProducer(bootstrap_servers='localhost:9094')
token = open("token2.txt", "r").read()
handler = logging.StreamHandler()

async def kafka_consume_play():
    consumer = KafkaConsumer('nerdj_play',bootstrap_servers='localhost:9094',client_id="nerd-jh", group_id='nerd-e')
    for message in consumer:
            print(message.value.decode('utf-8'))
            #cena.true_play(cena.bot.get_context(msg),search = message.value.decode('utf-8'))
            channel = bot.get_channel(712710066653888563)
            await channel.send(f"?play {message.value.decode('utf-8')}")
            #cena.true_play(ctx,search = message.value.decode('utf-8'))
    consumer = KafkaConsumer('nerdj_simplecommand',bootstrap_servers='localhost:9094',client_id="nerd-j", group_id='nerd-e')
    
    for message in consumer:
            print(message.value.decode('utf-8'))
            #cena.true_play(cena.bot.get_context(msg),search = message.value.decode('utf-8'))
            channel = bot.get_channel(712710066653888563)
            await channel.send(f"?{message.value.decode('utf-8')}")


async def kafka_consume_cmd():
    consumer = KafkaConsumer('nerdj_simplecommand',bootstrap_servers='localhost:9094',client_id="nerd-j", group_id='nerd-e')
    for message in consumer:
            print(message.value.decode('utf-8'))
            #cena.true_play(cena.bot.get_context(msg),search = message.value.decode('utf-8'))
            channel = bot.get_channel(712710066653888563)
            await channel.send(f"?{message.value.decode('utf-8')}")
            #cena.true_play(ctx,search = message.value.decode('utf-8'))



  


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
        asyncio.get_event_loop().create_task(kafka_consume_cmd())
        #asyncio.get_event_loop().create_task(kafka_consume_play())

        #threading.Thread(target=kafka_consume_play, args=()).start()
        #threading.Thread(target=kafka_consume, args=()).start()


    @bot.command(name='fix', aliases=['repair'])
    @commands.is_owner()
    async def fixbot(ctx):
        print('Fixing...')
        await ctx.send('Fixing myself...')
        os.execv(sys.executable, ['python3'] + sys.argv) 
        return
    
    async with bot:
        discord.utils.setup_logging(level=logging.INFO, root=False,handler=handler)
        await bot.start(token)


        

    

import asyncio
asyncio.run(main())