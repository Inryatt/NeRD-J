import asyncio
import logging
import os
import sys
import threading
import aiormq
import discord


from discord.ext import commands
#from kafka import KafkaConsumer



#consumer = KafkaConsumer('nerdj_play',bootstrap_servers='localhost:9094',client_id="nerd-jh", group_id='nerd-e')

#producer = kafka.KafkaProducer(bootstrap_servers='localhost:9094')
token = open("token2.txt", "r").read()
handler = logging.StreamHandler()

async def consume_play(message):
        
    print(f" [x] Received message {message!r}")
    print(f"Message body is: {message.body!r}")
    channel = bot.get_channel(712710066653888563)
    await channel.send(f"?play {message.body.decode('utf-8')}")
            
async def consume_cmd(message):
        
    print(f" [x] Received message {message!r}")
    print(f"Message body is: {message.body!r}")
    channel = bot.get_channel(712710066653888563)
    await channel.send(f"?{message.body.decode('utf-8')}")
            
  


async def main():
    intents = discord.Intents.default()
    intents.members = True
    intents.presences = True
    intents.messages = True
    intents.typing = True
    intents.message_content = True
    global bot
    bot =  commands.Bot(command_prefix='&', case_insensitive=True, description="The Superior Bot",intents=intents)
    global conn
    conn = await aiormq.connect("amqp://192.168.2.7/")
    global channel
    channel = await conn.channel()

    async def get_messages():
        play_dec = await channel.queue_declare('nerdj/play')
        play_consume = await channel.basic_consume(
            play_dec.queue, consume_play, no_ack=True
        )
        play_cmd = await channel.queue_declare('nerdj/cmd')
        play_cmd = await channel.basic_consume(
            play_cmd.queue, consume_cmd, no_ack=True
        )

    @bot.event
    async def on_ready():
        print('Logged in as:\n{0.user.name}\n{0.user.id}'.format(bot))
        #asyncio.get_event_loop().create_task(kafka_consume_play())
        asyncio.get_event_loop().create_task(get_messages())
       #channel.basic.consume(consume_play, 'nerdj/play', no_ack=True)
        #asyncio.get_event_loop().create_task(channel.start_consuming())
        

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
