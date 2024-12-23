import asyncio
import functools
import itertools
import math
import random
import logging
import os
import sys

import discord
import yt_dlp as youtube_dl
from async_timeout import timeout
from discord.ext import commands
from bs4 import BeautifulSoup

import vlc
from typing import Optional

import aiormq
from aiormq.abc import DeliveredMessage

player = vlc.MediaPlayer()

mp_em = player.event_manager()

#producer = kafka.KafkaProducer(bootstrap_servers='localhost:9094')

async def send_to_mq(song):

    await channel.basic_publish(song.encode('utf-8'), routing_key='nerdj/np')
    

def vlc_playNow(url):
    player.stop()
    print("SET TO PLAY")
    player.set_mrl(url)
    player.play()

# Silence useless bug reports messages
youtube_dl.utils.bug_reports_message = lambda: ''
handler = logging.StreamHandler()

token = open("token.txt", "r").read()

class VoiceError(Exception):
    pass


class YTDLError(Exception):
    pass


class YTDLSource(discord.PCMVolumeTransformer):
    YTDL_OPTIONS = {
        'format': 'bestaudio/best',
        'extractaudio': True,
        'audioformat': 'mp3',
        'outtmpl': '%(extractor)s-%(id)s-%(title)s.%(ext)s',
        'restrictfilenames': True,
        'noplaylist': True,
        'nocheckcertificate': True,
        'ignoreerrors': False,
        'logtostderr': False,
        'quiet': True,
        'no_warnings': True,
        'default_search': 'ytsearch',
        'source_address': '0.0.0.0',
    }

    FFMPEG_OPTIONS = {
        'before_options': '-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5',
        'options': '-vn',
    }

    ytdl = youtube_dl.YoutubeDL(YTDL_OPTIONS)

    def __init__(self, ctx: commands.Context, source: discord.FFmpegPCMAudio, *, data: dict, volume: float = 0.5):
        super().__init__(source, volume)

        self.requester = ctx.author
        self.channel = ctx.channel
        self.data = data

        self.uploader = data.get('uploader')
        self.uploader_url = data.get('uploader_url')
        date = data.get('upload_date')
        self.upload_date = date[6:8] + '.' + date[4:6] + '.' + date[0:4]
        self.title = data.get('title')
        self.thumbnail = data.get('thumbnail')
        self.description = data.get('description')
        self.duration = self.parse_duration(int(data.get('duration')))
        self.tags = data.get('tags')
        self.url = data.get('webpage_url')
        self.views = data.get('view_count')
        self.likes = data.get('like_count')
        self.dislikes = data.get('dislike_count')
        self.stream_url = data.get('url')

    def __str__(self):
        return '**{0.title}** by **{0.uploader}**'.format(self)

    @classmethod
    async def create_source(cls, ctx: commands.Context, search: str, *, loop: asyncio.BaseEventLoop = None):
        loop = loop or asyncio.get_event_loop()

        partial = functools.partial(cls.ytdl.extract_info, search, download=False, process=False)
        data = await loop.run_in_executor(None, partial)

        if data is None:
            raise YTDLError('Couldn\'t find anything that matches `{}`'.format(search))

        if 'entries' not in data:
            process_info = data
        else:
            process_info = None
            for entry in data['entries']:
                if entry:
                    process_info = entry
                    break

            if process_info is None:
                raise YTDLError('Couldn\'t find anything that matches `{}`'.format(search))

        webpage_url = process_info['webpage_url']
        partial = functools.partial(cls.ytdl.extract_info, webpage_url, download=False)
        processed_info = await loop.run_in_executor(None, partial)

        if processed_info is None:
            raise YTDLError('Couldn\'t fetch `{}`'.format(webpage_url))

        if 'entries' not in processed_info:
            info = processed_info
        else:
            info = None
            while info is None:
                try:
                    info = processed_info['entries'].pop(0)
                except IndexError:
                    raise YTDLError('Couldn\'t retrieve any matches for `{}`'.format(webpage_url))
        
        return cls(ctx, discord.FFmpegPCMAudio(info['url'], **cls.FFMPEG_OPTIONS), data=info)


    @classmethod
    async def create_source_rafado(cls, search: str, *, loop: asyncio.BaseEventLoop = None):
        loop = loop or asyncio.get_event_loop()

        partial = functools.partial(cls.ytdl.extract_info, search, download=False, process=False)
        data = await loop.run_in_executor(None, partial)

        if data is None:
            raise YTDLError('Couldn\'t find anything that matches `{}`'.format(search))

        if 'entries' not in data:
            process_info = data
        else:
            process_info = None
            for entry in data['entries']:
                if entry:
                    process_info = entry
                    break

            if process_info is None:
                raise YTDLError('Couldn\'t find anything that matches `{}`'.format(search))

        webpage_url = process_info['webpage_url']
        partial = functools.partial(cls.ytdl.extract_info, webpage_url, download=False)
        processed_info = await loop.run_in_executor(None, partial)

        if processed_info is None:
            raise YTDLError('Couldn\'t fetch `{}`'.format(webpage_url))

        if 'entries' not in processed_info:
            info = processed_info
        else:
            info = None
            while info is None:
                try:
                    info = processed_info['entries'].pop(0)
                except IndexError:
                    raise YTDLError('Couldn\'t retrieve any matches for `{}`'.format(webpage_url))
        
        return cls(discord.FFmpegPCMAudio(info['url'], **cls.FFMPEG_OPTIONS), data=info)

    @classmethod
    async def search_source(cls, ctx: commands.Context, search: str, *, loop: asyncio.BaseEventLoop = None):
        channel = ctx.channel
        loop = loop or asyncio.get_event_loop()

        cls.search_query = '%s%s:%s' % ('ytsearch', 10, ''.join(search))

        partial = functools.partial(cls.ytdl.extract_info, cls.search_query, download=False, process=False)
        info = await loop.run_in_executor(None, partial)

        cls.search = {}
        cls.search["title"] = f'Search results for:\n**{search}**'
        cls.search["type"] = 'rich'
        cls.search["color"] = 7506394
        cls.search["author"] = {'name': f'{ctx.author.name}', 'url': f'{ctx.author.avatar_url}', 'icon_url': f'{ctx.author.avatar_url}'}
        
        lst = []

        for e in info['entries']:
            #lst.append(f'`{info["entries"].index(e) + 1}.` {e.get("title")} **[{YTDLSource.parse_duration(int(e.get("duration")))}]**\n')
            VId = e.get('id')
            VUrl = 'https://www.youtube.com/watch?v=%s' % (VId)
            lst.append(f'`{info["entries"].index(e) + 1}.` [{e.get("title")}]({VUrl})\n')

        lst.append('\n**Type a number to make a choice, Type `cancel` to exit**')
        cls.search["description"] = "\n".join(lst)

        em = discord.Embed.from_dict(cls.search)
        await ctx.send(embed=em, delete_after=45.0)

        def check(msg):
            return msg.content.isdigit() == True and msg.channel == channel or msg.content == 'cancel' or msg.content == 'Cancel'
        
        try:
            m = await bot.wait_for('message', check=check, timeout=45.0)

        except asyncio.TimeoutError:
            rtrn = 'timeout'

        else:
            if m.content.isdigit() == True:
                sel = int(m.content)
                if 0 < sel <= 10:
                    for key, value in info.items():
                        if key == 'entries':
                            """data = value[sel - 1]"""
                            VId = value[sel - 1]['id']
                            VUrl = 'https://www.youtube.com/watch?v=%s' % (VId)
                            partial = functools.partial(cls.ytdl.extract_info, VUrl, download=False)
                            data = await loop.run_in_executor(None, partial)
                    rtrn = cls(ctx, discord.FFmpegPCMAudio(data['url'], **cls.FFMPEG_OPTIONS), data=data)
                else:
                    rtrn = 'sel_invalid'
            elif m.content == 'cancel':
                rtrn = 'cancel'
            else:
                rtrn = 'sel_invalid'
        
        return rtrn

    @staticmethod
    def parse_duration(duration: int):
        if duration > 0:
            minutes, seconds = divmod(duration, 60)
            hours, minutes = divmod(minutes, 60)
            days, hours = divmod(hours, 24)

            duration = []
            if days > 0:
                duration.append('{}'.format(days))
            if hours > 0:
                duration.append('{}'.format(hours))
            if minutes > 0:
                duration.append('{}'.format(minutes))
            if seconds > 0:
                duration.append('{}'.format(seconds))
            
            value = ':'.join(duration)
        
        elif duration == 0:
            value = "LIVE"
        
        return value


class Song:
    __slots__ = ('source', 'requester')

    def __init__(self, source: YTDLSource):
        self.source = source
        self.requester = source.requester
    
    def create_embed(self):
        embed = (discord.Embed(title='Now playing', description='```css\n{0.source.title}\n```'.format(self), color=discord.Color.blurple())
                .add_field(name='Duration', value=self.source.duration)
                .add_field(name='Requested by', value=self.requester.mention)
                .add_field(name='Uploader', value='[{0.source.uploader}]({0.source.uploader_url})'.format(self))
                .add_field(name='URL', value='[Click]({0.source.url})'.format(self))
                .set_thumbnail(url=self.source.thumbnail)
                .set_author(name=self.requester.name, icon_url=self.requester.avatar_url))
        return embed


class SongQueue(asyncio.Queue):
    def __getitem__(self, item):
        if isinstance(item, slice):
            return list(itertools.islice(self._queue, item.start, item.stop, item.step))
        else:
            return self._queue[item]

    def __iter__(self):
        return self._queue.__iter__()

    def __len__(self):
        return self.qsize()

    def clear(self):
        self._queue.clear()

    def shuffle(self):
        random.shuffle(self._queue)

    def remove(self, index: int):
        del self._queue[index]


class VoiceState:
    def __init__(self, bot: commands.Bot, ctx: commands.Context):
        self.bot = bot
        self._ctx = ctx
        global voice_state_g
        voice_state_g = self
        self.current = None
        self.next = asyncio.Event()
        self.songs = SongQueue()
        self.exists = True

        self._loop = False
        self._autoplay = True
        self._volume = 0.5
        self.audio_player = bot.loop.create_task(self.audio_player_task())
        mp_em.event_attach(vlc.EventType.MediaPlayerEndReached, self.play_song_if_ended)

    def __del__(self):
        self.audio_player.cancel()


    
    @property
    def loop(self):
        return self._loop

    @loop.setter
    def loop(self, value: bool):
        self._loop = value

    @property
    def autoplay(self):
        return self._autoplay

    @autoplay.setter
    def autoplay(self, value: bool):
        self._autoplay = value

    @property
    def volume(self):
        return self._volume

    @volume.setter
    def volume(self, value: float):
        self._volume = value

    @property
    def is_playing(self):
        return player.get_state()==3  

    async def audio_player_task(self):
        while True:
            print("WHAT DOING")
            self.next.clear()
            self.now = None
            print("AAAAAAA")
            print(player.get_state())
            if self.loop == False: 
                print("waiting for song")               
                self.current = await self.songs.get()

                print("got song")
                self.current.source.volume = self._volume
                vlc_playNow(self.current.source.stream_url)
                await send_to_mq(self.current.source.title)
                print("bruh")
            #If the song is looped
            elif self.loop == True:
                #self.now = discord.FFmpegPCMAudio(self.current.source.stream_url, **YTDLSource.FFMPEG_OPTIONS)
                vlc_playNow(self.current.source.stream_url)
                await send_to_mq(self.current.source.title)

            print("now waiting")
            await self.next.wait()

    def play_next_song(self):
        self.next.set()

    def play_song_if_ended(self,event):
        print("Song ended.")
        self.play_next_song()   

    def skip(self):
        print(self.songs)
        if self.songs.empty():
            player.stop()
        elif self.is_playing:
                self.play_next_song()
        

    async def stop(self):
        self.songs.clear()
        player.stop()
        self.current = None

      
#async def play_rafado(search: str):
#    try:
#        source = await YTDLSource.create_source_rafado(search=search, loop=bot.loop)
#    except YTDLError as e:
#        pass
#    else:
#        song = Song(source)
#        await voice_state_g.songs.put(song)
#        print("requested")
#        if player.get_state() == 0 or player.get_state() == 6 or player.get_state() == 5:
#            print("playing NOW")
#            voice_state_g.play_next_song()


class Music(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.voice_states = {}



    def get_voice_state(self, ctx: commands.Context):
        state = self.voice_states.get(ctx.guild.id)
        if not state or not state.exists:
            state = VoiceState(self.bot, ctx)
            self.voice_states[ctx.guild.id] = state

        return state


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

    @commands.Cog.listener()
    async def on_message(self, message):
            print(f"{message.guild}/{message.channel}/{message.author.name}>{message.content}")
            if message.embeds:
                print(message.embeds[0].to_dict())
            


    @commands.command(name='join', invoke_without_subcommand=True)
    async def _join(self, ctx: commands.Context):
        """Joins a voice channel."""
        pass
        #destination = ctx.author.voice.channel
        #if ctx.voice_state.voice:
        #    await ctx.voice_state.voice.move_to(destination)
        #    return

        #ctx.voice_state.voice = await destination.connect()

  
    @commands.command(name='volume')
    @commands.is_owner()
    async def _volume(self, ctx: commands.Context, *, volume: int):
        """Sets the volume of the player."""

        if not ctx.voice_state.is_playing:
            return await ctx.send('Nothing being played at the moment.')

        if 0 > volume > 100:
            return await ctx.send('Volume must be between 0 and 100')

        ctx.voice_state.volume = volume / 100
        await ctx.send('Volume of the player set to {}%'.format(volume))

    @commands.command(name='debug')
    @commands.is_owner()
    async def _debug(self, ctx: commands.Context):
        """Sets the volume of the player."""
        for key, value in self.voice_states.items():
            print(value.__dict__)

    @commands.command(name='now', aliases=['current', 'playing','np'])
    async def _now(self, ctx: commands.Context):
        """Displays the currently playing song."""
        await ctx.send("Este comando não funciona, usa Shazam :(")

    @commands.command(name='pause', aliases=['pa'])
    @commands.has_permissions(manage_guild=True)
    async def _pause(self, ctx: commands.Context):
        """Pauses the currently playing song."""
        print(">>>Pause Command:")
        if player.get_state() == vlc.State.Playing:
            player.pause()            
            await ctx.message.add_reaction('⏯')

    @commands.command(name='resume', aliases=['re', 'res'])
    @commands.has_permissions(manage_guild=True)
    async def _resume(self, ctx: commands.Context):
        """Resumes a currently paused song."""

        if player.get_state() == vlc.State.Paused:
            player.pause()            
            await ctx.message.add_reaction('⏯')

    @commands.command(name='stop')
    async def _stop(self, ctx: commands.Context):
        """Stops playing song and clears the queue."""

        ctx.voice_state.songs.clear()

        if ctx.voice_state.is_playing:
            await  ctx.voice_state.stop()
            await ctx.message.add_reaction('⏹')

    @commands.command(name='skip', aliases=['s'])
    async def _skip(self, ctx: commands.Context):
        """skip a song.
        """

        if not ctx.voice_state.is_playing:
            return await ctx.send('Not playing any music right now...')
        await ctx.message.add_reaction('⏭')
       
        ctx.voice_state.skip()


    @commands.command(name='queue')
    async def _queue(self, ctx: commands.Context, *, page: int = 1):
        """Shows the player's queue.
        You can optionally specify the page to show. Each page contains 10 elements.
        """

        if len(ctx.voice_state.songs) == 0:
            return await ctx.send('Empty queue.')

        items_per_page = 10
        pages = math.ceil(len(ctx.voice_state.songs) / items_per_page)

        start = (page - 1) * items_per_page
        end = start + items_per_page

        queue = ''
        for i, song in enumerate(ctx.voice_state.songs[start:end], start=start):
            queue += '`{0}.` [**{1.source.title}**]({1.source.url})\n'.format(i + 1, song)

        embed = (discord.Embed(description='**{} tracks:**\n\n{}'.format(len(ctx.voice_state.songs), queue))
                 .set_footer(text='Viewing page {}/{}'.format(page, pages)))
        await ctx.send(embed=embed)

    @commands.command(name='shuffle')
    async def _shuffle(self, ctx: commands.Context):
        """Shuffles the queue."""

        if len(ctx.voice_state.songs) == 0:
            return await ctx.send('Empty queue.')

        ctx.voice_state.songs.shuffle()
        await ctx.message.add_reaction('✅')

    @commands.command(name='remove')
    async def _remove(self, ctx: commands.Context, index: int):
        """Removes a song from the queue at a given index."""

        if len(ctx.voice_state.songs) == 0:
            return await ctx.send('Empty queue.')

        ctx.voice_state.songs.remove(index - 1)
        await ctx.message.add_reaction('✅')

    @commands.command(name='loop')
    async def _loop(self, ctx: commands.Context):
        """Loops the currently playing song.
        Invoke this command again to unloop the song.
        """

        if not ctx.voice_state.is_playing:
            return await ctx.send('Nothing being played at the moment.')

        # Inverse boolean value to loop and unloop.
        ctx.voice_state.loop = not ctx.voice_state.loop
        await ctx.message.add_reaction('✅')
        await ctx.send('Looping a song is now turned ' + ('on' if ctx.voice_state.loop else 'off') )

    @commands.command(name='autoplay')
    async def _autoplay(self, ctx: commands.Context):
        """Automatically queue a new song that is related to the song at the end of the queue.
        Invoke this command again to toggle autoplay the song.
        """

        if not ctx.voice_state.is_playing:
            return await ctx.send('Nothing being played at the moment.')

        # Inverse boolean value to loop and unloop.
        ctx.voice_state.autoplay = not ctx.voice_state.autoplay
        await ctx.message.add_reaction('✅')
        await ctx.send('Autoplay after end of queue is now ' + ('on' if ctx.voice_state.autoplay else 'off') )

   


    @commands.command(name='play', aliases=['p'])
    async def _play(self, ctx: commands.Context, *, search: str):
        async with ctx.typing():
            try:
                source = await YTDLSource.create_source(ctx, search, loop=self.bot.loop)
            except YTDLError as e:
                pass
            else:
                #if not ctx.voice_state.voice:
                #    await ctx.invoke(self._join)
                song = Song(source)
                await ctx.voice_state.songs.put(song)
                await ctx.send('Enqueued {}'.format(str(source)))
                print("requested")
                if player.get_state() == 0 or player.get_state() == 6 or player.get_state() == 5:
                    print("playing NOW")
                    ctx.voice_state.play_next_song()
                #if ctx.voice_state.current is None:
                #    ctx.voice_state.current = song



    @commands.command(name='search')
    async def _search(self, ctx: commands.Context, *, search: str):
        """Searches youtube.
        It returns an imbed of the first 10 results collected from youtube.
        Then the user can choose one of the titles by typing a number
        in chat or they can cancel by typing "cancel" in chat.
        Each title in the list can be clicked as a link.
        """
        async with ctx.typing():
            try:
                source = await YTDLSource.search_source(ctx, search, loop=self.bot.loop)
            except YTDLError as e:
                await ctx.send('An error occurred while processing this request: {}'.format(str(e)))
            else:
                if source == 'sel_invalid':
                    await ctx.send('Invalid selection')
                elif source == 'cancel':
                    await ctx.send(':white_check_mark:')
                elif source == 'timeout':
                    await ctx.send(':alarm_clock: **Time\'s up bud**')
                else:
                    #if not ctx.voice_state.voice:
                    #    await ctx.invoke(self._join)

                    song = Song(source)
                    await ctx.voice_state.songs.put(song)
                    await ctx.send('Enqueued {}'.format(str(source)))

            
    @_join.before_invoke
    @_play.before_invoke
    async def ensure_voice_state(self, ctx: commands.Context):
        pass
        #if not ctx.author.voice or not ctx.author.voice.channel:
        #    raise commands.CommandError('You are not connected to any voice channel.')
#
        #if ctx.voice_client:
        #    if ctx.voice_client.channel != ctx.author.voice.channel:
        #        raise commands.CommandError('Bot is already in a voice channel.')




class UnfilteredBot(commands.Bot):
    """An overridden version of the Bot class that will listen to other bots."""

    async def process_commands(self, message):
        """Override process_commands to listen to bots."""
        ctx = await self.get_context(message)
        await self.invoke(ctx)

async def main():
    intents = discord.Intents.default()
    intents.members = True
    intents.presences = True
    intents.messages = True
    intents.typing = True
    intents.message_content = True
    global bot
    global conn
    conn = await aiormq.connect("amqp://192.168.2.20/")
    global channel
    channel = await conn.channel()

    bot =  UnfilteredBot(command_prefix='?', case_insensitive=True, description="The Superior Bot",intents=intents)
    
    async def send_messages():
            declare_ok = await channel.queue_declare("nerde/np", auto_delete=False)
    

    @bot.event
    async def on_ready():
        print('Logged in as:\n{0.user.name}\n{0.user.id}'.format(bot))

    @bot.command(name='botstop', aliases=['bstop'])
    @commands.is_owner()
    async def botstop(ctx):
        print('Goodbye')
        await ctx.send('Goodbye')
        await bot.logout()
        return
    
    @bot.command(name='fix', aliases=['repair'])
    @commands.is_owner()
    async def fixbot(ctx):
        print('Fixing...')
        await ctx.send('Fixing myself...')
        os.execv(sys.executable, ['python3'] + sys.argv) 
        return
    
    async with bot:
        await bot.add_cog(Music(bot))
        discord.utils.setup_logging(level=logging.INFO, root=False,handler=handler)
        await bot.start(token)


        

    

import asyncio

asyncio.run(main())
