import re
import datetime
import asyncpg
import statistics
import math
from random import choice
from collections import defaultdict
from discord.ext import commands
from discord.utils import get
from discord import Intents
from discord import File
from matching import find_closest_match_and_score, rank_matches
from config import bot_token, PSQL_CREDENTIALS
from scraping.ebert import ebert_lookup
import plotting
from bot_narrate import NarrationCog
from bot_helpers import fetch_as_dict, get_user_id, get_guild_id
from make_melonbot_db import make_db

make_db() # update db tables. creates & closes its own conn

class Core(commands.Cog):
    @commands.command()
    async def add(self, ctx, *movie_title):
        """<movie title> — Add a movienight suggestion."""
        movie_title = " ".join(movie_title)
        user_id = await get_user_id(ctx, db_pool)
        guild_id = await get_guild_id(ctx, db_pool)
        existing_movie = await find_exact_movie(guild_id, movie_title)
        if existing_movie:
            if existing_movie['watched'] == 1:
                return await ctx.send(f"'{existing_movie['title']}' has already been rated.")
            if existing_movie['watched'] == 0:
                return await endorse_suggestion(ctx, guild_id, existing_movie['title'], user_id) # this will handle messaging back to user
            else:
                return await ctx.send("a terrible thing has happened here.") # watched was neither 0 nor 1
        try:
            async with db_pool.acquire() as connection:
                await connection.execute("INSERT INTO movies (guild_id, title, user_id, watched) values ($1,$2,$3,$4)", guild_id, movie_title, user_id, 0)
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        return await send_goodly(ctx, f"'{movie_title}' has been added.")
                
    @commands.command()
    async def remove(self, ctx, *movie_title):
        """<movie title> — Remove a suggestion."""
        movie_title = " ".join(movie_title)
        guild_id = await get_guild_id(ctx, db_pool)
        existing_movie = await find_exact_movie(guild_id, movie_title)
        if not existing_movie:
            return await ctx.send(f"'{movie_title}' doesn't exist.")
        elif existing_movie['watched'] == 1:
            return await ctx.send(f"'{existing_movie['title']}' has already been watched or rated and can't be removed.")
        try:
            async with db_pool.acquire() as connection:
                await connection.execute("DELETE FROM movies WHERE guild_id=$1 and title=$2 and watched=$3", guild_id, existing_movie['title'], 0)
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        return await send_goodly(ctx, f"'{existing_movie['title']}' has been deleted.")

    @commands.command()
    async def endorse(self, ctx, *movie_title):
        """<movie title> — Endorse a suggestion."""
        movie_title = " ".join(movie_title)
        user_id = await get_user_id(ctx, db_pool)
        guild_id = await get_guild_id(ctx, db_pool)
        return await endorse_suggestion(ctx, guild_id, movie_title, user_id)
        
    @commands.command()
    async def unendorse(self, ctx, *movie_title):
        """<movie title> Remove endorsement."""
        movie_title = " ".join(movie_title)
        user_id = await get_user_id(ctx, db_pool)
        guild_id = await get_guild_id(ctx, db_pool)
        existing_movie = await find_exact_movie(guild_id, movie_title)
        if not existing_movie:
            return await ctx.send(f"'{movie_title}' doesn't exist.")
        if not await movie_is_endorsed_by_user(ctx, guild_id, existing_movie['title'], user_id):
            return await ctx.send(f"You have not endorsed '{existing_movie['title']}'")
        try:
            async with db_pool.acquire() as connection:
                await connection.execute("DELETE FROM endorsements WHERE guild_id=$1 AND user_id=$2 AND movie_id=$3", guild_id, user_id, existing_movie['id'])
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        return await send_goodly(ctx, f"'You have unendorsed '{existing_movie['title']}'.")
                    
    @commands.command()
    async def rate(self, ctx, *movie_title_and_rating):
        """<movie title> <1-10> — Rate a movie."""
        user_id = await get_user_id(ctx, db_pool)
        guild_id = await get_guild_id(ctx, db_pool)
        movie_title = " ".join(movie_title_and_rating[:-1])
        existing_movie = await find_exact_movie(guild_id, movie_title)
        if not existing_movie:
            return await ctx.send(f"'{movie_title}' doesn't exist.")
        current_time = datetime.datetime.now()
        rating = movie_title_and_rating[-1]
        cutoff = str(rating).find("/10")
        if cutoff > -1:
            rating = str(rating)[:cutoff]
        rating = float(rating)
        rating = int(round(rating * 100)) / 100
        if rating < 1 or rating > 10:
            return await ctx.send("rating must be between 1 and 10")
        if existing_movie['date_watched']:
            # if movie already has date_watched, just update watched to = 1
            try:
                async with db_pool.acquire() as connection:
                    await connection.execute("UPDATE movies SET watched=$1 WHERE guild_id=$2 AND title=$3", 1, guild_id, existing_movie['title'])
            except asyncpg.exceptions.PostgresError as e:
                print(f"Database error: {e}")
                return await ctx.send("Ruh roh database error")
        else:
            # if movie has no date_watched, updated date_watched and watched
            try:
                async with db_pool.acquire() as connection:
                    await connection.execute("UPDATE movies SET watched=$1, date_watched=$2 WHERE guild_id=$3 AND title=$4", 1, current_time, guild_id, movie_title)
            except asyncpg.exceptions.PostgresError as e:
                await ctx.send("Ruh roh database error")
                print(f"Database error: {e}")
        try:
            async with db_pool.acquire() as connection:
                rows = await fetch_as_dict(connection, "SELECT rating FROM ratings where guild_id=$1 AND movie_id=$2 and user_id=$3", guild_id, existing_movie['id'], user_id)
                if rows:
                    await connection.execute("UPDATE ratings SET rating=$1 WHERE guild_id=$2 AND movie_id=$3 and user_id=$4", rating, guild_id, existing_movie['id'], user_id)
                else:
                    await connection.execute("INSERT INTO ratings (rating, guild_id, movie_id, user_id) values ($1,$2,$3,$4)", rating, guild_id, existing_movie['id'], user_id)
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        return await send_goodly(ctx, f"You rated '{existing_movie['title']}' {rating}/10.")

    @commands.command()
    async def unrate(self, ctx, *movie_title):
        """<movie title> — Remove rating."""
        user_id = await get_user_id(ctx, db_pool)
        guild_id = await get_guild_id(ctx, db_pool)
        movie_title = " ".join(movie_title)
        existing_movie = await find_exact_movie(guild_id, movie_title)
        if not existing_movie:
            return await ctx.send(f"'{movie_title}' doesn't exist.")
        try:
            async with db_pool.acquire() as connection:
                rows = await fetch_as_dict(connection, 
                    """SELECT FROM ratings WHERE guild_id=$1 AND user_id=$2 AND movie_id=$3""",
                    guild_id, user_id, existing_movie['id'])
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        if not rows:
            return await ctx.send(f"You have not yet rated '{existing_movie['title']}'.")
        try:
            async with db_pool.acquire() as connection:
                await connection.execute(
                    """DELETE FROM ratings WHERE guild_id=$1 AND user_id=$2 AND movie_id=$3""",
                    guild_id, user_id, existing_movie['id'])
                # check if any ratings are left to determine what to do next
                rows = await fetch_as_dict(connection, 
                    """SELECT FROM ratings WHERE guild_id=$1 AND movie_id=$2""",
                    guild_id, existing_movie['id'])
                if not rows:
                    # No ratings left, set watched to 0 and date_watched to 0
                    await connection.execute(
                        """UPDATE movies SET watched=$1, date_watched=$2
                           WHERE guild_id=$3 AND id=$4""",
                        0, None, guild_id, existing_movie['id'])
                    return await send_goodly(ctx, f"You have removed the last rating from '{existing_movie['title']}' and so it has been returned to suggestions.")
                else:
                    return await send_goodly(ctx, f"You have removed your rating from '{movie_title}'.")
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")

    @commands.command()
    async def review(self, ctx, movie_title, *review_text):
        """"<movie title>" <review text> — Review a movie."""
        user_id = await get_user_id(ctx, db_pool)
        guild_id = await get_guild_id(ctx, db_pool)
        review_text = " ".join(review_text)
        existing_movie = await find_exact_movie(guild_id, movie_title)
        if not existing_movie:
            return await ctx.send(f"'{movie_title}' doesn't exist.")
        ratings = await get_ratings_for_movie_ids(ctx, guild_id, [existing_movie['id']])
        user_rating = [i for i in ratings if i['user_id']==user_id]
        if not user_rating:
            return await ctx.send(f"you must rate '{existing_movie['title']}' before you can review it")
        try:
            async with db_pool.acquire() as connection:
                existing_review = await fetch_as_dict(connection, 
                    """SELECT * FROM reviews
                        INNER JOIN movies ON reviews.movie_id = movies.id
                        WHERE reviews.guild_id=$1 AND reviews.user_id=$2 AND movies.title=$3""",
                    guild_id, user_id, movie_title)
                if existing_review:
                    existing_review = existing_review[0]
                    await connection.execute("UPDATE reviews SET review_text=$1 WHERE guild_id=$2 AND user_id=$3 AND movie_id=$4",
                        review_text, guild_id, user_id, existing_movie['id'])
                else:
                    await connection.execute("INSERT INTO reviews (guild_id, movie_id, user_id, review_text) values ($1,$2,$3,$4)",
                        guild_id, existing_movie['id'], user_id, review_text)
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        return await send_goodly(ctx, f"You have reviewed {existing_movie['title']}.")

    @commands.command()
    async def transfer(self, ctx, movie_title, *name_or_mention):
        """"<movie title>" <name or mention> — Transfer movie choosership to a new person."""
        name_or_mention = " ".join(name_or_mention)
        user_id = await name_or_mention_to_id(ctx, name_or_mention)
        if not user_id:
            return await ctx.send(f"User '{name_or_mention}' not found") # CONFIRMED this works, use return await ctx.send() more.
        username = await user_id_to_username(ctx, user_id)
        if not username:
            username = str(row['user_id'])
        guild_id = await get_guild_id(ctx, db_pool)
        existing_movie = await find_exact_movie(guild_id, movie_title)
        if not existing_movie:
            return await ctx.send(f"'{movie_title}' doesn't exist.")
        if existing_movie['user_id'] == user_id:
            return await ctx.send(f"{existing_movie['title']} is already owned by {username}")
        try:
            async with db_pool.acquire() as connection:
                await connection.execute("UPDATE movies SET user_id=$1 WHERE guild_id=$2 AND title=$3", user_id, guild_id, existing_movie['title'])
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        return await send_goodly(ctx, f"'{existing_movie['title']}' choosership has been transfered to '{username}'.")

    @commands.command()
    async def find(self, ctx, *user_input):
        """<search text> <[n,p]> — Search for users or movies."""
        guild_id = await get_guild_id(ctx, db_pool)
        guild_user_info = [[member.id, member.name] for member in ctx.message.guild.members]
        user_input, pagination = await parse_squarefucker(user_input)
        user_input = " ".join(user_input)
        # STEP 1) check if the input is user_id (i.e. <@3087243312874>)
        # if yes, no need to do rest of algo
        # this is only done if not pagination cus it can only be an exact match & pagination is for browsing through a list of partial matches
        if not pagination:
            user_id = await id_from_mention(user_input.strip()) # checks if the remaining user_input is a mention
            if user_id:            
                try:
                    message = await create_found_username_message(ctx, guild_id, user_id)
                except Exception as e:
                    print(f"Error: {e}")
                    return await ctx.send("Ruh roh error")
                return await send_goodly(ctx, message)
        
        # STEP 2) rank all usernames/movie titles against user input for match closeness
        usernames = [(i[1].lower(), "username") for i in guild_user_info]
        movies = await get_all_guild_movies(ctx)
        movie_titles = [(row['title'].lower(), "movie") for row in movies]
        full_search_list = usernames + movie_titles
        ranked_matches = rank_matches(user_input, full_search_list)
        # STEP 3) determine what to return based on pagination options
        if not pagination:
            matches = [ranked_matches[0]]
            # detailed breakdown of first result
        else:
            matches = await paginate(ranked_matches, pagination[0], pagination[1])
            
        # STEP 4A) Give a detailed breakdown for a single result
        #          details will be different for username vs. movie
        if len(matches) == 1:
            match = matches[0]
            if match[1] == "username":
                user_id = await name_or_mention_to_id(ctx, match[0])
                try:
                    message = await create_found_username_message(ctx, guild_id, user_id)
                except Exception as e:
                    print(f"Error: {e}")
                    return await ctx.send("Ruh roh error")
                return await send_goodly(ctx, message)
            if match[1] == "movie":
                try:
                    message = await create_found_movie_message(ctx, guild_id, match[0])
                except Exception as e:
                    print(f"Error: {e}")
                    return await ctx.send("Ruh roh error")
                return await send_goodly(ctx, message)
                
        # STEP 4B) If there are multiple matches, just list the results and the type
        if len(matches) > 1:
            message = "------ SEARCH RESULTS FROM USERNAMES AND MOVIES ------\n"
            for word, identifier, _ in matches:
                message += f"{word} ({identifier})\n"
            return await send_goodly(ctx, message)
        return await ctx.send(f'"{user_input}" could not be found in movies or users.')

    @commands.command()
    async def change_date_watched(self, ctx, *user_input):
        """<movie title> <yyyy-mm-dd> Change the date watched for a movienight"""
        guild_id = await get_guild_id(ctx, db_pool)
        user_input = list(user_input)
        date_watched = user_input.pop().strip()
        try:
            date_watched = datetime.datetime.strptime(date_watched, "%Y-%m-%d")
        except Exception as e:
            print("e")
            return await ctx.send(f"Couldn't parse date {date_watched}.\n use tthe format yyyy-mm-dd, i.e. 2024-12-31")
        movie_title = " ".join(user_input)
        existing_movie = await find_exact_movie(guild_id, movie_title)
        if not existing_movie:
            return await ctx.send(f"'{movie_title}' doesn't exist.")
        try:
            async with db_pool.acquire() as connection:
                await connection.execute("""
                    UPDATE movies SET date_watched=$1
                    WHERE guild_id=$2
                    AND id=$3""", date_watched, guild_id, existing_movie['id'])
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        return await send_goodly(ctx, f"date watched of {existing_movie['title']} has been changed to {date_watched.strftime('%Y-%m-%d')}.")

class BrowseSuggestions(commands.Cog):
    @commands.command()
    async def suggestions(self, ctx, *user_input):
        """<name or mention> <[n,p]> — Chronological suggestions from the server or a specific user."""
        guild_id = await get_guild_id(ctx, db_pool)
        try:
            pagination, discord_id, username = await parse_user_input_for_mention(ctx, user_input)
        except Exception as e:
            print(f"Parsing User Input error: {e}")
            return await ctx.send("Ruh roh user input error")
        if not pagination:
            pagination = (15,1)
        if pagination[0] > 0:
            title_descriptor = "RECENT"
        else:
            title_descriptor = "OLDEST"
        if not discord_id:
            sql = """SELECT title, date_suggested, user_id FROM movies WHERE guild_id=$1 AND watched=$2 ORDER BY date_suggested desc"""
            sql_args = [guild_id, 0]
            title_from = "SERVER"
        else:
            sql = """SELECT title, date_suggested FROM movies WHERE guild_id=$1 AND user_id=$2 AND watched=$3 ORDER BY date_suggested desc"""
            sql_args = [guild_id, discord_id, 0]
            title_from = username
        try:
            async with db_pool.acquire() as connection:
                suggestions = await fetch_as_dict(connection, sql, *sql_args) 
                if not suggestions:
                    return await ctx.send(f"No suggestions found for user {username}")
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        suggestions = await paginate(suggestions, pagination[0], pagination[1])
        message = f"------ {title_descriptor} SUGGESTIONS FROM {title_from.upper()} ------\n"
        for suggestion in suggestions:
            if not suggestion['date_suggested']:
                date = "????-??-??"
            elif type(suggestion['date_suggested']) is datetime.datetime:
                date = suggestion['date_suggested'].strftime("%Y-%m-%d")
            else:
                date = suggestion['date_suggested']
            if "user_id" in suggestion:
                username = await user_id_to_username(ctx, suggestion['user_id'])
                username = str(suggestion['user_id']) if not username else username
                message += f"{date} - {suggestion['title']} ({username})\n"
            else:
                message += f"{date} - {suggestion['title']}\n"
        return await send_goodly(ctx, message)
    
    @commands.command()
    async def endorsed(self, ctx, *user_input):
        """<name or mention> <[n,p]> — Most-endorsed suggestions from the server or a specific user."""
        guild_id = await get_guild_id(ctx, db_pool)
        try:
            pagination, discord_id, username = await parse_user_input_for_mention(ctx, user_input)
        except Exception as e:
            print(f"Parsing User Input error: {e}")
            return await ctx.send("Ruh roh user input error")
        if not pagination:
            pagination = (15,1)
        if pagination[0] > 10:
            title_descriptor = "MOST"
        else:
            title_descriptor = "LEAST"
        if not discord_id:
            sql = """SELECT movies.id, movies.title, movies.user_id, movies.date_suggested, COUNT(endorsements.id) AS endorsement_count 
                     FROM movies
                     INNER JOIN endorsements ON endorsements.movie_id = movies.id
                     WHERE endorsements.guild_id=$1
                       AND watched=$2
                     GROUP BY movies.id"""
            sql_args = [guild_id, 0]
            title_from = "server"
        else:
            sql = """SELECT movies.id, movies.title, movies.date_suggested, COUNT(endorsements.id) AS endorsement_count 
                     FROM movies
                     INNER JOIN endorsements ON endorsements.movie_id = movies.id
                     WHERE endorsements.guild_id=$1
                     AND movies.user_id=$2
                     AND watched=$3
                     GROUP BY movies.id"""
            sql_args = [guild_id, discord_id, 0]
            title_from = username
        try:
            async with db_pool.acquire() as connection:
                suggestions = await fetch_as_dict(connection, sql, *sql_args) 
                if not suggestions:
                    return await ctx.send(f"No suggestions found for user {title_from}")
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        movies_chooser_endorsements = []
        suggestions.sort(key=lambda x: x['endorsement_count'], reverse=True)
        suggestions = await paginate(suggestions, pagination[0], pagination[1])
        message = f"------ {title_descriptor}-ENDORSED MOVIES FROM {title_from.upper()}------\n"
        for suggestion in suggestions:
            if not suggestion['date_suggested']:
                date = "????-??-??"
            elif type(suggestion['date_suggested']) is datetime.datetime:
                date = suggestion['date_suggested'].strftime("%Y-%m-%d")
            else:
                date = suggestion['date_suggested']
            if "user_id" in suggestion:
                username = await user_id_to_username(ctx, suggestion['user_id'])
                username = str(suggestion['user_id']) if not username else username
                message += f"{date} - {suggestion['title']} ({username}) ({suggestion['endorsement_count']})\n"
            else:
                message += f"{date} - {suggestion['title']} ({suggestion['endorsement_count']})"
        return await send_goodly(ctx, message)
        
    @commands.command()
    async def endorsements(self, ctx, *user_input):
        """<name or mention> <[n,p]> — Chronological endorsements from a user."""
        guild_id = await get_guild_id(ctx, db_pool)
        try:
            pagination, discord_id, username = await parse_user_input_for_mention(ctx, user_input)
        except Exception as e:
            print(f"Parsing User Input error: {e}")
            return await ctx.send("Ruh roh user input error")
        if not pagination:
            pagination = (15,1)
        if pagination[0] > 10:
            title_descriptor = "RECENT"
        else:
            title_descriptor = "OLDEST"
        if not discord_id:
            discord_id = ctx.message.author.id
            username = await user_id_to_username(ctx, discord_id)
            if not username:
                username = str(discord_id)
        try:
            async with db_pool.acquire() as connection:
                endorsements = await fetch_as_dict(connection, 
                    """SELECT movies.title, endorsements.date FROM movies
                       INNER JOIN endorsements ON endorsements.movie_id = movies.id
                       WHERE endorsements.guild_id=$1 AND endorsements.user_id=$2 and watched=$3 ORDER BY endorsements.date desc""",
                    guild_id, discord_id, 0)
                if not endorsements:
                    return await ctx.send(f"No endorsements found for user {username}")
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        endorsements = await paginate(endorsements, pagination[0], pagination[1])
        message = f"------ {title_descriptor} ENDORSEMENTS FROM {username.upper()} ------\n"
        for endorsement in endorsements:
            if not endorsement['date']:
                date = "????-??-??"
            else:
                date = endorsement['date'].strftime("%Y-%m-%d")
            message += f"{date} - {endorsement['title']}\n"
        return await send_goodly(ctx, message)

    @commands.command()
    async def random(self, ctx, *_):
        """Random movie."""
        guild_id = await get_guild_id(ctx, db_pool)
        try:
            async with db_pool.acquire() as connection:
                suggestions = await fetch_as_dict(connection, 
                    """SELECT title FROM movies WHERE guild_id=$1 AND watched=$2""", guild_id, 0)
                if not suggestions:
                    return await ctx.send(f"No suggestions found in this server")
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        titles = [i['title'] for i in suggestions]
        random_title = choice(titles)
        return await send_goodly(ctx, random_title)

class BrowseMovienights(commands.Cog):
    @commands.command()
    async def movienights(self, ctx, *user_input):
        """<name or mention> <[n,p]> — Chronological movienights from the server or a specific user."""
        guild_id = await get_guild_id(ctx, db_pool)
        try:
            pagination, discord_id, username = await parse_user_input_for_mention(ctx, user_input)
        except Exception as e:
            print(f"Parsing User Input error: {e}")
            return await ctx.send("Ruh roh user input error")
        if not pagination:
            pagination = (15,1)
        if pagination[0] > 0:
            title_descriptor = "MOST RECENT"
        else:
            title_descriptor = "OLDEST"
        if not discord_id:
            sql = """SELECT
                        movies.id,
                        movies.title,
                        movies.user_id,
                        movies.date_watched,
                        AVG(ratings.rating) as avg_rating
                     FROM movies
                     JOIN ratings ON movies.id=ratings.movie_id
                     WHERE movies.guild_id=$1
                       AND watched=$2
                     GROUP BY movies.id
                     ORDER BY movies.date_watched DESC"""
            sql_args = [guild_id, 1]
            title_from = "SERVER"
        else:
            sql = """SELECT
                        movies.id,
                        movies.title,
                        movies.date_watched,
                        AVG(ratings.rating) as avg_rating
                     FROM movies
                     JOIN ratings ON movies.id=ratings.movie_id
                     WHERE movies.guild_id=$1
                       AND watched=$2
                       AND movies.user_id=$3
                     GROUP BY movies.id
                     ORDER BY movies.date_watched DESC"""
            sql_args = [guild_id, 1, discord_id]
            title_from = username
        try:
            async with db_pool.acquire() as connection:
                movies = await fetch_as_dict(connection, sql, *sql_args)
                if not movies:
                    return await ctx.send(f"No watched movies found for user {username}")
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        movie_ids = [movie['id'] for movie in movies] 
        movies = await paginate(movies, pagination[0], pagination[1])
        message = f"------ {title_descriptor} MOVIENIGHTS FROM {title_from.upper()} ------\n"
        for movie in movies:
            if movie['avg_rating']:
                average = movie['avg_rating']
            else:
                average = "0"
            date_watched = movie['date_watched']
            if not date_watched:
                date_watched = "????-??-??"
            else:
                date_watched = date_watched.strftime("%Y-%m-%d")
            if 'user_id' in movie:
                username = await user_id_to_username(ctx, movie['user_id'])
                username = str(movie['user_id']) if not username else username
                message += f"{date_watched} - {movie['title']} ({username}): {average:.1f}\n"
            else:
                message += f"{date_watched} - {movie['title']}: {average:.1f}\n"
        return await send_goodly(ctx, message)
 
    @commands.command()
    async def top_movienights(self, ctx, *user_input):
        """<name or mention> <[n,p]> — Highest-rated movies from the server or a specific user."""
        guild_id = await get_guild_id(ctx, db_pool)
        try:
            pagination, discord_id, username = await parse_user_input_for_mention(ctx, user_input)
        except Exception as e:
            print(f"Parsing User Input error: {e}")
            return await ctx.send("Ruh roh user input error")
        if not pagination:
            pagination = (15,1)
        if pagination[0] > 0:
            title_descriptor = "HIGHEST"
        else:
            title_descriptor = "LOWEST"
        if not discord_id:
            sql = """SELECT
                        movies.id,
                        movies.title,
                        movies.user_id,
                        movies.date_watched,
                        AVG(ratings.rating) as avg_rating
                     FROM movies
                     JOIN ratings ON movies.id=ratings.movie_id
                     WHERE movies.guild_id=$1
                       AND watched=$2
                     GROUP BY movies.id"""
            sql_args = [guild_id, 1]
            title_from = "SERVER"
        else:
            sql = """SELECT
                        movies.id,
                        movies.title,
                        movies.date_watched,
                        AVG(ratings.rating) as avg_rating
                     FROM movies
                     JOIN ratings ON movies.id=ratings.movie_id
                     WHERE movies.guild_id=$1
                       AND movies.watched=$2
                       AND movies.user_id=$3
                     GROUP BY movies.id"""
            sql_args = [guild_id, 1, discord_id]
            title_from = username
        try:
            async with db_pool.acquire() as connection:
                movies = await fetch_as_dict(connection, sql, *sql_args)
                if not movies:
                    return await ctx.send(f"No watched movies found for user {username}")
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        movies.sort(key=lambda x: x['avg_rating'], reverse=True)
        movies = await paginate(movies, pagination[0], pagination[1])
        message = f"------ {title_descriptor}-RATED MOVIENIGHTS FROM {title_from.upper()} ------\n"
        for movie in movies:
            average = movie['avg_rating']
            if not average:
                average = 0 
            date_watched = movie['date_watched']
            if not date_watched:
                date_watched = "????-??-??"
            else:
                date_watched = date_watched.strftime("%Y-%m-%d")
            if 'user_id' in movie:
                username = await user_id_to_username(ctx, movie['user_id'])
                username = str(movie['user_id']) if not username else username
                message += f"{date_watched} - {movie['title']} ({username}): {average:.1f}\n"
            else:
                message += f"{date_watched} - {movie['title']}: {average:.1f}\n"
        return await send_goodly(ctx, message)
        
    @commands.command()
    async def ratings(self, ctx, *user_input):
        """<name or mention> <[n,p]> — Chronological ratings given by a user."""
        guild_id = await get_guild_id(ctx, db_pool)
        try:
            pagination, discord_id, username = await parse_user_input_for_mention(ctx, user_input)
        except Exception as e:
            print(f"Parsing User Input error: {e}")
            return await ctx.send("Ruh roh user input error")
        if not pagination:
            pagination = (15,1)
        if pagination[0] > 0:
            title_descriptor = "MOST RECENT"
        else:
            title_descriptor = "OLDEST"
        if not discord_id:
            discord_id = ctx.message.author.id
            username = await user_id_to_username(ctx, discord_id)
            if not username:
                username = str(discord_id)
        try:
            async with db_pool.acquire() as connection:
                ratings = await fetch_as_dict(connection, 
                    """SELECT movies.title, movies.date_watched, ratings.rating FROM ratings
                       INNER JOIN movies ON ratings.movie_id = movies.id
                       WHERE ratings.guild_id=$1 AND ratings.user_id=$2
                       ORDER BY movies.date_watched desc""", guild_id, discord_id)
                if not ratings:
                    return await ctx.send(f"No ratings found for user {username}")
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        all_ratings_from_rater = [row['rating'] for row in ratings]
        overall_average = sum(all_ratings_from_rater)/len(all_ratings_from_rater)
        ratings = await paginate(ratings, pagination[0], pagination[1])
        message = f"{title_descriptor} RATINGS FROM {username.upper()} (avg: {overall_average:.1f})\n"
        for rating in ratings:
            date_watched = rating['date_watched']
            if not date_watched:
                date_watched = "????-??-??"
            else:
                date_watched = date_watched.strftime("%Y-%m-%d")
            message += f"{date_watched} - {rating['title']}: {rating['rating']:.1f}\n"
        return await send_goodly(ctx, message)
   
    @commands.command()
    async def top_ratings(self, ctx, *user_input):
        """<name or mention> <[n,p]> — Highest ratings given by a user."""
        guild_id = await get_guild_id(ctx, db_pool)
        try:
            pagination, discord_id, username = await parse_user_input_for_mention(ctx, user_input)
        except Exception as e:
            print(f"Parsing User Input error: {e}")
            return await ctx.send("Ruh roh user input error")
        if not pagination:
            pagination = (15,1)
        if pagination[0] > 0:
            title_descriptor = "HIGHEST"
        else:
            title_descriptor = "LOWEST"
        if not discord_id:
            discord_id = ctx.message.author.id
            username = await user_id_to_username(ctx, discord_id)
            if not username:
                username = str(discord_id)
        try:
            async with db_pool.acquire() as connection:
                ratings = await fetch_as_dict(connection, 
                    """SELECT movies.title, movies.date_watched, ratings.rating FROM ratings
                       INNER JOIN movies ON ratings.movie_id = movies.id
                       WHERE ratings.guild_id=$1 AND ratings.user_id=$2""",
                        guild_id, discord_id)
                if not ratings:
                    return await ctx.send(f"No ratings found for user {username}")
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        all_ratings_from_rater = [row['rating'] for row in ratings]
        overall_average = sum(all_ratings_from_rater)/len(all_ratings_from_rater)
        ratings.sort(key = lambda x: x['rating'], reverse=True)
        ratings = await paginate(ratings, pagination[0], pagination[1])
        message = f"{title_descriptor} RATINGS FROM {username.upper()} (avg: {overall_average:.1f})\n"
        for rating in ratings:
            date_watched = rating['date_watched']
            if not date_watched:
                date_watched = "????-??-??"
            else:
                date_watched = date_watched.strftime("%Y-%m-%d")
            message += f"{date_watched} - {rating['title']}: {rating['rating']:.1f}\n"
        return await send_goodly(ctx, message)
 
    @commands.command()
    async def unrated(self, ctx, *user_input):
        """<name or mention> <[n,p]> — Chronological unrated movies from a user."""
        guild_id = await get_guild_id(ctx, db_pool)
        try:
            pagination, discord_id, username = await parse_user_input_for_mention(ctx, user_input)
        except Exception as e:
            print(f"Parsing User Input error: {e}")
            return await ctx.send("Ruh roh user input error")
        if not pagination:
            pagination = (15,1)
        if not discord_id:
            discord_id = ctx.message.author.id
            username = await user_id_to_username(ctx, discord_id)
            if not username:
                username = str(discord_id)
        try:
            async with db_pool.acquire() as connection:
                unrated_movies = await fetch_as_dict(connection, 
                    """SELECT title, date_watched from movies WHERE guild_id=$1 AND watched=$2 AND id NOT IN
                        (SELECT DISTINCT movie_id FROM ratings WHERE guild_id=$3 AND user_id=$4)
                    ORDER BY date_watched desc""", guild_id, 1, guild_id, discord_id)
                if not unrated_movies:
                    return await ctx.send(f"No unrated movies found for user {username}")
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        unrated_movies = await paginate(unrated_movies, pagination[0], pagination[1])
        message = f"------ UNRATED MOVIES FROM {username.upper()} ------\n"
        for movie in unrated_movies:
            date_watched = movie['date_watched']
            if not date_watched:
                date_watched = "????-??-??"
            else:
                date_watched = date_watched.strftime("%Y-%m-%d")
            message += f"{date_watched} - {movie['title']}\n"
        return await send_goodly(ctx, message)
            
    @commands.command()
    async def reviews(self, ctx, *user_input):
        """<search text> <[n,p]> — Search for reviews."""
        # uses diff user input parsing cus it expects a list of keywords
        guild_id = await get_guild_id(ctx, db_pool)
        guild_user_info = [[member.id, member.name] for member in ctx.message.guild.members]
        user_input, pagination = await parse_squarefucker(user_input)
        if not pagination:
            pagination = (5,1)
            
        mention_user_ids = []
        search_terms = []
        
        # separate mentions into a different list
        for i in user_input:
            user_id = await id_from_mention(i)
            if user_id:
                mention_user_ids.append(user_id)
            else:
                search_terms.append(i)
                
        guild_reviews = await get_all_guild_reviews(ctx)
        review_scores = []
        for review in guild_reviews:
            review_score = 0 
            search_terms_copy = search_terms.copy()
            primary_targets = []
            secondary_targets = review['review_text'].split(" ")
            matched_reviewer_by_mention = None  # score 100% on reviewer if a mention matches
            primary_target_matches = []
            secondary_target_matches = []
            primary_target_misses = []  # unmatched words which will reduce final score
            # build primary targets (movie title + reviewer name)
            movie = await find_movie_by_id(guild_id, review['movie_id'])
            review['movie'] = movie  # add to review dict so it can be accessed later
            primary_targets += movie['title'].split(" ")
            reviewer_name = await user_id_to_username(ctx, review['user_id'])
            if not reviewer_name:
                reviewer_name = str(review['user_id'])
            review['reviewer_name'] = reviewer_name  # add to review dict so it can be accessed later
            if review['user_id'] in mention_user_ids:
                # if matching mention provided, dont include reviewer name in primary targets 
                matched_reviewer_by_mention = review['user_id']
            else:
                primary_targets += reviewer_name.split(" ")
            
            while primary_targets:
                matching_search_terms_for_target = []
                for target in primary_targets:
                    matching_search_term, score = find_closest_match_and_score(target, search_terms_copy) # default threshold is 50% 
                    if matching_search_term:
                        matching_search_terms_for_target.append((matching_search_term, score, target))
                if matching_search_terms_for_target:
                    # keep only the highest matching_search_term and then restart the loop
                    highest_matching_search_term, score, target = max(matching_search_terms_for_target, key=lambda x: x[1])
                    primary_target_matches.append((highest_matching_search_term, score))
                    search_terms_copy.remove(highest_matching_search_term) # removes the just word once, in the case that there are multiples of it
                    primary_targets.remove(target)
                else:
                    # no matching_search_term, all targets can be disregarded
                    primary_target_misses += primary_targets
                    primary_targets = []
            if search_terms_copy:
            # if there are still words left, that means that the matching against the movie title/reviewer name
                # didn't exhaust the user's inputs, so we should look in review text as well
                while secondary_targets:                
                    matching_search_terms_for_target = []
                    for target in secondary_targets:
                        matching_search_term, score = find_closest_match_and_score(target, search_terms_copy) # default threshold is 50% 
                        if matching_search_term:
                            matching_search_terms_for_target.append((matching_search_term, score, target))
                    if matching_search_terms_for_target:
                        highest_matching_search_term, score, target = max(matching_search_terms_for_target, key=lambda x: x[1])
                        secondary_target_matches.append((highest_matching_search_term, score))
                        search_terms_copy.remove(highest_matching_search_term)
                        secondary_targets.remove(target)
                    else:
                        # no matching_search_term, all targets can be disregarded
                        secondary_targets = []
            # calc score
            review_score = 0
            primary_target_score = 0
            primary_target_match_length = 0
            primary_target_miss_length = 0
            for word, score in primary_target_matches:
                primary_target_match_length += len(word) * score  # score is a number from 0-1 that measures how closely the user's search term matched the target word
            primary_target_miss_length = sum([len(i) for i in primary_target_misses])
            primary_target_score = 100 * primary_target_match_length / (primary_target_match_length + primary_target_miss_length)
            if matched_reviewer_by_mention:
                primary_target_score = primary_target_score / 2 # change max score from primary to 50 instead of 100
                primary_target_score += 50
            secondary_target_score = 0
            for word, score in secondary_target_matches:
                secondary_target_score += len(word) * score * 5 # up to 5 points per letter of matching words. "i hated this movie" = 15 chars = 75 points.
            for word in search_terms_copy:
                # remaining unmatched search terms from user input
                secondary_target_score -= len(word) * 5
            if secondary_target_score < 0:
                secondary_target_score = 0      
            review_score = primary_target_score + secondary_target_score
            review_scores.append((review, review_score))
            
        reviews = sorted(review_scores, key=lambda x: x[1], reverse=True)
        reviews = await paginate(reviews, pagination[0], pagination[1])
        message = "------ SEARCH RESULTS FROM REVIEWS ------\n"
        for review, _ in reviews:
            ratings = await get_ratings_for_movie_ids(ctx, guild_id, [review['movie']['id']])
            reviewer_rating = [i for i in ratings if i['user_id']==review['user_id']]
            if reviewer_rating:
                reviewer_rating = reviewer_rating[0]['rating']
            message += f"{review['movie']['title'].upper()} - {reviewer_rating}/10\n- by {review['reviewer_name']}\n{review['review_text']}\n{'-'*60}\n"
        return await send_goodly(ctx, message)
        
    @commands.command()
    async def standings(self, ctx, *user_input):
        """<[n,p]> — Chooser rankings (avg rating received)."""
        guild_id = await get_guild_id(ctx, db_pool)
        pagination = await parse_user_input_for_number_or_pagination(user_input)
        if not pagination:
            pagination = (15,1)
        try:
            async with db_pool.acquire() as connection:
                watched_movies = await fetch_as_dict(connection, 
                    """SELECT movies.user_id, movies.title, ratings.rating FROM ratings
                       INNER JOIN movies ON ratings.movie_id=movies.id
                       WHERE movies.guild_id=$1 AND movies.watched=$2""", guild_id, 1)
                if not watched_movies:
                    return await ctx.send(f"No watched movies found in this server")
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        watched_movies.sort(key=lambda x: str(x['user_id']))
        standings_data = []
        ratings_for_current_chooser = []
        for row in watched_movies:
            if len(ratings_for_current_chooser) == 0:
                current_chooser = row['user_id']
                ratings_for_current_chooser = [row['rating']]
                titles = [row['title']]
            else:
                if row['user_id'] == current_chooser:
                    ratings_for_current_chooser.append(row['rating'])
                    titles.append(row['title'])
                else:
                    average_rating = sum(ratings_for_current_chooser) / len(ratings_for_current_chooser)
                    movie_count = len(set(titles))
                    standings_data.append([current_chooser, average_rating, movie_count])
                    current_chooser = row['user_id']
                    ratings_for_current_chooser = [row['rating']]
                    titles = [row['title']]
        average_rating = sum(ratings_for_current_chooser) / len(ratings_for_current_chooser)
        movie_count = len(set(titles))
        standings_data.append([current_chooser, average_rating, movie_count])
        standings_data.sort(key=lambda x: float(x[1]), reverse=True)
        standings_data = await paginate(standings_data, pagination[0], pagination[1])
        message = "------ OVERALL STANDINGS ------\n"
        for user_id, average_rating, movie_count in standings_data:
            if movie_count > 0:
                average = '{:02.1f}'.format(float(average_rating))
                username = await user_id_to_username(ctx, user_id)
                if not username:
                    username = str(user_id)
                message += f"{username} ({str(movie_count)}): {average}\n"
        return await send_goodly(ctx, message)

    @commands.command()
    async def attendance(self, ctx, *user_input):
        """<[n,p]> — Movies ranked by attendance."""
        guild_id = await get_guild_id(ctx, db_pool)
        pagination = await parse_user_input_for_number_or_pagination(user_input)
        if not pagination:
            pagination = (15,1)
        if pagination[0] > 0:
            title_descriptor = "BIGGEST"
        else:
            title_descriptor = "SMALLEST"
        try:
            async with db_pool.acquire() as connection:
                movies = await fetch_as_dict(connection, 
                    """SELECT
                        movies.id,
                        movies.title,
                        movies.user_id,
                        movies.date_watched,
                        AVG(ratings.rating) as avg_rating,
                        COUNT(ratings.rating) as attendance
                     FROM movies
                     JOIN ratings ON movies.id=ratings.movie_id
                     WHERE movies.guild_id=$1
                       AND watched=$2
                     GROUP BY movies.id
                     ORDER BY movies.date_watched DESC""", guild_id, 1)
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        movies.sort(key=lambda x: x['attendance'], reverse=True)
        movies = await paginate(movies, pagination[0], pagination[1])
        message = f"------ {title_descriptor} MOVIE NIGHTS ------\n"
        for movie in movies:
            if not movie['date_watched']:
                date_watched = "????-??-??"
            else:
                date_watched = movie['date_watched'].strftime("%Y-%m-%d")
            username = await user_id_to_username(ctx, movie['user_id'])
            if not username:
                username = str(movie['user_id'])
            message += f"{date_watched} {movie['title']} ({username}): {movie['attendance']}\n"
        return await send_goodly(ctx, message)

    @commands.command()
    async def seen(self, ctx, *_):
        """Total movies watched in this server."""
        guild_id = await get_guild_id(ctx, db_pool)
        try:
            async with db_pool.acquire() as connection:
                rows = await fetch_as_dict(connection, 
                    """SELECT COUNT(*) FROM movies WHERE guild_id=$1 AND watched=$2""", guild_id, 1)
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        if rows:
            movie_watched_count = rows[0]["count"]  # Extract the integer count
            if movie_watched_count == 1:
                message = "One movie has been seen"
            else:
                message = f"{movie_watched_count} movies have been seen!"
            return await send_goodly(ctx, message)
        else:
            return await ctx.send("No results returned.")
    
class Scraping(commands.Cog):
    @commands.command()
    async def ebert(self, ctx, *movie):
        """<movie title> — Return a Rogert Ebert review for a movie."""
        movie = " ".join(movie)
        return await send_goodly(ctx, ebert_lookup(movie))
        
class Plotting(commands.Cog):
    @commands.command()
    async def plot_ratings(self, ctx, *user_input):
        """<name or mention> — Plot ratings from a user."""
        guild_id = await get_guild_id(ctx, db_pool)
        try:
            pagination, discord_id, username = await parse_user_input_for_mention(ctx, user_input)
        except Exception as e:
            print(f"Parsing User Input error: {e}")
            return await ctx.send("Ruh roh user input error")
        if not discord_id:
            discord_id = ctx.message.author.id
        try:
            async with db_pool.acquire() as connection:
                ratings = await fetch_as_dict(connection, 
                    """SELECT movies.title, movies.user_id, movies.date_watched, ratings.rating FROM ratings
                       INNER JOIN movies ON ratings.movie_id = movies.id
                       WHERE ratings.guild_id=$1 AND ratings.user_id=$2""", guild_id, discord_id)
                if not ratings:
                    return await ctx.send(f"No ratings found for user {username}")
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        user_ids_and_names = {}
        for i in ratings:
            if i['user_id'] not in user_ids_and_names:
                username = await user_id_to_username(ctx, i['user_id'])
                if not username:
                    username = str(i['user_id'])
                user_ids_and_names[i['user_id']] = username
            i[username] = user_ids_and_names[i['user_id']]
            
        image_buffer = plotting.plot_ratings_to_users(ratings)
        return await ctx.send(file=File(fp=image_buffer, filename="ratings_plot.png"))        
        
    @commands.command()
    async def plot_movienights(self, ctx, *user_input):
        """<name or mention> — Plot movienights from the server or from a specific user."""
        guild_id = await get_guild_id(ctx, db_pool)
        try:
            pagination, discord_id, username = await parse_user_input_for_mention(ctx, user_input)
        except Exception as e:
            print(f"Parsing User Input error: {e}")
            return await ctx.send("Ruh roh user input error")
        if not discord_id:
            sql = """SELECT
                        movies.id,
                        movies.title,
                        movies.user_id,
                        movies.date_watched,
                        AVG(ratings.rating) as avg_rating,
                        COUNT(ratings.rating) as attendance
                     FROM movies
                     JOIN ratings ON movies.id=ratings.movie_id
                     WHERE movies.guild_id=$1
                       AND watched=$2
                     GROUP BY movies.id
                     ORDER BY movies.date_watched DESC"""
            sql_args = [guild_id, 1]
            title_from = "SERVER"
        else:
            sql = """SELECT
                        movies.id,
                        movies.title,
                        movies.date_watched,
                        AVG(ratings.rating) as avg_rating,
                        COUNT(ratings.rating) as attendance
                     FROM movies
                     JOIN ratings ON movies.id=ratings.movie_id
                     WHERE movies.guild_id=$1
                       AND watched=$2
                       AND movies.user_id=$3
                     GROUP BY movies.id
                     ORDER BY movies.date_watched DESC"""
            sql_args = [guild_id, 1, discord_id]
            title_from = username
        try:
            async with db_pool.acquire() as connection:
                movies = await fetch_as_dict(connection, sql, *sql_args)
                if not movies:
                    return await ctx.send(f"No watched movies found for user {username}")
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        movie_ids = [movie['id'] for movie in movies] 
        data = []
        for movie in movies:
            if not movie['date_watched']:
                continue # skip data with no date
            date_watched = movie['date_watched'].strftime("%Y-%m-%d")
            if movie['avg_rating']:
                average = movie['avg_rating']
            else:
                average = 0
            if movie['attendance']:
                attendance = movie['attendance']
            else:
                attendance = 0
            data.append((date_watched, average, attendance))
        image_buffer = plotting.plot_movienights(data)
        return await ctx.send(file=File(fp=image_buffer, filename="movienights_plot.png"))        
        
        
    @commands.command()
    async def plot_favorites(self, ctx, *user_input):
        """<name or mention> — Plot average ratings given from one user to each movie owner in the server."""    
        guild_id = await get_guild_id(ctx, db_pool)
        try:
            pagination, discord_id, username = await parse_user_input_for_mention(ctx, user_input)
        except Exception as e:
            print(f"Parsing User Input error: {e}")
            return await ctx.send("Ruh roh user input error")
        if not discord_id:
            discord_id = ctx.message.author.id
            username = await user_id_to_username(ctx, discord_id)
            if not username:
                username = str(discord_id)
        try:
            async with db_pool.acquire() as connection:
                ratings = await fetch_as_dict(connection, 
                    """SELECT
                       movies.title,
                       movies.user_id as movie_owner,
                       ratings.rating,
                       ratings.user_id as rating_giver
                       FROM movies
                       JOIN ratings ON movies.id=ratings.movie_id
                       WHERE movies.guild_id=$1
                         AND watched=$2""", guild_id, 1)
                if not ratings:
                    return await ctx.send(f"No ratings found in the server")
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
            
        owner_ids_to_username = {}
        owner_ids = list(set([i['movie_owner'] for i in ratings]))
        for i in owner_ids:
            username = await user_id_to_username(ctx, i)
            if not username:
                username = str(i)
            owner_ids_to_username[i] = username
            
        owner_ratings = defaultdict(list)
        for row in ratings:
            owner = owner_ids_to_username[row["movie_owner"]]
            owner_ratings[owner].append(row["rating"])
        owner_avg_rating = {}
        for owner, rating_list in owner_ratings.items():
            owner_avg_rating[owner] = sum(rating_list) / len(rating_list)
            
        user_ratings_given_to_each_owner = defaultdict(list)
        for row in ratings:
            if row["rating_giver"] == discord_id:
                owner = owner_ids_to_username[row["movie_owner"]]
                user_ratings_given_to_each_owner[owner].append(row['rating'])
        
        user_avg_rating = {}
        for owner, rating_list in user_ratings_given_to_each_owner.items():
            user_avg_rating[owner] = sum(rating_list) / len(rating_list)
            
        image_buffer = plotting.plot_favorites(owner_avg_rating, user_avg_rating)
        return await ctx.send(file=File(fp=image_buffer, filename="ratings_plot.png"))        
        
    @commands.command()
    async def plot_user_similarity(self, ctx, min_common: int = 5):
        """<min_common> — Plot user rating similarity matrix. min_common (default 5) is minimum movies in common."""
        guild_id = await get_guild_id(ctx, db_pool)
        try:
            async with db_pool.acquire() as connection:
                ratings = await fetch_as_dict(connection, 
                    """SELECT ratings.user_id, ratings.movie_id, ratings.rating
                       FROM ratings
                       WHERE ratings.guild_id=$1""", guild_id)
                if not ratings:
                    return await ctx.send("No ratings found in this server")
                
                # Get total number of users who have rated movies
                unique_users = await fetch_as_dict(connection,
                    """SELECT COUNT(DISTINCT user_id) as user_count
                       FROM ratings
                       WHERE guild_id=$1""", guild_id)
                user_count = unique_users[0]['user_count']
                
                if user_count < 2:
                    return await ctx.send("Need at least 2 users with ratings to generate similarity plot.")
                
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")

        try:
            image_buffer = plotting.plot_user_similarity(ratings, min_common)
            return await ctx.send(file=File(fp=image_buffer, filename="user_similarity.png"))
        except ValueError as e:
            return await ctx.send(str(e))
        except Exception as e:
            print(f"Plotting error: {e}")
            return await ctx.send("Failed to generate plot. This might be due to insufficient rating data.")

    @commands.command()
    async def plot_user_similarity_test(self, ctx):
        """Test the similarity plot with synthetic data."""
        try:
            image_buffer = plotting.plot_user_similarity_test()
            return await ctx.send(file=File(fp=image_buffer, filename="user_similarity_test.png"))
        except Exception as e:
            print(f"Test plotting error: {e}")
            return await ctx.send(f"Test plot failed with error: {str(e)}")

    @commands.command()
    async def plot_movie_spread(self, ctx, *movie_title):
        """<movie title> — Plot the distribution of ratings for a movie."""
        guild_id = await get_guild_id(ctx, db_pool)
        movie_title = " ".join(movie_title)
        
        # Find the movie using existing helper
        movie = await find_exact_movie(guild_id, movie_title)
        if not movie:
            return await ctx.send(f"The movie'{movie_title}' doesn't exist.")
            
        # Get all ratings for this movie
        try:
            async with db_pool.acquire() as connection:
                ratings = await fetch_as_dict(connection,
                    """SELECT ratings.rating, ratings.user_id
                       FROM ratings
                       WHERE ratings.guild_id=$1 AND ratings.movie_id=$2""",
                    guild_id, movie['id'])
                if not ratings:
                    return await ctx.send(f"No ratings found for '{movie_title}'")
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")

        try:
            image_buffer = plotting.plot_movie_spread(movie, ratings)
            return await ctx.send(file=File(fp=image_buffer, filename="movie_spread.png"))
        except Exception as e:
            print(f"Plotting error: {e}")
            return await ctx.send("Failed to generate plot.")

async def send_goodly(ctx, message):
    """standard way of sending a MESSAGE to the stupid user"""
    try:
        messages = await chunk(message)
    except ValueError as e:
        return await ctx.send(f"somehow the basic way i am supposed to send messages broke that is very bad.\n{e}")
    for message in messages:
        await ctx.send("```ansi\n" + message + "```")
        
async def chunk(message, max_length=1900):
    """returns list of strings
    each chunk is either max_length or was separated by a newline in the original message"""
    chunks = []
    while message:
        chunk = ""
        newline_pos = None
        while (len(chunk) <= max_length) and message:
            character = message[0]
            message = message[1:]
            chunk += character
            if character == "\n":
                newline_pos = len(chunk)
        if newline_pos and message:
            extra = chunk[newline_pos:]
            message = extra + message
            chunk = chunk[:newline_pos - 1]
        chunks.append(chunk)
    return chunks
    
async def create_found_username_message(ctx, guild_id, user_id):
    """part of find()"""
    movies_watched = []
    suggestions = []
    ratings_received = []
    ratings_given = []
    try:
        async with db_pool.acquire() as connection:
            rows = await fetch_as_dict(connection, "SELECT title FROM movies WHERE guild_id=$1 AND user_id=$2 AND watched=$3", guild_id, user_id, 1)
            movies_watched = [row['title'] for row in rows]
            rows = await fetch_as_dict(connection, "SELECT title FROM movies WHERE guild_id=$1 AND user_id=$2 AND watched=$3", guild_id, user_id, 0)
            suggestions = [row['title'] for row in rows]
            rows = await fetch_as_dict(connection, 
                """SELECT ratings.rating FROM ratings
                   INNER JOIN movies ON ratings.movie_id = movies.id
                   WHERE ratings.guild_id=$1 AND movies.user_id=$2 AND movies.watched=$3""", guild_id, user_id, 1)
            ratings_received = [row['rating'] for row in rows]
            rows = await fetch_as_dict(connection, "SELECT rating FROM ratings WHERE guild_id=$1 AND user_id=$2", guild_id, user_id)
            ratings_given = [row['rating'] for row in rows]        
    except:
        pass # who cares
    matched_username = await user_id_to_username(ctx, user_id)
    if not matched_username:
        matched_username = str(user_id)
    message = f'------ {matched_username.upper()} ------\n'
    if not movies_watched:
        message += f"None of {matched_username}'s suggestions have been watched yet.\n"
    else:
        message += f"{len(movies_watched)} of {matched_username}'s suggestions {'have' if len(movies_watched) > 1 else 'has'} been watched so far.\n"
        average_score = sum(ratings_received)/len(ratings_received)
        average_score = '{:02.1f}'.format(float(average_score))
        message += f'{matched_username} receives an average score of {average_score}.\n'
    if not ratings_given:
        message += f'{matched_username} has not rated any movies.\n'
    else:
        average = sum(ratings_given)/len(ratings_given)
        average = '{:02.1f}'.format(float(average))
        message += f'{matched_username} has given {len(ratings_given)} rating{"s" if len(ratings_given) > 1 else ""}, with an average of {average}.\n'
    if not suggestions:
        message += f'{matched_username} does not currently have any movie suggestions.\n'
    else:
        message += f'{matched_username} currently has {len(suggestions)} movie suggestion{"s" if len(suggestions) > 1 else ""}.\n\n'
    message += f'find more info with !suggestions {matched_username}, !endorsements {matched_username}, !movienights {matched_username} or !ratings {matched_username}.'
    return message
    
async def create_found_movie_message(ctx, guild_id, matched_movie_title):    
    """part of find()"""
    async with db_pool.acquire() as connection:
        rows = await fetch_as_dict(connection, "SELECT id, user_id, watched, date_watched FROM movies WHERE guild_id=$1 AND title=$2", guild_id, matched_movie_title)
    if not rows:
        raise RuntimeError(f"Found {matched_movie_title} but couldn't get info on it for some reason. Bad!")
    movie = rows[0]
    username = await user_id_to_username(ctx, movie['user_id'])
    if not username:
        username = str(movie['user_id'])
    if movie['watched']:
        ratings = await get_ratings_for_movie_ids(ctx, guild_id=guild_id, movie_ids=[movie['id']])
        date_watched = movie['date_watched']
        if not ratings:
            average = float('nan')
        else:
            ratings_for_movie = [rating_row['rating'] for rating_row in
                                 ratings if rating_row['movie_id'] == movie['id']]
            average = sum(ratings_for_movie) / len(ratings_for_movie)
            if not date_watched:
                date_watched = "????-??-??"
            else:
                date_watched = date_watched.strftime("%Y-%m-%d")
        message = f"------ {matched_movie_title.upper()} ({username.upper()}) ({average:.1f})------\n" \
                  f"Date Watched: {date_watched}\n"
        for row in ratings:
            rating = '{:02.1f}'.format(float(row['rating']))
            rater_username = await user_id_to_username(ctx, row['user_id'])
            if not rater_username:
                rater_username = str(row['user_id'])
            message += f"{rater_username}: {rating}\n"
        return message
    else:
        message = f"------ {matched_movie_title.upper()} - {username.upper()} ------\n"
        endorsments = await get_movie_endorsments(guild_id, movie['id'])
        if endorsments:
            message += "Endorsed by:\n"
            for row in endorsments:
                endorser = await user_id_to_username(ctx, row['user_id'])
                if not endorser:
                    endorser = str(row['user_id'])
                message += f"{endorser}\n"
        else:
            message += "No endorsments\n"
    return message
    
async def parse_user_input_for_number_or_pagination(user_input):
    """parses user input where a number or pagination squarefucker is expected"""
    user_input, pagination = await parse_squarefucker(user_input)
    if pagination:
        return pagination
    else:
        try:
            number = int(user_input.strip())
            return (number, 1) # i.e. a valid pagination format:(results_per_page, page_num)
        except:
            return None
    
async def parse_user_input_for_mention(ctx, user_input):
    """parses user input where a mention is expected, as well as pagination squarefucker
    first, extract squarefucker and parse pagination info
    2nd, look for mention:
        extracts a discord_id if a mention
        if not, searches the provided user input for a best match in username list
        if no username provided, uses the requestor's user id."""
    user_input, pagination = await parse_squarefucker(user_input)
    discord_id = None
    if user_input:
        user_input, discord_id = await find_mention_in_user_input(user_input)
    if not discord_id:
        if user_input:
            target_username = " ".join(user_input) # i.e. all remaining user input is assumed to be a fuzzy-find for a username
            discord_id = await name_or_mention_to_id(ctx, target_username)
    if not discord_id:
        return pagination, None, None
    username = await user_id_to_username(ctx, discord_id)
    if not username:
        username = str(discord_id)
    return pagination, discord_id, username
        
async def parse_squarefucker(user_input):
    """determines if custom [] arg is included in user input
    returns parsed [] arg & returns the rest of the args without []"""
    new_user_input = [] # will convert to tuple before returning
    pattern = r"\[(-?)(\d+),?(\d*)\]"
    # pattern = r"\[(-?)(\d+) *(?:, *(\d*))?\]"  # this would match [1 , 2], but...
    # the problem is that discord splits arguments on spaces, so i would have to...
    # try joining the last few args and then seeing if that's a match and that's...
    # just a bit more of a pain than i want to deal with right now
    is_negative = False
    results_per_page = None
    page_num = None
    for i in user_input:
        thingy_found_in_loop = False
        match = re.fullmatch(pattern, i)
        if match:
            is_negative = bool(match.group(1))
            results_per_page = int(match.group(2))
            if is_negative:
                results_per_page = -results_per_page
            page_num = int(match.group(3)) if match.group(3) else 1
            pagination = [results_per_page, page_num]
            thingy_found_in_loop = True
        if not thingy_found_in_loop:
            new_user_input.append(i)
    if results_per_page:
        if page_num < 1:
            page_num = 1 
        return tuple(new_user_input), (results_per_page, page_num)
    return tuple(new_user_input), None
        
async def find_mention_in_user_input(user_input):
    """if present, find first mention
    return (user_input with mention removed, user_id)"""
    new_user_input = [] # will convert to tuple before returning
    user_id = None
    for i in user_input:
        id_found_in_loop = False
        if not user_id:
            user_id = await id_from_mention(i) # only look if not found yet
            if user_id:
                id_found_in_loop = True
        if not id_found_in_loop:
            new_user_input.append(i)
    return tuple(new_user_input), user_id
    
async def paginate(input_list, results_per_page, page_num):
    """takes a list and returns list of length indexed correctly
    reverses list order if results_per_page is negative
    no support for negative page_num atm"""
    max_results_per_page = 100 # TODO lower this if it results in a double message
    if page_num < 1:
        page_num = 1 
    if results_per_page < 0:
        input_list.reverse()
        results_per_page = -results_per_page
    if results_per_page < -max_results_per_page:
        results_per_page = -max_results_per_page
    if results_per_page > max_results_per_page:
        results_per_page = max_results_per_page
    past_last_page = (page_num - 1) * results_per_page >= len(input_list)
    if past_last_page:
        last_page_is_partial = len(input_list) % results_per_page > 0
        if last_page_is_partial:
            last_page_num = int(len(input_list) / results_per_page) + 1
        else:
            last_page_num = int(len(input_list) / results_per_page)
        page_num = last_page_num
    start = (page_num-1) * results_per_page
    end = start + results_per_page
    return input_list[start:end]
    
async def id_from_mention(text):
    pattern = "<@!?([0-9]+)>"
    mention = re.search(pattern, text)
    if mention:
        return int(mention[1])
    else:
        return None
        
async def name_or_mention_to_id(ctx, name_or_mention):
    """when provided with a user's name or an @, find the user id.
       good for funcs where user is expected to supply a member name as an argument"""
    guild_user_info = [[member.id, member.name] for member in ctx.message.guild.members]
    user_id = None
    user_id = await id_from_mention(name_or_mention)
    if user_id:
        guild_user_ids = [i[0] for i in guild_user_info]
        if user_id not in guild_user_ids:
            return None # send error through ctx here or let the func that calls this do it?
    else:
        names = [i[1] for i in guild_user_info]
        matched_name, _ = find_closest_match_and_score(search_term=name_or_mention, bank=names)
        if matched_name:
            for i in guild_user_info:
                if i[1] == matched_name:
                    user_id = i[0]
        else:
            return None
    try:
        async with db_pool.acquire() as connection:
            rows = await fetch_as_dict(connection, "SELECT id FROM users WHERE id = $1", user_id)
            if rows:
                return user_id
            else:
                await connection.execute("INSERT INTO users (id) values ($1)", user_id)
                return user_id
    except asyncpg.exceptions.PostgresError as e:
        await ctx.send("Ruh roh database error")
        print(f"Database error: {e}")
        return None
        
async def user_id_to_username(ctx, user_id):
    """finds a user's username given their discord id"""
    guild_user_info = [[member.id, member.name] for member in ctx.message.guild.members]
    for i in guild_user_info:
        if i[0] == user_id:
            return i[1]
    return None
        
async def find_exact_movie(guild_id, movie_title):
    """finds an exact movie (case insensitive), as opposed to the best match technique in find_all()"""
    try:
        async with db_pool.acquire() as connection:
            rows = await fetch_as_dict(connection, "SELECT * FROM movies WHERE guild_id=$1 AND title=$2", guild_id, movie_title)
            if rows:
                return rows[0]
            else:
                return None
    except asyncpg.exceptions.PostgresError as e:
        await ctx.send("Ruh roh database error")
        print(f"Database error: {e}")
        return None
        
async def find_movie_by_id(guild_id, movie_id):
    """finds movie by its id()"""
    try:
        async with db_pool.acquire() as connection:
            rows = await fetch_as_dict(connection, "SELECT * FROM movies WHERE guild_id=$1 AND id=$2", guild_id, movie_id)
            if rows:
                return rows[0]
            else:
                return None
    except asyncpg.exceptions.PostgresError as e:
        await ctx.send("Ruh roh database error")
        print(f"Database error: {e}")
        return None
        
async def endorse_suggestion(ctx, guild_id, movie_title, endorser_user_id):
    """goes through full logic to determine if endorse can be done.
    Abstracted outside of discord command since multiple things call it (not just the endorse command)."""
    existing_movie = await find_exact_movie(guild_id, movie_title)
    if not existing_movie:
        return await ctx.send(f"'{movie_title}' doesn't exist.")
    if existing_movie['watched'] == 1:
        return await ctx.send(f"'{existing_movie['title']}' has already been watched or rated, so it can't be endorsed.")
    if existing_movie['user_id'] == endorser_user_id:
        return await ctx.send(f"You cannot endorse your own movie")
    if await movie_is_endorsed_by_user(ctx, guild_id, existing_movie['title'], endorser_user_id):
        return await ctx.send(f"You have already endorsed '{existing_movie['title']}'")
    try:
        async with db_pool.acquire() as connection:
            await connection.execute("INSERT INTO endorsements (guild_id, user_id, movie_id) values ($1,$2,$3)", guild_id, endorser_user_id, existing_movie['id'])
    except asyncpg.exceptions.PostgresError as e:
        await ctx.send("Ruh roh database error")
        print(f"Database error: {e}")
    return await send_goodly(ctx, f"You have endorsed '{existing_movie['title']}'.")

async def movie_is_endorsed_by_user(ctx, guild_id, movie_title, endorser_user_id):
    """returns True if movie is endorsed by user_id&guild_id, False otherwise."""
    async with db_pool.acquire() as connection:
        rows = await fetch_as_dict(connection,
            """SELECT endorsements.user_id FROM endorsements
               INNER JOIN movies ON endorsements.movie_id=movies.id
               WHERE endorsements.guild_id=$1 AND movies.title=$2""", guild_id, movie_title)
    current_endorsers = [i['user_id'] for i in rows]
    if endorser_user_id in current_endorsers:
        return True
    else:
        return False

async def get_all_guild_movies(ctx):
    guild_id = await get_guild_id(ctx, db_pool)
    try:
        async with db_pool.acquire() as connection:
            rows = await fetch_as_dict(connection, "SELECT * FROM movies WHERE guild_id=$1", guild_id)
            return rows
    except asyncpg.exceptions.PostgresError as e:
            await ctx.send("Ruh roh database error")
            print(f"Database error: {e}")
            return []
 
async def get_all_guild_reviews(ctx):
    guild_id = await get_guild_id(ctx, db_pool)
    try:
        async with db_pool.acquire() as connection:
            rows = await fetch_as_dict(connection, "SELECT * FROM reviews WHERE guild_id=$1", guild_id)
            return rows
    except asyncpg.exceptions.PostgresError as e:
            await ctx.send("Ruh roh database error")
            print(f"Database error: {e}")
            return []
 
async def get_all_guild_reviewed_movied(ctx):
    guild_id = await get_guild_id(ctx, db_pool)
    try:
        async with db_pool.acquire() as connection:
            rows = await fetch_as_dict(connection, 
                """SELECT * FROM movies
                   INNER JOIN reviews ON movies.id=reviews.movie_id
                   WHERE reviews.guild_id=$1""", guild_id)
            return rows
    except asyncpg.exceptions.PostgresError as e:
            await ctx.send("Ruh roh database error")
            print(f"Database error: {e}")
            return []
            
async def get_ratings_for_movie_ids(ctx, guild_id, movie_ids):
    try:
        async with db_pool.acquire() as connection:
            rows = await fetch_as_dict(connection,"SELECT * FROM ratings WHERE guild_id=$1 AND movie_id=ANY($2::integer[])", guild_id, movie_ids)
            if rows:
                return rows
            else:
                return []
    except asyncpg.exceptions.PostgresError as e:
        await ctx.send("Ruh roh database error")
        print(f"Database error: {e}")
        return []
        
async def get_movie_endorsments(guild_id, movie_id):
    try:
        async with db_pool.acquire() as connection:
            rows = await fetch_as_dict(connection,
                """SELECT endorsements.user_id FROM endorsements
                   INNER JOIN movies ON endorsements.movie_id = movies.id
                   WHERE endorsements.guild_id=$1 AND movies.id=$2""", guild_id, movie_id)
            if rows:
                return rows
            else:
                return []
    except asyncpg.exceptions.PostgresError as e:
        await ctx.send("Ruh roh database error")
        print(f"Database error: {e}")
        return []
        
async def get_user_average_rating_given(ctx, guild_id, user_id):
    try:
        async with db_pool.acquire() as connection:
            rows = await fetch_as_dict(connection,
                """SELECT AVG(rating) as avg_rating 
                   FROM ratings
                   WHERE guild_id=$1
                   AND user_id=$2""", guild_id, user_id)
    except asyncpg.exceptions.PostgresError as e:
        await ctx.send("Ruh roh database error")
        print(f"Database error: {e}")
        return []
    if rows:
        return rows[0]['avg_rating']
    else:
        return 0

async def get_server_average_rating_given(ctx, guild_id):
    try:
        async with db_pool.acquire() as connection:
            rows = await fetch_as_dict(connection,
                """SELECT AVG(rating) as avg_rating 
                   FROM ratings
                   WHERE guild_id=$1""", guild_id)
    except asyncpg.exceptions.PostgresError as e:
        await ctx.send("Ruh roh database error")
        print(f"Database error: {e}")
        return []
    if rows:
        return rows[0]['avg_rating']
    else:
        return 0

async def get_server_median_rating_given(ctx, guild_id):
    try:
        async with db_pool.acquire() as connection:
            rows = await fetch_as_dict(connection,
                """SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY rating) as median_rating
                   FROM ratings
                   WHERE guild_id=$1""", guild_id)
    except asyncpg.exceptions.PostgresError as e:
        await ctx.send("Ruh roh database error")
        print(f"Database error: {e}")
        return []
    if rows:
        return rows[0]['median_rating']
    else:
        return 0

async def get_server_median_attendance(ctx, guild_id):
    try:
        async with db_pool.acquire() as connection:
            rows = await fetch_as_dict(connection,
                """SELECT percentile_cont(0.5)
                     WITHIN GROUP (ORDER BY ratings_count) as median_attendance
                   FROM (
                     SELECT movie_id, COUNT(*) as ratings_count
                     FROM ratings
                     WHERE guild_id=$1
                     GROUP BY movie_id""", guild_id)
    except asyncpg.exceptions.PostgresError as e:
        await ctx.send("Ruh roh database error")
        print(f"Database error: {e}")
        return []
    if rows:
        return rows[0]['median_attendance']
    else:
        return 0 

class MyHelpCommand(commands.HelpCommand):
    def __init__(self):
        super().__init__()
        self.cog_order = [
            ("Core", "Core Functionality"),
            ("BrowseSuggestions", "Browse Suggestions"),
            ("BrowseMovienights", "Browse Movienights"),
            ("Scraping", "Scraping"),
            ("Plotting", "Plotting"),
            ("Narrate", "Narrate")
        ]
        
        # These are the commands you want in each category, in the exact order:
        self.commands_order = {
            "Core": [
                "add",
                "remove",
                "endorse",
                "unendorse",
                "rate",
                "unrate",
                "review",
                "transfer",
                "find",
                "change_date_watched"
            ],
            "BrowseSuggestions": [
                "suggestions",
                "endorsements",
                "endorsed",
                "random",
            ],
            "BrowseMovienights": [
                "movienights",
                "top_movienights",
                "ratings",
                "top_ratings",
                "unrated",
                "reviews",
                "standings",
                "attendance",
                "seen",
            ],
            "Scraping": [
                "ebert"
            ],
            "Plotting": [
                "plot_ratings",
                "plot_movienights",
                "plot_favorites",
                "plot_user_similarity",
                "plot_movie_spread"
            ],
            "Narrate": [
                "narrate",  # subcommands: start / stop / status
            ]
        }
        
    detailed_help = {
        "add": "<movie title> — Adds a movienight suggestion. Ex. !add shrek 3",
        "remove": "<movie title> — Remove a suggestion. Ex. !remove shrek 3",
        "endorse": "<movie title> — Endorse a suggestion. Ex. !endorse shrek 3",
        "unendorse": "<movie title> Remove endorsement. Ex. !unendorse shrek 3",
        "rate": "<movie title> <1-10> — Rate a movie. Ex. !rate shrek 3 10",
        "unrate": "<movie title> — Remove rating. Ex. !unrate shrek 3",
        "review": '"<movie title>" <review text> — Review a movie. If the movie is more than 1 word, put quotes around it. Ex. !review "shrek 3" this was a super great amazing movie!!!',
        "transfer":'"<movie title>" <name or mention> — Transfer movie choosership to a new person. If the movie is more than 1 word, put quotes around it. Ex. !transfer "shrek 3" some discord user. You can also use @discord_user',
        "find": '<search text> <[n,p]> — Search for users or movies. Ex. !find discord user; !find movie title. Browse through results by adding a paginator. Ex. !find movie title [10,2] -> return the 2nd page; 10 results per page.',
        "change_date_watched": '<yyyy-mm-dd> <movie title> — Change the "date watched" of an existing movienight.',
        "suggestions": '<name or mention> <[n,p]> — A chronological list of movie suggestions by a user. Ex. !suggestions some discord user. Browse through results by adding a paginator. Ex. !suggestions some discord user [10,2] -> return the 2nd page; 10 results per page.',
        "endorsements": '<name or mention> <[n,p]> — A chronological listing of endorsements given by a user. Ex. !endorsements some discord user. Browse through results by adding a paginator. Ex. !endorsements some discord user [10,2] -> return the 2nd page; 10 results per page.',
        "endorsed": '<[n,p]> — A list of the most-endorsed movies. Ex. !endorsed. Browse results by adding a paginator. Ex. !endorsed [10,2] -> return the 2nd page; 10 results per page.',
        "random": 'Random movie. Ex. !random.',
        "movienights": '<name or mention> <[n,p]> — A chronological list of movienights (i.e. rated movies) from the server or a specific user. To look at server movienights, do not provide any name/mention.. Ex. !chooser some discord user. Browse through results by adding a paginator. Ex. !chooser some discord user [10,2] -> return the 2nd page; 10 results per page.',
        "top_movienights": '<name or mention> <[n,p]> — A list of the highest-rated movienights (i.e. rated movies) from the server or a specific user. To look at server movienights, do not provide any name/mention.. Ex. !chooser some discord user. Browse through results by adding a paginator. Ex. !chooser some discord user [10,2] -> return the 2nd page; 10 results per page.',
        "ratings": '<name or mention> <[n,p]> — A chronological list of ratings given by a specific user. If name or mention is not provided, the user calling the function will be used as the target. Ex. !ratings some discord user. Browse through results by adding a paginator. Ex. !ratings some discord user [10,2] -> return the 2nd page; 10 results per page.',
        "top_ratings": '<name or mention> <[n,p]> — A list of the highest ratings given by a specific user. If name or mention is not provided, the user calling the function will be used as the target. Ex. !ratings some discord user. Browse through results by adding a paginator. Ex. !ratings some discord user [10,2] -> return the 2nd page; 10 results per page.',
        "unrated": '<name or mention> <[n,p]> — A chronological list of unrated movies from a user. Ex. !unrated some discord user. Browse through results by adding a paginator. Ex. !unrated some discord user [10,2] -> return the 2nd page; 10 results per page.',
        "reviews": '<search text> <[n,p]> — Search for reviews by A) reviewer name B) movie title and C) review text. Ex. !reviews discord user shrek 3 awesome movie. Browse through results by adding a paginator. Ex. !reviews shrek 3 [10,2] -> return the 2nd page; 10 results per page.',
        "standings": '<[n,p]> — Chooser rankings (avg rating received). Ex. !standings. Browse results by adding a paginator. Ex. !standings [10,2] -> return the 2nd page; 10 results per page.',
        "attendance": '<[n,p]> — Movies ranked by attendance. Ex. !attendance. Browse through results by adding a paginator. Ex. !attendance [10,2] -> return the 2nd page; 10 results per page.',
        "recent": '<[n,p]> — Recently watched movies. Ex. !recent. Browse through results by adding a paginator. Ex. !recent [10,2] -> return the 2nd page; 10 results per page.',
        "seen": 'Total movies watched in this server. Ex. !seen.',
        "ebert": '<movie title> — Return a Rogert Ebert review for a movie. Ex. !ebert shrek 3',
        "plot_ratings": '<name or mention> — Plot ratings from a user.',
        "plot_movienights": '<name or mention> — Plot movienights from the server or from a specific user.',
        "plot_favorites": '<name or mention> — Plot average ratings given from one user to each movie owner in the server.',
        "plot_user_similarity": '<min_common> — Plot user rating similarity matrix. min_common (default 5) is minimum movies in common.',
        "plot_movie_spread": '<movie title> — Plot the distribution of ratings for a movie.',
        "narrate on": (
            "<#text-channel> <[voice]> <[rate]> — Enable narration and set the text channel you'll type in.\n"
            "Examples:\n"
            "• !narrate on #general\n"
            "• !narrate on #watchparty en-US-Wavenet-D 1.0\n"
            "Notes:\n"
            "• If channel/voice/rate are omitted, your saved defaults are used.\n"
            "• If you’re already in a voice channel, the bot will auto-join."
        ),
        "narrate off": (
            "Disable narration for you.\n"
            "Example: !narrate off"
        ),
        "narrate channel": (
            "<#text-channel> — Set (or change) your default narration text channel.\n"
            "If narration is currently on, the bot will switch to watch this channel immediately.\n"
            "Example: !narrate channel #watchparty"
        ),
        "narrate voice": (
            "<voice> <[rate]> — Set your default voice (Google full name) and optional speaking rate (0.25–4.0).\n"
            "If narration is currently on, the new voice/rate take effect immediately.\n"
            "Examples:\n"
            "• !narrate voice en-US-Wavenet-D\n"
            "• !narrate voice en-US-Chirp3-HD-Gacrux 1.0\n"
            "Tip: Use !narrate voices to see the available names."
        ),
        "narrate voices": (
            "List or link to available Google TTS voices.\n"
            "Example: !narrate voices\n"
            "Optional: filter by language code (e.g., en-US): !narrate voices en-US"
        ),
        "narrate status": (
            "Show your narration status (enabled, channel, voice, rate) and the bot’s active narrator/VC."
        ),
        "narrate cancel": (
            "Stop speaking and clear the queue for your guild.\n"
            "Aliases: !narrate x\n"
            "Example: !narrate cancel"
        ),
    }
    
    async def send_bot_help(self, mapping):
        dest = self.get_destination()
        help_message = ""
        for cog_name, cog_text in self.cog_order:
            help_message += f"{cog_text}\n"
            cog = self.context.bot.get_cog(cog_name)
            if not cog:
                continue
            ordered_commands = []
            for cmd_name in self.commands_order.get(cog_name, []):
                cmd = get(cog.get_commands(), name=cmd_name)
                if cmd and not cmd.hidden:
                    ordered_commands.append(cmd)
            if not ordered_commands:
                continue
            # List each command in the order we specified
            lines = []
            for command in ordered_commands:
                # If it's a group (e.g., 'narrate'), list its subcommands as 'narrate on', etc.
                if isinstance(command, commands.Group):
                    for sub in command.commands:
                        if sub.hidden:
                            continue
                        qualified = f"{command.name} {sub.name}"
                        desc = self.detailed_help.get(qualified) or sub.help or 'No description'
                        help_message += f"\u001b[1;40;32m{qualified}\u001b[0;0m — {desc}\n"
                else:
                    help_message += f"\u001b[1;40;32m{command.name}\u001b[0;0m — {command.help or 'No description'}\n"
            help_message += "\n"

        return await send_goodly(dest, help_message)

    async def send_cog_help(self, cog):
        """
        Called when the user asks for help about a specific cog, e.g. "!help Core"
        """
        await super().send_cog_help(cog)

    async def send_command_help(self, command):
        """
        Called when the user asks for help on a specific command, e.g. "!help add"
        """
        dest = self.get_destination()
        if command and not command.hidden:
            parent = getattr(command, "full_parent_name", None)
            key = f"{parent} {command.name}" if parent else command.name
            command_detailed_help = self.detailed_help.get(key) or self.detailed_help.get(command.name)
            help_message = f"\u001b[1;40;32m{key}\u001b[0;0m — {command_detailed_help or command.help or 'No description'}"
            return await send_goodly(dest, help_message)


      
db_pool = None

intents = Intents.default()
intents.members = True
intents.message_content = True
intents.voice_states = True


bot = commands.Bot(command_prefix="!",
                   case_insensitive=True,
                   intents=intents,
                   description='ur fav movienight companion.')  
bot.help_command = MyHelpCommand()


@bot.event
async def on_ready():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(**PSQL_CREDENTIALS)
        print("Database connection pool created successfully.")
    except Exception as e:
        print(f"Failed to connect to the database: {e}")

    # expose the pool so bot_voice can use it
    bot.db_pool = db_pool

    await bot.add_cog(Core(bot))
    await bot.add_cog(BrowseSuggestions(bot))
    await bot.add_cog(BrowseMovienights(bot))
    await bot.add_cog(Scraping(bot))
    await bot.add_cog(Plotting(bot))

    # NEW: voice/narration cog
    await bot.add_cog(NarrationCog(bot))

    print("cogs added")
    print("setup complete")
        
bot.run(bot_token)
