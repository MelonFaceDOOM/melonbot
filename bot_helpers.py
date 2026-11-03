import re
import asyncpg
from matching import find_closest_match_and_score

# helper func for asyncpg
async def fetch_as_dict(connection, query, *args):
    rows = await connection.fetch(query, *args)
    return [dict(row) for row in rows]


def _guild_icon_url(guild):
    if guild is None or guild.icon is None:
        return None
    return str(guild.icon.url)


def _clip(value, max_len):
    if value is None:
        return None
    text = str(value)
    return text[:max_len] if len(text) > max_len else text


async def upsert_guild(db, guild):
    """Insert/update guild id + Discord name/icon cache for Nitwitch."""
    if guild is None:
        return None
    name = _clip(guild.name, 128)
    icon_url = _guild_icon_url(guild)
    try:
        await db.execute(
            """
            INSERT INTO guilds (id, name, icon_url, updated_at)
            VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
            ON CONFLICT (id) DO UPDATE SET
              name = EXCLUDED.name,
              icon_url = EXCLUDED.icon_url,
              updated_at = CURRENT_TIMESTAMP
            """,
            guild.id,
            name,
            icon_url,
        )
        return guild.id
    except asyncpg.exceptions.PostgresError as e:
        print(f"Database error upserting guild {guild.id}: {e}")
        return None


async def upsert_user(db, user):
    """Insert/update user id + username/global_name cache for Nitwitch.

    Uses Discord username / global_name — not guild nick (display_name).
    """
    if user is None:
        return None
    username = _clip(getattr(user, "name", None), 64)
    global_name = _clip(getattr(user, "global_name", None), 64)
    try:
        await db.execute(
            """
            INSERT INTO users (id, username, global_name, updated_at)
            VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
            ON CONFLICT (id) DO UPDATE SET
              username = EXCLUDED.username,
              global_name = EXCLUDED.global_name,
              updated_at = CURRENT_TIMESTAMP
            """,
            user.id,
            username,
            global_name,
        )
        return user.id
    except asyncpg.exceptions.PostgresError as e:
        print(f"Database error upserting user {user.id}: {e}")
        return None


async def get_user_id(ctx, db_pool):
    """get user id from ctx; upsert Discord name cache."""
    user = ctx.message.author
    try:
        result = await upsert_user(db_pool, user)
        if result is None:
            await ctx.send("Ruh roh database error")
        return result
    except asyncpg.exceptions.PostgresError as e:
        await ctx.send("Ruh roh database error")
        print(f"Database error: {e}")
        return None


async def get_guild_id(ctx, db_pool):
    """get guild id from ctx; upsert Discord name/icon cache."""
    guild = ctx.message.guild
    try:
        result = await upsert_guild(db_pool, guild)
        if result is None:
            await ctx.send("Ruh roh database error")
        return result
    except asyncpg.exceptions.PostgresError as e:
        await ctx.send("Ruh roh database error")
        print(f"Database error: {e}")
        return None


async def send_goodly(ctx, message):
    """standard way of sending a MESSAGE to the stupid user"""
    try:
        messages = await _chunk(message)
    except ValueError as e:
        return await ctx.send(f"somehow the basic way i am supposed to send messages broke that is very bad.\n{e}")
    for message in messages:
        await ctx.send("```ansi\n" + message + "```")


async def _chunk(message, max_length=1900):
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


# ---------------------------------------------------------------------------
# User input: mentions / fuzzy names and the [n,p] pagination syntax
# (moved from bot.py so cogs can use them; bot.py is the entry script)
# ---------------------------------------------------------------------------

async def parse_user_input_for_mention(db, ctx, user_input):
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
            discord_id = await name_or_mention_to_id(db, ctx, target_username)
    if not discord_id:
        return pagination, None, None
    username = await user_id_to_username(ctx, discord_id)
    if not username:
        username = str(discord_id)
    return pagination, discord_id, username


async def name_or_mention_to_id(db, ctx, name_or_mention):
    """when provided with a user's name or an @, find the user id.
       good for funcs where user is expected to supply a member name as an argument"""
    guild = ctx.message.guild
    members_by_id = {member.id: member for member in guild.members}
    guild_user_info = [[member.id, member.name] for member in guild.members]
    user_id = None
    user_id = await id_from_mention(name_or_mention)
    if user_id:
        if user_id not in members_by_id:
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
    member = members_by_id.get(user_id)
    if member is None:
        return None
    try:
        result = await upsert_user(db, member)
        if result is None:
            await ctx.send("Ruh roh database error")
        return result
    except asyncpg.exceptions.PostgresError as e:
        await ctx.send("Ruh roh database error")
        print(f"Database error: {e}")
        return None


async def parse_squarefucker(user_input):
    """determines if custom [] arg is included in user input
    returns parsed [] arg & returns the rest of the args without []"""
    new_user_input = [] # will convert to tuple before returning
    pattern = r"\[(-?)(\d+),?(\d*)\]"
    # pattern = r"\[(-?)(\d+) *(?:, *(\d*))?\]"  # this would match [1 , 2], but...
    # the problem is that discord splits arguments on spaces, so i would have to...
    # try joining the last few args and then seeing if that's a match and that's...
    # just a bit more of a pain than i want to deal with right now
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


async def user_id_to_username(ctx, user_id):
    """finds a user's username given their discord id"""
    guild_user_info = [[member.id, member.name] for member in ctx.message.guild.members]
    for i in guild_user_info:
        if i[0] == user_id:
            return i[1]
    return None
