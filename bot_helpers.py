import asyncpg

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
