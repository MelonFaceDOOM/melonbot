import asyncpg 

# helper func for asyncpg
async def fetch_as_dict(connection, query, *args):
    rows = await connection.fetch(query, *args)
    return [dict(row) for row in rows]

async def get_user_id(ctx, db_pool):
    """get user id from ctx. add it to db if it doesn't exist"""
    user_id = ctx.message.author.id
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
        
async def get_guild_id(ctx, db_pool):
    """get guild id from ctx. add it to db if it doesn't exist"""
    guild_id = ctx.message.guild.id
    try:
        async with db_pool.acquire() as connection:
            rows = await fetch_as_dict(connection, "SELECT id FROM guilds WHERE id=$1", guild_id)
            if rows:
                return guild_id
            else:
                await connection.execute("INSERT INTO guilds (id) values ($1)", guild_id)
                return guild_id
    except asyncpg.exceptions.PostgresError as e:
        await ctx.send("Ruh roh database error")
        print(f"Database error: {e}")
        return None