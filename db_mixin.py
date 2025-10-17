import asyncpg  # optional, just for type hints

class DbMixin:
    @property
    def db(self) -> "asyncpg.Pool":
        bot = getattr(self, "bot", None)
        if bot is None:
            raise RuntimeError("Object is missing .bot; set self.bot = bot in __init__")
        pool = getattr(bot, "db_pool", None)
        if pool is None:
            raise RuntimeError("db_pool not available on bot")
        return pool