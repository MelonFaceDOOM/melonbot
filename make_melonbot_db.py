from config import PSQL_CREDENTIALS
import psycopg2
from psycopg2 import sql
import sqlite3
from psycopg2.extras import execute_values
import datetime

def make_db():
    conn = psycopg2.connect(**PSQL_CREDENTIALS)
    cur = conn.cursor()
    cur.execute("""CREATE EXTENSION IF NOT EXISTS citext""")
    cur.execute("""CREATE TABLE IF NOT EXISTS guilds (
                id BIGINT PRIMARY KEY NOT NULL,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS users (
                id BIGINT PRIMARY KEY NOT NULL,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS movies (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                guild_id BIGINT NOT NULL,
                title CITEXT NOT NULL CHECK (char_length(title) <= 256),
                date_suggested TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                date_watched TIMESTAMP,
                watched INTEGER DEFAULT 0,
                FOREIGN KEY (guild_id) REFERENCES guilds (id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
                UNIQUE (guild_id, title))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS endorsements (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                guild_id BIGINT NOT NULL,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                movie_id INTEGER NOT NULL,
                FOREIGN KEY (guild_id) REFERENCES guilds (id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
                FOREIGN KEY (movie_id) REFERENCES movies (id) ON DELETE CASCADE,
                UNIQUE (guild_id, user_id, movie_id))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS ratings (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                guild_id BIGINT NOT NULL,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                movie_id INTEGER NOT NULL,
                rating DOUBLE PRECISION NOT NULL,
                FOREIGN KEY (guild_id) REFERENCES guilds (id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
                FOREIGN KEY (movie_id) REFERENCES movies (id) ON DELETE CASCADE,
                UNIQUE (guild_id, user_id, movie_id))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS reviews (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                guild_id BIGINT NOT NULL,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                movie_id INTEGER NOT NULL,
                review_text TEXT NOT NULL CHECK (char_length(review_text) <= 1200),
                FOREIGN KEY (guild_id) REFERENCES guilds (id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
                FOREIGN KEY (movie_id) REFERENCES movies (id) ON DELETE CASCADE,
                UNIQUE (guild_id, user_id, movie_id))""")
    conn.commit()
    cur.close()
    conn.close()
    
def drop_all_tables():
    #return # so i dont call by accident
    try:
        conn = psycopg2.connect(**PSQL_CREDENTIALS)
        cur = conn.cursor()
        cur.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public';""")
        tables = cur.fetchall()
        for table in tables:
            table_name = table[0]
            cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
            print(f"Dropped table: {table_name}")
        conn.commit()
        print("All tables dropped successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    cur.close()
    conn.close()

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def insert_dicts_into_psql(pcur, table_name, dicts):
    """Assumes dict keys match column names and that all dicts have same keys"""
    for i in dicts:
        for k,v in i.items():
            if type(v) == str and v.strip()=="":
                i[k] = None
        if 'user_id' in i and (i['user_id'] is None or i['user_id']==""):
            i['user_id']=0
            user = {"id": 0}
            insert_dicts_into_psql(pcur, "users", [user])
        if 'user_id' in i and i['user_id'] != "":
            user = {"id": i['user_id']}
            insert_dicts_into_psql(pcur, "users", [user])
        if 'guild_id' in i and i['guild_id'] != "":
            guild = {"id": i['guild_id']}
            insert_dicts_into_psql(pcur, "guilds", [guild])

    cols = dicts[0].keys()
    values = [tuple(item[col] for col in cols) for item in dicts]
    query = sql.SQL("""INSERT INTO {table} ({fields}) VALUES %s ON CONFLICT DO NOTHING""").format(
        table=sql.Identifier(table_name),
        fields=sql.SQL(", ").join(map(sql.Identifier, cols)),
    )
    execute_values(pcur, query, values)

def transfer_from_sqlite():
    pconn = psycopg2.connect(**PSQL_CREDENTIALS)
    pcur = pconn.cursor()
    sqlite_filename = "melonbot.db"
    conn = sqlite3.connect(sqlite_filename)
    conn.row_factory = dict_factory
    cur = conn.cursor()
    cur.execute("""SELECT * FROM guilds""")
    guilds = cur.fetchall()
    insert_dicts_into_psql(pcur, "guilds", guilds)
    cur.execute("""SELECT * FROM users""")
    users = cur.fetchall()
    for i in users:
        del i['guild_id']
    insert_dicts_into_psql(pcur, "users", users)
    cur.execute("""SELECT * FROM movies""")
    _ = cur.fetchall()
    movies = []
    for i in _:
        if i['title'] is None or i['title'] == "":
            continue
        if i['watched']==1:
            i['date_watched'] = fix_missing_date(i['date_watched'])
        i['date_suggested'] = fix_missing_date(i['date_suggested'])
        movies.append(i)
    insert_dicts_into_psql(pcur, "movies", movies)
    cur.execute("""SELECT * FROM endorsements""")
    _ = cur.fetchall()
    endorsements = keep_if_movie_exists(pcur, _)
    for i in endorsements:
        i['date'] = fix_missing_date(i['date'])
    insert_dicts_into_psql(pcur, "endorsements", endorsements)
    cur.execute("""SELECT * FROM ratings""")
    _ = cur.fetchall()
    ratings = keep_if_movie_exists(pcur, _)
    for i in ratings:
        i['date'] = fix_missing_date(i['date'])
    insert_dicts_into_psql(pcur, "ratings", ratings)
    cur.execute("""SELECT * FROM reviews""")
    _ = cur.fetchall()
    reviews = keep_if_movie_exists(pcur, _)
    for i in reviews:
        i['date'] = fix_missing_date(i['date'])
    good_reviews = []
    for i in reviews:
        if type(i['review_text']) == str and len(i['review_text'].strip()) > 0:
            good_reviews.append(i)
    insert_dicts_into_psql(pcur, "reviews", good_reviews)
    pconn.commit()

def keep_if_movie_exists(pcur, rows):
    good_rows = []
    for i in rows:
        pcur.execute("""SELECT * FROM movies WHERE id = %s""", (i['movie_id'],))
        movies = pcur.fetchall()
        if len(movies) < 1:
            print("skipping movie cus it doesn't exist", i['movie_id'])
        else:
            good_rows.append(i)
    return good_rows

def fix_sequencers():
    conn = psycopg2.connect(**PSQL_CREDENTIALS)
    cur = conn.cursor()
    tables = ["movies", "endorsements", "ratings", "reviews"]
    for table in tables:
        seq = f"{table}_id_seq"
        cur.execute(f"""SELECT setval('{seq}', (SELECT MAX(id) FROM {table}));""")
        #cur.execute("""SELECT pg_get_serial_sequence('movies', 'id')""")
        #r = cur.fetchall()
        #print(table, r)
    conn.commit()
    cur.close()
    conn.close()

def fix_missing_date(date):
    if type(date) is datetime.datetime:
        return date
    else:
        try:
            if len(date) == 10:
                return datetime.datetime.strptime(date, "%Y-%m-%d")
            else:
                # Otherwise, assume "YYYY-MM-DD HH-MM-SS"
                date = date.split(".")[0] # accounts for "datetime.123123" which is apparently another dumb format
                return datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        except:
            print("failed to parse date:", date)
            return datetime.date(2020,1,1)

if __name__ == "__main__":
    drop_all_tables()
    make_db()
    transfer_from_sqlite()
    fix_sequencers()

