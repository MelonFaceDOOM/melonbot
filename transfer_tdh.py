import psycopg2
from config import PSQL_CREDENTIALS

def main():
    conn = psycopg2.connect(**PSQL_CREDENTIALS)
    # transfer_tdh_to_test_server(conn)
    transfer_tdh_to_test_server(conn)
    conn.close()


def transfer_tdh_to_test_server(conn):
    cur = conn.cursor()
    
    # Source and target server IDs
    server_id = 366280562190843904
    new_server_id = 686619158611230720

    # Clear all data from the target server first
    # Delete in reverse order of dependencies
    print(f"Clearing data from server {new_server_id}...")
    cur.execute("DELETE FROM reviews WHERE guild_id = %s", (new_server_id,))
    cur.execute("DELETE FROM ratings WHERE guild_id = %s", (new_server_id,))
    cur.execute("DELETE FROM endorsements WHERE guild_id = %s", (new_server_id,))
    cur.execute("DELETE FROM movies WHERE guild_id = %s", (new_server_id,))
    conn.commit()
    print("Server data cleared.")

    # First, ensure the new guild exists
    cur.execute("INSERT INTO guilds (id) VALUES (%s) ON CONFLICT DO NOTHING", (new_server_id,))

    # Copy movies first and create a mapping of old movie IDs to new ones
    movie_id_map = {}
    cur.execute("""
        INSERT INTO movies (user_id, guild_id, title, date_suggested, date_watched, watched)
        SELECT user_id, %s, title, date_suggested, date_watched, watched
        FROM movies 
        WHERE guild_id = %s
        RETURNING id, title""", (new_server_id, server_id))
    
    # Store the mapping of old to new movie IDs
    old_movies = {}
    cur.execute("SELECT id, title FROM movies WHERE guild_id = %s", (server_id,))
    for old_id, title in cur.fetchall():
        old_movies[title] = old_id

    new_movies = {}
    cur.execute("SELECT id, title FROM movies WHERE guild_id = %s", (new_server_id,))
    for new_id, title in cur.fetchall():
        new_movies[title] = new_id

    # Create the movie ID mapping
    for title in old_movies:
        if title in new_movies:
            movie_id_map[old_movies[title]] = new_movies[title]

    print(f"Copying {len(movie_id_map)} movies...")

    # Now copy the related tables using the movie ID mapping
    # Copy endorsements
    for old_movie_id, new_movie_id in movie_id_map.items():
        cur.execute("""
            INSERT INTO endorsements (user_id, guild_id, date, movie_id)
            SELECT e.user_id, %s, e.date, %s
            FROM endorsements e
            WHERE e.guild_id = %s AND e.movie_id = %s""", 
            (new_server_id, new_movie_id, server_id, old_movie_id))

    # Copy ratings
    for old_movie_id, new_movie_id in movie_id_map.items():
        cur.execute("""
            INSERT INTO ratings (user_id, guild_id, date, movie_id, rating)
            SELECT r.user_id, %s, r.date, %s, r.rating
            FROM ratings r
            WHERE r.guild_id = %s AND r.movie_id = %s""",
            (new_server_id, new_movie_id, server_id, old_movie_id))

    # Copy reviews
    for old_movie_id, new_movie_id in movie_id_map.items():
        cur.execute("""
            INSERT INTO reviews (user_id, guild_id, date, movie_id, review_text)
            SELECT r.user_id, %s, r.date, %s, r.review_text
            FROM reviews r
            WHERE r.guild_id = %s AND r.movie_id = %s""",
            (new_server_id, new_movie_id, server_id, old_movie_id))

    conn.commit()
    print("Transfer complete!")
    cur.close()

    

if __name__ == "__main__":
    main()