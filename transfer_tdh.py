import psycopg2
from config import PSQL_CREDENTIALS

def main():
    conn = psycopg2.connect(**PSQL_CREDENTIALS)
    transfer_tdh_to_test_server(conn)
    conn.close()

def change_dates(conn):
    cur = conn.cursor()
    cur.execute("""UPDATE movies SET date_watched""")
    conn.commit()
    cur.close()

def transfer_tdh_to_test_server(conn):
    cur = conn.cursor()
    tables = ['movies', 'endorsements', 'ratings', 'reviews']
    for table in tables:
        #cur.execute(f"""select guilds.id, count({table}.id) from guilds join {table} on guilds.id = {table}.guild_id group by guilds.id""")
        #r = cur.fetchall()
        #r.sort(key=lambda x: x[1], reverse=True)
        #server_id = r[0][0]
        #new_server_id = r[-1][0]
        server_id = 366280562190843904
        new_server_id = 686619158611230720

        cur.execute(f"""delete from {table} where guild_id = %s""", (new_server_id,))
        cur.execute(f"""update {table} set guild_id = %s where guild_id = %s""", 
                (new_server_id, server_id))
    conn.commit()
    cur.close()

if __name__ == "__main__":
    main()