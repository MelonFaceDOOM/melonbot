from config import PSQL_CREDENTIALS
import psycopg2

def main():
    conn = psycopg2.connect(**PSQL_CREDENTIALS)
    cur = conn.cursor()
    movies_and_correct_dates = []
    with open("updated_dates.txt", "r") as f:
        data = f.read()
        lines = data.split("\n")
        for line in lines:
            line_data = line.split("\t")
            if line_data[2]:
                movies_and_correct_dates.append((line_data[0], line_data[2]))
    for movie, date in movies_and_correct_dates:
        cur.execute(
            """
            UPDATE movies
            SET date_watched=%s
            WHERE title=%s
            """,
            (date, movie)
        )
    conn.commit()
    cur.close()
    conn.close()
    
if __name__ == "__main__":
    main()
