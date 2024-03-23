import requests
import pymysql
import csv
from datetime import datetime
import os

def fetch_movie_data(api_key):
    api_endpoint_movies = "https://api.themoviedb.org/3/discover/movie"
    api_endpoint_credits = "https://api.themoviedb.org/3/movie/{}/credits"

    query_params_movies = {
        "include_adult": "true",
        "include_video": "true",
        "sort_by": "popularity.desc",
        "with_original_language": "hi",
        "year": "2024",
        "api_key": api_key
    }

    response = requests.get(api_endpoint_movies, params=query_params_movies)
    if response.status_code == 200:
        total_pages = response.json()["total_pages"]
        all_data = []

        for page_num in range(1, total_pages + 1):
            query_params_movies["page"] = page_num
            response = requests.get(api_endpoint_movies, params=query_params_movies)
            if response.status_code == 200:
                movie_data = response.json()["results"]
                for movie in movie_data:
                    movie_id = movie['id']
                    response_credits = requests.get(api_endpoint_credits.format(movie_id), params={"api_key": api_key})
                    if response_credits.status_code == 200:
                        credits_data = response_credits.json()
                        all_data.append((movie, credits_data))
                    else:
                        print("Error fetching credits for movie:", movie_id)
            else:
                print("Error fetching data for page", page_num)
    else:
        print("Error fetching data:", response.status_code)
    
    return all_data

def write_to_csv(data, folder, filename):
    if not os.path.exists(folder):
        os.makedirs(folder)

    file_path = os.path.join(folder, filename)

    fieldnames = ['movie_data', 'credits_data']

    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for movie, credits in data:
            writer.writerow({'movie_data': str(movie), 'credits_data': str(credits)})

    print("Data written to", file_path)

def insert_into_mysql(csv_filename):
    try:
        connection = pymysql.connect(
            host="localhost",
            user="root",
            password="root@789",
            database="shubham"
        )
        
        cursor = connection.cursor()

        cursor.execute("CREATE TABLE IF NOT EXISTS raw_data (movie_data longTEXT, credits_data longTEXT)")

        with open(csv_filename, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                movie_data = row['movie_data']
                credits_data = row['credits_data']
                
                cursor.execute("INSERT INTO raw_data (movie_data, credits_data) VALUES (%s, %s)", (movie_data, credits_data))

        connection.commit()
        print("Data inserted into raw_data table.")
    except Exception as e:
        print("Error inserting data into raw_data table:", e)
    finally:
        if connection:
            cursor.close()
            connection.close()

def insert_into_mapped_data(data):
    try:
        connection = pymysql.connect(
            host="localhost",
            user="root",
            password="root@789",
            database="shubham"
        )
        
        cursor = connection.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS mapped_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            producer_name VARCHAR(255),
            release_date YEAR
        )
        """)

        for movie, credits in data:
            release_year = int(movie.get('release_date')[:4]) if movie.get('release_date') else None
            if release_year == 2024:
                movie_data = movie.get('title')
                release_date = release_year

                producer_names = []
                for crew_member in credits.get('crew', []):
                    if crew_member.get('job') == 'Producer':
                        producer_names.append(crew_member.get('name'))

                if producer_names:
                    for producer_name in producer_names:
                        cursor.execute("INSERT INTO mapped_data (name, producer_name, release_date) VALUES (%s, %s, %s)", (movie_data, producer_name, release_date))

        connection.commit()
        print("Data inserted into mapped_data table.")
    except Exception as e:
        print("Error inserting data into mapped_data table:", e)
    finally:
        if connection:
            cursor.close()
            connection.close()


def process_and_insert_into_consolidated_data():
    try:
        connection = pymysql.connect(
            host="localhost",
            user="root",
            password="root@789",
            database="shubham"
        )
        
        cursor = connection.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Consolidated_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            producer VARCHAR(255),
            year YEAR,
            total_movies INT
        )
        """)

        cursor.execute("""
        SELECT producer_name, release_date, COUNT(*) AS total_movies 
        FROM mapped_data 
        GROUP BY producer_name, release_date
        """)
        rows = cursor.fetchall()

        for row in rows:
            producer_name = row[0]
            release_year = row[1]
            total_movies = row[2]
            
            cursor.execute("INSERT INTO Consolidated_data (producer, year, total_movies) VALUES (%s, %s, %s)", (producer_name, release_year, total_movies))

        connection.commit()
        print("Data inserted into Consolidated_data table.")
    except Exception as e:
        print("Error inserting data into Consolidated_data table:", e)
    finally:
        if connection:
            cursor.close()
            connection.close()

def main():
    api_key = "e5aecae70222885cc851ab541e57c24e"
    current_date = datetime.now().strftime('%Y-%m-%d')
    folder = os.path.join(current_date)
    csv_filename = f'{current_date}.csv'

    data = fetch_movie_data(api_key)

    write_to_csv(data, folder, csv_filename)

    insert_into_mysql(os.path.join(folder, csv_filename))

    insert_into_mapped_data(data)

    process_and_insert_into_consolidated_data()

if __name__ == "__main__":
    main()
