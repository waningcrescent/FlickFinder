import time
import pandas as pd
from sqlalchemy import create_engine, text
import requests

# Database connections
netflix_engine = create_engine('mysql+pymysql://root:1234@localhost/netflix', pool_recycle=3600)
amazon_engine = create_engine('mysql+pymysql://root:1234@localhost/amazon', pool_recycle=3600)
hotstar_engine = create_engine('mysql+pymysql://root:1234@localhost/hotstar', pool_recycle=3600)
warehouse_engine = create_engine('mysql+pymysql://root:1234@localhost/iia', pool_recycle=3600)

# TMDB API Configuration
api_key = "c0130ed03393870771f01685ef2ae381"
base_url = 'https://api.themoviedb.org/3'
image_base_url = 'https://image.tmdb.org/t/p/w500'

# Function to fetch unprocessed logs from trigger_log
def fetch_trigger_logs(engine):
    query = "SELECT * FROM trigger_log WHERE processed = 0 ORDER BY triggered_at"
    with engine.connect() as conn:
        logs = pd.read_sql(query, conn)
        print(f"Fetched logs: {len(logs)} entries")
        return logs

# Function to mark logs as processed
def mark_log_as_processed(engine, log_id):
    query = text("UPDATE trigger_log SET processed = 1 WHERE id = :log_id")
    with engine.begin() as conn:  # Automatically commits on success
        conn.execute(query, {"log_id": log_id})
    print(f"Marked log ID {log_id} as processed.")

# Function to enrich data using TMDB API
def enrich_with_tmdb_data(row):
    try:
        is_movie = row['type'].lower() == 'movie'
        search_type = 'movie' if is_movie else 'tv'
        search_url = f"{base_url}/search/{search_type}?api_key={api_key}&query={row['title']}"
        response = requests.get(search_url)
        response.raise_for_status()

        results = response.json().get('results', [])
        if not results:
            print(f"No results found for '{row['title']}' via TMDB API.")
            return row

        tmdb_id = results[0]['id']
        detail_url = f"{base_url}/{search_type}/{tmdb_id}?api_key={api_key}&append_to_response=credits"
        detail_response = requests.get(detail_url)
        detail_response.raise_for_status()
        details = detail_response.json()
        #director fix
        crew = details.get('credits', {}).get('crew', [])
        director = next((person['name'] for person in crew if person['job'] == 'Director'), None)
        row['director'] = director or row.get('director', 'Unknown')
        ##
        row['description'] = details.get('overview', row.get('description'))
        row['poster_url'] = f"{image_base_url}{details.get('poster_path')}" if details.get('poster_path') else row.get('poster_url')
        row['runtime'] = details.get('runtime', row.get('runtime'))
        row['production_countries'] = ', '.join([country['name'] for country in details.get('production_countries', [])])
        row['imdb_id'] = details.get('imdb_id', row.get('imdb_id'))
        row['imdb_score'] = details.get('vote_average', row.get('imdb_score'))

        release_date = details.get('release_date')
        if release_date:
            row['year'] = int(release_date.split('-')[0])

        print(f"Enriched data for '{row['title']}' via TMDB API.")
    except Exception as e:
        print(f"Failed to enrich '{row['title']}' via TMDB API. Error: {e}")
    return row

# Function to upsert rows into the warehouse
def upsert_movies(df, warehouse_engine, platform_flag):
    if df.empty:
        print("No data to upsert.")
        return

    for _, row in df.iterrows():
        row = row.to_dict()

        # Check if the movie already exists in the warehouse
        select_query = text("""
            SELECT * FROM movies WHERE title = :title
        """)
        try:
            with warehouse_engine.connect() as conn:
                existing_row = conn.execute(select_query, {"title": row['title']}).fetchone()

            if existing_row:
                # Update the platform-specific flag
                update_query = text(f"""
                    UPDATE movies
                    SET {platform_flag} = 1
                    WHERE title = :title
                """)
                with warehouse_engine.begin() as conn:
                    conn.execute(update_query, {"title": row['title']})
                print(f"Updated flag {platform_flag} for movie: {row['title']}")
            else:
                # Enrich data if necessary and insert the movie
                row = enrich_with_tmdb_data(row)
                row['is_netflix'] = 1 if platform_flag == 'is_netflix' else 0
                row['is_amazon'] = 1 if platform_flag == 'is_amazon' else 0
                row['is_hotstar'] = 1 if platform_flag == 'is_hotstar' else 0
                row['director'] = row.get('director', 'Unknown')  # Default value if director is missing


                insert_query = text("""
                    INSERT INTO movies (title, type, description, runtime, production_countries, imdb_id, imdb_score, is_amazon, year, is_hotstar, director, rating, is_netflix, poster_url)
                    VALUES (:title, :type, :description, :runtime, :production_countries, :imdb_id, :imdb_score, :is_amazon, :year, :is_hotstar, :director, :rating, :is_netflix, :poster_url)
                """)
                with warehouse_engine.begin() as conn:
                    conn.execute(insert_query, row)
                print(f"Inserted new movie: {row['title']}")
        except Exception as e:
            print(f"Error processing movie: {row['title']}. Error: {e}")

def upsert_movies(df, warehouse_engine, platform_flag):
    if df.empty:
        print("No data to upsert.")
        return

    for _, row in df.iterrows():
        row = row.to_dict()

        # Ensure missing fields are populated with default values
        row['rating'] = row.get('rating', 'Unknown')  # Default value if rating is missing
        row['director'] = row.get('director', 'Unknown')  # Default value if director is missing
        row['description'] = row.get('description', '')  # Default to an empty string if description is missing
        row['runtime'] = row.get('runtime', 0)  # Default to 0 for runtime
        row['production_countries'] = row.get('production_countries', '')  # Default to empty string
        row['imdb_score'] = row.get('imdb_score', None)  # Default to None for numeric fields
        row['year'] = row.get('year', None)  # Default to None for year
        row['poster_url'] = row.get('poster_url', '')  # Default to empty string for poster_url

        # Check if the movie already exists in the warehouse
        select_query = text(""" 
            SELECT * FROM movies WHERE title = :title 
        """)
        try:
            with warehouse_engine.connect() as conn:
                existing_row = conn.execute(select_query, {"title": row['title']}).fetchone()

            if existing_row:
                # Update the platform-specific flag
                update_query = text(f"""
                    UPDATE movies 
                    SET {platform_flag} = 1 
                    WHERE title = :title
                """)
                with warehouse_engine.begin() as conn:
                    conn.execute(update_query, {"title": row['title']})
                print(f"Updated flag {platform_flag} for movie: {row['title']}")
            else:
                # Enrich data if necessary and insert the movie
                row = enrich_with_tmdb_data(row)
                row['is_netflix'] = 1 if platform_flag == 'is_netflix' else 0
                row['is_amazon'] = 1 if platform_flag == 'is_amazon' else 0
                row['is_hotstar'] = 1 if platform_flag == 'is_hotstar' else 0

                insert_query = text("""
                    INSERT INTO movies (title, type, description, runtime, production_countries, imdb_id, imdb_score, 
                                        is_amazon, year, is_hotstar, director, rating, is_netflix, poster_url)
                    VALUES (:title, :type, :description, :runtime, :production_countries, :imdb_id, :imdb_score, 
                            :is_amazon, :year, :is_hotstar, :director, :rating, :is_netflix, :poster_url)
                """)
                with warehouse_engine.begin() as conn:
                    conn.execute(insert_query, row)
                print(f"Inserted new movie: {row['title']}")
        except Exception as e:
            print(f"Error processing movie: {row['title']}. Error: {e}")


# Function to process a single log entry
def process_trigger_log(log, source_engine, warehouse_engine):
    table_name = log['table_name']
    operation = log['operation']
    row_id = log['row_id']
    title_from_log = log['title']

    id_column_map = {
        'Netflix': 'show_id',
        'AmazonPrime': 'id',
        'Hotstar': 'hotstar_id',
    }
    platform_flag_map = {
        'Netflix': 'is_netflix',
        'AmazonPrime': 'is_amazon',
        'Hotstar': 'is_hotstar',
    }

    id_column = id_column_map.get(table_name)
    platform_flag = platform_flag_map.get(table_name)

    if operation in ('INSERT', 'UPDATE'):
        query = f"SELECT * FROM `{table_name}` WHERE {id_column} = %s"
        try:
            with source_engine.connect() as conn:
                row_df = pd.read_sql(query, conn, params=(row_id,))
            if not row_df.empty:
                upsert_movies(row_df, warehouse_engine, platform_flag)
        except Exception as e:
            print(f"Error processing {operation} for {table_name}. ID: {row_id}. Error: {e}")

    elif operation == 'DELETE':
        if not title_from_log:
            print(f"Skipping DELETE operation for log ID {log['id']}: Title is missing.")
            return

        # Update platform flag to 0 and delete if all flags are 0
        update_query = text(f"""
            UPDATE movies 
            SET {platform_flag} = 0
            WHERE title = :title;
        """)
        delete_query = text("""
            DELETE FROM movies 
            WHERE is_netflix = 0 AND is_amazon = 0 AND is_hotstar = 0 AND title = :title;
        """)
        try:
            with warehouse_engine.begin() as conn:
                conn.execute(update_query, {"title": title_from_log})
                conn.execute(delete_query, {"title": title_from_log})
            print(f"Processed DELETE operation for title '{title_from_log}'.")
        except Exception as e:
            print(f"Error deleting row with title '{title_from_log}' from warehouse: {e}")

# Main real-time monitoring loop
def incremental_load():
    while True:
        try:
            print("Starting incremental load cycle...")
            for source_engine, source_name in [
                (netflix_engine, 'Netflix'),
                (amazon_engine, 'AmazonPrime'),
                (hotstar_engine, 'Hotstar'),
            ]:
                logs = fetch_trigger_logs(source_engine)
                for _, log in logs.iterrows():
                    process_trigger_log(log, source_engine, warehouse_engine)
                    mark_log_as_processed(source_engine, log['id'])
            time.sleep(5)
        except Exception as e:
            print(f"Error in incremental load cycle: {e}")

if __name__ == "__main__":
    incremental_load()
