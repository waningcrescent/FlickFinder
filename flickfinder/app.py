from flask import Flask, render_template, request
from models import db, Movie
from sqlalchemy import create_engine, text

app = Flask(__name__)

# Connect to the database
engine = create_engine('mysql+pymysql://root:1234@127.0.0.1/iia')


@app.route('/', methods=['GET'])
def home():
    filters = []
    params = {}

    # Get filter parameters from the request
    title = request.args.get('title', '')
    genre = request.args.get('genre', '')
    min_rating = request.args.get('min_rating', 0)
    director = request.args.get('director', '')
    platform = request.args.getlist('platform')  # Multiple platforms

    # Build dynamic query with filters
    query = "SELECT * FROM movies WHERE imdb_score >= :min_rating"
    params['min_rating'] = min_rating

    if title:
        query += " AND title LIKE :title"
        params['title'] = f"%{title}%"
    if genre:
        query += " AND genre = :genre"
        params['genre'] = genre
    if director:
        query += " AND director LIKE :director"
        params['director'] = f"%{director}%"
    if platform:
        query += " AND platform_name IN :platform"
        params['platform'] = tuple(platform)

    query += " ORDER BY imdb_score DESC LIMIT 10"

    # Execute the query and fetch data
    with engine.connect() as connection:
        movies = connection.execute(text(query), params).fetchall()

    return render_template('index.html', movies=movies)

@app.route('/details/<int:movie_id>', methods=['GET'])
def movie_details(movie_id):
    query = """
    SELECT
        m.id,
        m.title,
        m.runtime,
        m.year AS release_year,
        m.description AS overview,
        m.imdb_score,
        GROUP_CONCAT(DISTINCT g.name SEPARATOR ', ') AS genres,
        m.rating,
        m.director,
        m.poster_url,
        m.is_netflix,
        m.is_amazon,
        m.is_hotstar,
        GROUP_CONCAT(DISTINCT c.name SEPARATOR ', ') AS cast
    FROM movies m
    LEFT JOIN movie_genres mg ON m.id = mg.movie_id
    LEFT JOIN genres g ON mg.genre_id = g.id
    LEFT JOIN movie_cast mc ON m.id = mc.movie_id
    LEFT JOIN cast c ON mc.cast_id = c.id
    WHERE m.id = :movie_id
    GROUP BY m.id
    """
    with engine.connect() as connection:
        result = connection.execute(text(query), {'movie_id': movie_id}).fetchone()

    if not result:
        return "Movie not found", 404

    # Map result to a dictionary for rendering
    movie = {
        "id": result[0],
        "title": result[1],
        "runtime": result[2],
        "release_year": result[3],
        "overview": result[4],
        "imdb_score": result[5],
        "genres": result[6],
        "rating": result[7],
        "director": result[8],
        "poster_url": result[9],
        "platforms": {
            "Netflix": result[10],
            "Amazon": result[11],
            "Hotstar": result[12]
        },
        "cast": result[13]  # Cast information
    }

    return render_template('details.html', movie=movie)

'''
@app.route('/details/<int:movie_id>', methods=['GET'])
def movie_details(movie_id):
    query = """
    SELECT
        m.id,
        m.title,
        m.runtime,
        m.year AS release_year,
        m.description AS overview,
        m.imdb_score,
        GROUP_CONCAT(g.name SEPARATOR ', ') AS genres,
        m.rating,
        m.director,
        m.poster_url,
        m.is_netflix,
        m.is_amazon,
        m.is_hotstar
    FROM movies m
    LEFT JOIN movie_genres mg ON m.id = mg.movie_id
    LEFT JOIN genres g ON mg.genre_id = g.id
    WHERE m.id = :movie_id
    GROUP BY m.id
    """
    with engine.connect() as connection:
        result = connection.execute(text(query), {'movie_id': movie_id}).fetchone()

    if not result:
        return "Movie not found", 404

    # Map result to a dictionary for rendering (accessing by index)
    movie = {
        "id": result[0],
        "title": result[1],
        "runtime": result[2],
        "release_year": result[3],
        "overview": result[4],
        "imdb_score": result[5],
        "genres": result[6],
        "rating": result[7],
        "director": result[8],
        "poster_url": result[9],
        "platforms": {
            "Netflix": result[10],
            "Amazon": result[11],
            "Hotstar": result[12]
        },
    }

    return render_template('details.html', movie=movie)
'''
from sqlalchemy import text
from rapidfuzz import process, fuzz
from Levenshtein import distance as levenshtein_distance

'''
@app.route('/filter', methods=['GET'])

def filter_movies():
    # Get filter parameters from the request
    platforms = request.args.getlist('platform')
    types = request.args.getlist('type')
    cast = request.args.get('cast_members', '').strip()
    rating = request.args.get('rating_type')
    year = request.args.get('year')
    min_runtime = request.args.get('min_runtime')
    max_runtime = request.args.get('max_runtime')
    min_rating = request.args.get('min_rating')
    max_rating = request.args.get('max_rating')
    genres = request.args.getlist('genre')
    search = request.args.get('search', '').strip()

    # Ensure at least one filter is provided
    if not (platforms or types or cast or rating or year or min_runtime or max_runtime or min_rating or max_rating or genres or search):
        return render_template('filtered_results.html', movies=[], error="Please provide at least one query parameter.")

    # Base SQL query
    query = """
    SELECT m.id, m.title, m.poster_url, m.year, m.imdb_score, 
           GROUP_CONCAT(DISTINCT g.name) AS genres, 
           GROUP_CONCAT(DISTINCT c.name) AS cast_members
    FROM movies m
    LEFT JOIN movie_genres mg ON m.id = mg.movie_id
    LEFT JOIN genres g ON mg.genre_id = g.id
    LEFT JOIN movie_cast mc ON m.id = mc.movie_id
    LEFT JOIN cast c ON mc.cast_id = c.id
    WHERE 1=1
    """
    params = {}

    # Add platform filter (OR logic within platform group)
    if platforms:
        query += " AND (" + " OR ".join([f"m.is_{p.lower()} = 1" for p in platforms]) + ")"

    # Add type filter (OR logic within type group)
    if types:
        query += " AND m.type IN :types"
        params['types'] = tuple(types)

    # Add rating filter
    if rating:
        query += " AND m.rating = :rating"
        params['rating'] = rating

    # Add year filter
    if year:
        query += " AND m.year IN :year"
        params['year'] = tuple(year.split(','))

    # Add runtime filters
    if min_runtime:
        query += " AND m.runtime >= :min_runtime"
        params['min_runtime'] = min_runtime

    if max_runtime:
        query += " AND m.runtime <= :max_runtime"
        params['max_runtime'] = max_runtime

    # Add IMDb rating filters
    if min_rating:
        query += " AND m.imdb_score >= :min_rating"
        params['min_rating'] = min_rating

    if max_rating:
        query += " AND m.imdb_score <= :max_rating"
        params['max_rating'] = max_rating

    # Add genre filter (intersection logic)
    if genres:
        query += f"""
        AND m.id IN (
            SELECT mg.movie_id
            FROM movie_genres mg
            JOIN genres g ON mg.genre_id = g.id
            WHERE g.name IN :genres
            GROUP BY mg.movie_id
            HAVING COUNT(DISTINCT g.name) = {len(genres)}
        )
        """
        params['genres'] = tuple(genres)

    # Add fuzzy matching for cast members
    with engine.connect() as connection:
        if cast:
            cast_list = [name.strip() for name in cast.split(',') if name.strip()]
        matched_cast_ids = []

        with engine.connect() as connection:
            # Fetch all cast names from the database
            all_cast_members = connection.execute(text("SELECT id, name FROM cast")).fetchall()
            all_cast = {row[0]: row[1] for row in all_cast_members}

            # Perform approximate matching for each provided cast name
            for cast_input in cast_list:
                matched_ids = [
                    cast_id
                    for cast_id, cast_name in all_cast.items()
                    if levenshtein_distance(cast_input.lower(), cast_name.lower()) <= 4
                ]
                matched_cast_ids.append(set(matched_ids))

        # Ensure movies match ALL specified cast members
        if matched_cast_ids:
            # Intersect matched IDs for all cast members
            common_cast_ids = set.intersection(*matched_cast_ids)
            if common_cast_ids:
                query += f"""
                AND m.id IN (
                    SELECT mc.movie_id
                    FROM movie_cast mc
                    WHERE mc.cast_id IN :matched_cast_ids
                    GROUP BY mc.movie_id
                    HAVING COUNT(DISTINCT mc.cast_id) = {len(cast_list)}
                )
                """
                params['matched_cast_ids'] = tuple(common_cast_ids)
            else:
                return render_template('filtered_results.html', movies=[], error="No matches found for the provided cast members.")

        # Add fuzzy matching for movie title
        if search:
            # Fetch all movie titles from the database
            all_movies = connection.execute(text("SELECT DISTINCT title FROM movies")).fetchall()
            all_titles = [row[0] for row in all_movies]

            # Perform fuzzy matching for the search query
            matches = process.extract(search, all_titles, scorer=fuzz.WRatio, limit=10)
            matched_titles = [match[0] for match in matches if match[1] > 70]

            if matched_titles:
                query += " AND m.title IN :matched_titles"
                params['matched_titles'] = tuple(matched_titles)

    # Group by movie title and sort by IMDb score
    query += """
    GROUP BY m.id, m.title, m.poster_url, m.year, m.imdb_score
    ORDER BY CASE WHEN m.imdb_score IS NULL THEN 1 ELSE 0 END, m.imdb_score DESC
    """

    # Execute the query and fetch results
    with engine.connect() as connection:
        results = connection.execute(text(query), params).fetchall()

    # Render results
    return render_template('filtered_results.html', movies=results)
'''
@app.route('/filter', methods=['GET'])
def filter_movies():
    # Get filter parameters from the request
    platforms = request.args.getlist('platform')
    types = request.args.getlist('type')
    cast = request.args.get('cast_members', '').strip()
    rating = request.args.get('rating_type')
    year = request.args.get('year')
    min_runtime = request.args.get('min_runtime')
    max_runtime = request.args.get('max_runtime')
    min_rating = request.args.get('min_rating')
    max_rating = request.args.get('max_rating')
    genres = request.args.getlist('genre')
    search = request.args.get('search', '').strip()

    # Ensure at least one filter is provided
    if not (platforms or types or cast or rating or year or min_runtime or max_runtime or min_rating or max_rating or genres or search):
        return render_template('filtered_results.html', movies=[], error="Please provide at least one query parameter.")

    # Base SQL query
    query = """
    SELECT m.id, m.title, m.poster_url, m.year, m.imdb_score, 
           GROUP_CONCAT(DISTINCT g.name) AS genres, 
           GROUP_CONCAT(DISTINCT c.name) AS cast_members
    FROM movies m
    LEFT JOIN movie_genres mg ON m.id = mg.movie_id
    LEFT JOIN genres g ON mg.genre_id = g.id
    LEFT JOIN movie_cast mc ON m.id = mc.movie_id
    LEFT JOIN cast c ON mc.cast_id = c.id
    WHERE 1=1
    """
    params = {}

    # Add platform filter (OR logic within platform group)
    if platforms:
        query += " AND (" + " OR ".join([f"m.is_{p.lower()} = 1" for p in platforms]) + ")"

    # Add type filter (OR logic within type group)
    if types:
        query += " AND m.type IN :types"
        params['types'] = tuple(types)

    # Add rating filter
    if rating:
        query += " AND m.rating = :rating"
        params['rating'] = rating

    # Add year filter
    if year:
        query += " AND m.year IN :year"
        params['year'] = tuple(year.split(','))

    # Add runtime filters
    if min_runtime:
        query += " AND m.runtime >= :min_runtime"
        params['min_runtime'] = min_runtime

    if max_runtime:
        query += " AND m.runtime <= :max_runtime"
        params['max_runtime'] = max_runtime

    # Add IMDb rating filters
    if min_rating:
        query += " AND m.imdb_score >= :min_rating"
        params['min_rating'] = min_rating

    if max_rating:
        query += " AND m.imdb_score <= :max_rating"
        params['max_rating'] = max_rating

    # Add genre filter (intersection logic)
    if genres:
        query += f"""
        AND m.id IN (
            SELECT mg.movie_id
            FROM movie_genres mg
            JOIN genres g ON mg.genre_id = g.id
            WHERE g.name IN :genres
            GROUP BY mg.movie_id
            HAVING COUNT(DISTINCT g.name) = {len(genres)}
        )
        """
        params['genres'] = tuple(genres)

    # Initialize variables for cast matching
    cast_list = []
    matched_cast_ids = []

    if cast:
        cast_list = [name.strip() for name in cast.split(',') if name.strip()]

    matched_titles = []
    matched_titles_with_scores = []

    with engine.connect() as connection:
        if cast_list:
            # Fetch all cast names from the database
            all_cast_members = connection.execute(text("SELECT id, name FROM cast")).fetchall()
            all_cast = {row[0]: row[1] for row in all_cast_members}

            # Perform approximate matching for each provided cast name
            for cast_input in cast_list:
                matched_ids = [
                    cast_id
                    for cast_id, cast_name in all_cast.items()
                    if levenshtein_distance(cast_input.lower(), cast_name.lower()) <= 3
                ]
                matched_cast_ids.append(set(matched_ids))

        # Ensure movies match ALL specified cast members
        if matched_cast_ids:
            common_cast_ids = set.intersection(*matched_cast_ids)
            if common_cast_ids:
                query += f"""
                AND m.id IN (
                    SELECT mc.movie_id
                    FROM movie_cast mc
                    WHERE mc.cast_id IN :matched_cast_ids
                    GROUP BY mc.movie_id
                    HAVING COUNT(DISTINCT mc.cast_id) = {len(cast_list)}
                )
                """
                params['matched_cast_ids'] = tuple(common_cast_ids)
            else:
                return render_template('filtered_results.html', movies=[], error="No matches found for the provided cast members.")

        # Add fuzzy matching for movie title
        if search:
            all_movies = connection.execute(text("SELECT DISTINCT title FROM movies")).fetchall()
            all_titles = [row[0] for row in all_movies]

            # Perform fuzzy matching for the search query
            matches = process.extract(search, all_titles, scorer=fuzz.WRatio, limit=10)
            matched_titles = [match[0] for match in matches if match[1] > 70]
            matched_titles_with_scores = [(match[0], match[1]) for match in matches if match[1] > 70]

            if matched_titles:
                query += " AND m.title IN :matched_titles"
                params['matched_titles'] = tuple(matched_titles)

    query += """
    GROUP BY m.id, m.title, m.poster_url, m.year, m.imdb_score
    """

    with engine.connect() as connection:
        results = connection.execute(text(query), params).fetchall()

    # Handle ordering
    if search and matched_titles_with_scores:
        title_score_map = {title: score for title, score in matched_titles_with_scores}
        results = sorted(results, key=lambda movie: title_score_map.get(movie[1], 0), reverse=True)
    else:
        results = sorted(results, key=lambda movie: (movie[4] is None, movie[4]), reverse=True)

    return render_template('filtered_results.html', movies=results)

@app.route('/genres')
def genres():
    query = """
    SELECT gtm.genre, gtm.movie_id, 
           MIN(gtm.title) AS title, 
           MAX(gtm.imdb_score) AS imdb_score, 
           MAX(m.poster_url) AS poster_url
    FROM genre_top_movies gtm
    JOIN movies m ON gtm.movie_id = m.id
    GROUP BY gtm.genre, gtm.movie_id
    ORDER BY gtm.genre, MIN(gtm.pos)
    """
    with engine.connect() as connection:
        results = connection.execute(text(query)).fetchall()

    # Organize results by genre
    genre_data = {}
    for row in results:
        genre = row[0]  # Access by index
        if genre not in genre_data:
            genre_data[genre] = []
        genre_data[genre].append({
            'id': row[1],  # Movie ID
            'title': row[2],  # Movie Title
            'imdb_score': row[3],  # IMDb Score
            'poster_url': row[4],  # Poster URL from movies table
        })

    return render_template('genres.html', genre_data=genre_data)


if __name__ == "__main__":
    app.run(debug=True)
