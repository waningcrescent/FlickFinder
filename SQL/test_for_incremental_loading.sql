-- Step 1: Insert a movie into Netflix with partial data
USE netflix;

INSERT INTO Netflix (show_id, type, title, director, cast, country, date_added, release_year, rating, duration, listed_in, description)
VALUES ('1148172', 'Movie', 'One True Loves', NULL, NULL, NULL, '2024-11-25', 2023, NULL, NULL, NULL, NULL);

-- Verify that the movie is partially added to `iia.movies`


-- Wait for the Python script to process the log and enrich data in `iia.movies`
-- You can simulate a delay to allow the Python script to run (e.g., wait for a few seconds).

-- Step 3: Verify that the missing data in `iia.movies` is populated by the API
USE iia;

SELECT * FROM movies WHERE title = 'One True Loves';

-- Expected Output:
-- The `movies` table should now include enriched fields like `runtime`, `production_countries`, `imdb_id`, `imdb_score`, `poster_url`, `director`, etc.

-- Step 4: Add the same movie into AmazonPrime with partial data
USE amazon;

INSERT INTO AmazonPrime (id, title, type, description, release_year, age_certification, runtime, genres, production_countries, seasons, imdb_id, imdb_score, imdb_votes, tmdb_popularity, tmdb_score)
VALUES ('1148172', 'One True Loves', 'Movie', 'A romantic movie based on a novel.', 2023, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- Verify the update in `iia.movies`
USE iia;

SELECT * FROM movies WHERE title = 'One True Loves';

-- Expected Output:
-- The `is_amazon` flag should now be updated to `1` in the `movies` table, and all other fields should remain enriched.

-- Step 5: Delete the movie from AmazonPrime
USE amazon;

DELETE FROM AmazonPrime WHERE id = '1148172';

-- Verify that `is_amazon` is set to `0` in `iia.movies`
USE iia;

SELECT * FROM movies WHERE title = 'One True Loves';

-- Expected Output:
-- The `is_amazon` flag should be updated to `0` in the `movies` table, but the row should still exist because `is_netflix` is `1`.

-- Step 6: Delete the movie from Netflix
USE netflix;

DELETE FROM Netflix WHERE show_id = '1148172';

-- Verify that the movie is deleted from `iia.movies` entirely
USE iia;

SELECT * FROM movies WHERE title = 'One True Loves';

-- Expected Output:
-- The movie should no longer exist in the `movies` table because all flags (`is_netflix`, `is_amazon`, `is_hotstar`) are `0`.
