-- Step 1: Create the Data Mart Table
CREATE TABLE IF NOT EXISTS genre_top_movies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    genre VARCHAR(100) NOT NULL,
    movie_id INT NOT NULL,
    title VARCHAR(255),
    imdb_score DECIMAL(3, 1),
    pos INT,
    FOREIGN KEY (movie_id) REFERENCES movies(id)
);

-- Step 2: Populate the Data Mart with Initial Data
-- Step 2: Populate the Data Mart with Initial Data
INSERT INTO genre_top_movies (genre, movie_id, title, imdb_score, pos)
SELECT 
    ranked_movies.genre,
    ranked_movies.movie_id,
    ranked_movies.title,
    ranked_movies.imdb_score,
    ranked_movies.rank_val
FROM (
    SELECT 
        g.name AS genre,
        m.id AS movie_id,
        m.title,
        m.imdb_score,
        RANK() OVER (PARTITION BY g.name ORDER BY m.imdb_score DESC) AS rank_val
    FROM 
        movies m
    JOIN 
        movie_genres mg ON m.id = mg.movie_id
    JOIN 
        genres g ON mg.genre_id = g.id
    WHERE 
        m.imdb_score IS NOT NULL
) ranked_movies
WHERE ranked_movies.rank_val <= 10;


DELIMITER //
CREATE TRIGGER update_genre_top_movies_after_insert
AFTER INSERT ON movies
FOR EACH ROW
BEGIN
    -- Delete existing rankings for affected genres
    DELETE FROM genre_top_movies WHERE genre IN (
        SELECT g.name 
        FROM movie_genres mg
        JOIN genres g ON mg.genre_id = g.id
        WHERE mg.movie_id = NEW.id
    );

    -- Recompute rankings and insert into the data mart
    INSERT INTO genre_top_movies (genre, movie_id, title, imdb_score, pos)
    SELECT 
        ranked_movies.genre,
        ranked_movies.movie_id,
        ranked_movies.title,
        ranked_movies.imdb_score,
        ranked_movies.rank_val
    FROM (
        SELECT 
            g.name AS genre,
            m.id AS movie_id,
            m.title,
            m.imdb_score,
            RANK() OVER (PARTITION BY g.name ORDER BY m.imdb_score DESC) AS rank_val
        FROM 
            movies m
        JOIN 
            movie_genres mg ON m.id = mg.movie_id
        JOIN 
            genres g ON mg.genre_id = g.id
        WHERE 
            g.name IN (SELECT name FROM genres WHERE id IN (SELECT genre_id FROM movie_genres WHERE movie_id = NEW.id))
    ) ranked_movies
    WHERE ranked_movies.rank_val <= 10;
END;
//
DELIMITER ;


-- Step 4: Create Trigger for Updates to Movie Records
DELIMITER //
CREATE TRIGGER update_genre_top_movies_after_update
AFTER UPDATE ON movies
FOR EACH ROW
BEGIN
    -- Delete existing rankings for affected genres
    DELETE FROM genre_top_movies WHERE genre IN (
        SELECT g.name 
        FROM movie_genres mg
        JOIN genres g ON mg.genre_id = g.id
        WHERE mg.movie_id = NEW.id
    );

    -- Recompute rankings and insert into the data mart
    INSERT INTO genre_top_movies (genre, movie_id, title, imdb_score, pos)
    SELECT 
        ranked_movies.genre,
        ranked_movies.movie_id,
        ranked_movies.title,
        ranked_movies.imdb_score,
        ranked_movies.rank_val
    FROM (
        SELECT 
            g.name AS genre,
            m.id AS movie_id,
            m.title,
            m.imdb_score,
            RANK() OVER (PARTITION BY g.name ORDER BY m.imdb_score DESC) AS rank_val
        FROM 
            movies m
        JOIN 
            movie_genres mg ON m.id = mg.movie_id
        JOIN 
            genres g ON mg.genre_id = g.id
        WHERE 
            g.name IN (SELECT name FROM genres WHERE id IN (SELECT genre_id FROM movie_genres WHERE movie_id = NEW.id))
    ) ranked_movies
    WHERE ranked_movies.rank_val <= 10;
END;
//
DELIMITER ;


-- Step 5: Create a Scheduled Job (Optional)
-- For environments where triggers are not ideal, use a scheduled job.
DELIMITER //
CREATE EVENT update_genre_top_movies_event
ON SCHEDULE EVERY 1 HOUR
DO
BEGIN
    DELETE FROM genre_top_movies;

    INSERT INTO genre_top_movies (genre, movie_id, title, imdb_score, pos)
    SELECT 
        g.name AS genre,
        m.id AS movie_id,
        m.title,
        m.imdb_score,
        RANK() OVER (PARTITION BY g.name ORDER BY m.imdb_score DESC) AS pos
    FROM 
        movies m
    JOIN 
        movie_genres mg ON m.id = mg.movie_id
    JOIN 
        genres g ON mg.genre_id = g.id
    WHERE 
        m.imdb_score IS NOT NULL
    HAVING 
        pos <= 10;
END;
//
DELIMITER ;

-- Step 6: Query the Data Mart for a Genre
-- Example Query to Fetch Top 10 Movies for a Specific Genre
SELECT 
    movie_id, title, imdb_score, pos
FROM 
    genre_top_movies
WHERE 
    genre = 'Comedy'
ORDER BY pos;
