-- Movies Table
CREATE TABLE movies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    description TEXT,
    runtime INT,
    production_countries VARCHAR(255),
    imdb_id VARCHAR(15) UNIQUE,
    imdb_score DECIMAL(3, 1),
    is_amazon TINYINT(1) DEFAULT 0,
    year YEAR,
    is_hotstar TINYINT(1) DEFAULT 0,
    director VARCHAR(255),
    rating VARCHAR(10),
    is_netflix TINYINT(1) DEFAULT 0,
    poster_url TEXT
);

-- Genres Table
CREATE TABLE genres (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) UNIQUE
);

-- Movie Genres Table (Many-to-Many)
CREATE TABLE movie_genres (
    movie_id INT,
    genre_id INT,
    FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, genre_id)
);

-- Cast Table
CREATE TABLE cast (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) UNIQUE
);

-- Movie Cast Table (Many-to-Many)
CREATE TABLE movie_cast (
    movie_id INT,
    cast_id INT,
    FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
    FOREIGN KEY (cast_id) REFERENCES iia.cast(id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, cast_id)
);
