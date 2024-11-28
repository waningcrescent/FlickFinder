USE iia;
-- Drop existing views if any
DROP VIEW IF EXISTS amazon_view;
DROP VIEW IF EXISTS netflix_view;
DROP VIEW IF EXISTS hotstar_view;

-- Create Amazon View
CREATE VIEW amazon_view AS
SELECT 
    id,              
    title,                       
    type,                         
    description,
    year, 
    rating as age_certification,           
    runtime,         
    NULL as genres,              
    production_countries,    
    NULL as seasons,                      
    imdb_id,                       
    imdb_score,  
    NULL AS imdb_votes,
    NULL AS tmdb_popularity,    
    NULL AS tmdb_score,          
    NULL AS episodes             
FROM movies 
WHERE is_amazon = '1';

-- Create Netflix View
CREATE VIEW netflix_view AS
SELECT 
    id AS show_id,          
    type,                        
    title,                        
    director,                     
    NULL AS cast,                 
    production_countries AS country, 
    NULL AS date_added,           
    year,                         
    rating,                      
    runtime AS duration,            
    NULL AS listed_in,          
    description                   
FROM movies 
WHERE is_netflix = '1';

-- Create Hotstar View
CREATE VIEW hotstar_view AS
SELECT 
    id AS hotstar_id,       -- Maps 'movie_id' to Hotstar's 'hotstar_id'
    title,                        -- Maps directly
    NULL AS description,          -- Hotstar schema does not provide 'description'
    NULL AS genre,              -- Maps 'genres' directly
    year,                         -- Maps directly
    rating AS age_rating,         -- Maps 'rating' to Hotstar's 'age_rating'
    runtime AS running_time,      -- Maps 'runtime' to 'running_time'
    NULL AS seasons,              -- Hotstar schema does not provide 'seasons'
    NULL AS episodes,             -- Hotstar schema does not provide 'episodes'
    type                          -- Maps directly
FROM movies 
WHERE is_hotstar = '1';


-- Query Amazon View
SELECT * FROM amazon_view;

-- Query Netflix View
SELECT * FROM netflix_view;

-- Query Hotstar View
SELECT * FROM hotstar_view;

