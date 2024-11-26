from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

# Database configuration
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:1234@127.0.0.1/iia'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Define the Movie model for ORM-based querying
class Movie(db.Model):
    __tablename__ = 'movies'
    movie_id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(255))
    release_year = db.Column(db.Integer)
    genre = db.Column(db.String(50))
    imdb_score = db.Column(db.Float)
    runtime = db.Column(db.Integer)
    platform_name = db.Column(db.String(50))
    director = db.Column(db.String(255))
    poster_url = db.Column(db.String(255))
