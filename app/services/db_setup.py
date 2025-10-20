from flask_sqlalchemy import SQLAlchemy

# SQLAlchemy ka ek instance banayein, jo hamare models ke liye base class ka kaam karega
db = SQLAlchemy()

# Ab aap apne database models (tables) ko yahan define kar sakte hain
# Example:
# class Channels(db.Model):
#     id = db.Column(db.Integer, primary_key=True)
#     ...
# Lekin abhi ke liye, hum psycopg2 ka istemal karte rahenge, isliye models define nahi kar rahe.
# Flask-Migrate ko kaam karne ke liye bas 'db' object ki zaroorat hai.
