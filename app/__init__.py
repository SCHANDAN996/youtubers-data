from flask import Flask
from dotenv import load_dotenv
import os

def create_app():
    # .env file se variables load karein
    load_dotenv()

    app = Flask(__name__)
    app.secret_key = os.urandom(24)

    # Database ko initialize karein
    from app.services import youtube_service
    youtube_service.init_db()
    
    # Routes ko register karein
    with app.app_context():
        from .routes import main_routes
        app.register_blueprint(main_routes.main_bp)

    return app
