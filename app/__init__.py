from flask import Flask
from dotenv import load_dotenv
import os

def create_app():
    """
    Application Factory: Flask app ko banata aur configure karta hai.
    """
    load_dotenv()

    # **SOLUTION for TemplateNotFound**: __name__ se Flask ko pata chalta hai ki
    # templates aur static folder isi 'app' directory ke andar hain.
    app = Flask(__name__, instance_relative_config=True)
    
    # Secret key set karein (flash messages ke liye zaroori)
    app.config.from_mapping(
        SECRET_KEY=os.urandom(24)
    )

    # Database ko app ke sath initialize karein
    from .services import youtube_service
    with app.app_context():
        youtube_service.init_db()
    
    # App ke routes (web pages) ko register karein
    from .routes import main_routes
    app.register_blueprint(main_routes.main_bp)

    return app
