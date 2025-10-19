from flask import Flask
from dotenv import load_dotenv
import os

def create_app():
    load_dotenv()

    # --- SOLUTION ---
    # We tell Flask that the root path is the 'app' directory
    # This will automatically find the 'templates' and 'static' folders inside it.
    app = Flask(__name__, instance_relative_config=True)
    
    app.config.from_mapping(
        SECRET_KEY=os.urandom(24)
    )

    # Database ko initialize karein
    from .services import youtube_service
    with app.app_context():
        youtube_service.init_db()
    
    # Routes ko register karein
    from .routes import main_routes
    app.register_blueprint(main_routes.main_bp)

    return app
