from flask import Flask
from dotenv import load_dotenv
import os
from flask_migrate import Migrate # SUDHAR: Migrate import kiya gaya

# SUDHAR: Database object ko yahan define karein, lekin abhi tak app se connect na karein
# from .services.database import db # Yeh ek alag file mein ho sakta hai, ya hum seedhe Migrate ka istemal kar sakte hain.
# Saralt ke liye, hum abhi migrate object ko seedhe yahan banayenge.

# Database migration ke liye object
migrate = Migrate()

def create_app():
    """
    Application Factory: Flask app ko banata aur configure karta hai.
    """
    load_dotenv()
    
    app = Flask(__name__, instance_relative_config=True)

    # Secret key set karein
    app.config.from_mapping(
        SECRET_KEY=os.getenv("SECRET_KEY", os.urandom(24)),
        # SUDHAR: Database URI ko config mein add karein
        SQLALCHEMY_DATABASE_URI=os.getenv('DATABASE_URL'),
        SQLALCHEMY_TRACK_MODIFICATIONS=False # Behtar performance ke liye
    )

    # SUDHAR: Ab hum init_db() call nahi karenge. Migrate sab sambhal lega.
    # from .services import youtube_service
    # with app.app_context():
    #     youtube_service.init_db() # Iski ab zaroorat nahi hai.

    # SUDHAR: Database migrations ko app ke sath initialize karein
    from .services import db_setup # Ek nayi file banayenge iske liye
    db_setup.db.init_app(app)
    migrate.init_app(app, db_setup.db)


    # App ke routes (web pages) ko register karein
    from .routes import main_routes
    app.register_blueprint(main_routes.main_bp)

    return app
