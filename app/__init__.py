from flask import Flask
from dotenv import load_dotenv
import os
# flask_migrate को हटा दिया गया है

def create_app():
    """
    Application Factory: Flask app ko banata aur configure karta hai.
    """
    load_dotenv()
    
    app = Flask(__name__, instance_relative_config=True)

    # Secret key set karein
    app.config.from_mapping(
        SECRET_KEY=os.getenv("SECRET_KEY", os.urandom(24)),
        # नोट: नीचे की दो लाइनें अब ज़रूरी नहीं हैं, पर रखने से कोई नुकसान नहीं है
        SQLALCHEMY_DATABASE_URI=os.getenv('DATABASE_URL'),
        SQLALCHEMY_TRACK_MODIFICATIONS=False
    )

    # माइग्रेशन से संबंधित सभी कोड को यहाँ से हटा दिया गया है

    # App ke routes (web pages) ko register karein
    from .routes import main_routes
    app.register_blueprint(main_routes.main_bp)

    return app
