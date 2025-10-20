from app import create_app
import os # SUDHAR: os import kiya gaya

# .flaskenv ya environment variable se FLASK_DEBUG ka istemal karega
app = create_app()

if __name__ == '__main__':
    # Production mein debug mode ko False rakhein, development mein True
    # Yeh environment variable se control karna behtar hai
    debug_mode = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(debug=debug_mode)

