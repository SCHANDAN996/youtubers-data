from app import create_app

# Application factory se app instance banayein
app = create_app()

# Agar yeh file seedhe chalaayi jaaye, to app ko run karein
if __name__ == '__main__':
    # Debug mode production mein False hona chahiye
    app.run(debug=False)
