from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'YouTubers Data Tool is under construction.'

if __name__ == '__main__':
    app.run(debug=True)
