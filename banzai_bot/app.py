from flask import Flask, render_template
import requests
from banzai_bot import utils

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/reprocess_dayobs')
def reprocess_dayobs():
    pass


    

