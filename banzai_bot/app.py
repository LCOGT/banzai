from flask import Flask, render_template
import requests
from banzai_bot import utils
from forms import ReprocessDayObsForm

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/reprocess_dayobs')
def reprocess_dayobs():
    sites, instruments = utils.get_sites_and_instruments()
    form = ReprocessDayObsForm()
    form.sites.choices = sites
    form.instruments.choices = instruments



    

