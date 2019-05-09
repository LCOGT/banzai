from flask import Flask, render_template
import requests
<<<<<<< HEAD
from banzai_bot.utils import get_sites_and_instruments
from banzai_bot.forms import ReprocessDayObsForm

=======
from banzai_bot import utils
from forms import ReprocessDayObsForm
>>>>>>> d7972fc88fce91f39b18341410ebd544dd0a63f1

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/reprocess_dayobs')
def reprocess_dayobs():
<<<<<<< HEAD
    form = ReprocessDayObsForm()
    sites, instruments = get_sites_and_instruments()
    form.sites.choices = sites
    form.instruments.choices = instruments
    return render_template('reprocess_dayobs.html', title='Reprocess Dayobs', form=form)
=======
    sites, instruments = utils.get_sites_and_instruments()
    form = ReprocessDayObsForm()
    form.sites.choices = sites
    form.instruments.choices = instruments



    

>>>>>>> d7972fc88fce91f39b18341410ebd544dd0a63f1
