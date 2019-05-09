from flask import Flask, render_template
import requests
from banzai_tools.utils import get_sites_and_instruments
from banzai_tools.forms import ReprocessDayObsForm

app = Flask(__name__)

app.config['SECRET_KEY'] = 'asdf'

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/reprocess_dayobs')
def reprocess_dayobs():
    form = ReprocessDayObsForm()
    sites, instruments = get_sites_and_instruments()

    sites_display = [(site, site) for site in sites]
    instruments_display = [(instrument, instrument) for instrument in instruments]
    form.site.choices = sites_display
    form.instrument.choices = instruments_display
    return render_template('reprocess_dayobs.html', title='Reprocess Dayobs', form=form)

    #after submission, we want to call a script that will execute on chanunpa
