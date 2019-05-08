from flask_wtf import FlaskForm
from wtforms import DateField, Selectfield 



class ReprocessDayObsForm(FlaskForm):
    dayobs = DateField('Day-Obs', validators=[DataRequired()])

