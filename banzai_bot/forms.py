from flask_wtf import FlaskForm
from wtforms import DateField, Selectfield 


class ReprocessDayObsForm(FlaskForm):
    dayobs_start = DateField('Day-Obs Start', validators=[DataRequired()])
    dayobs_end = DateField('Day-Obs End', validators=[DataRequired())
    site = SelectField('Site Code', validators=[DataRequired()])
    instrument = SelectField('Instrument Code', validators=[DataRequired()])


