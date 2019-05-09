FROM python:3.6.8-slim-stretch

EXPOSE 80

WORKDIR /banzai_bot

COPY requirements.txt /banzai_bot

RUN pip install -r requirements.txt

COPY . /banzai_bot

RUN python setup.py install

CMD gunicorn -k gevent -b 0.0.0.0:80 --access-logfile=- --error-logfile=- app:app
