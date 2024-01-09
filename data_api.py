import nfl_data_py as nfl
from flask import Flask, json, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from flask_cors import CORS, cross_origin
import datetime
from db import get_off_epa, tempo, get_epa


app = Flask(__name__)
cors = CORS(app)
scheduler = BackgroundScheduler()

@app.route('/')
def homepage():
    get_epa()
    with open ('data.json', 'r') as file:
        data = json.load(file)
        return jsonify(data)
    

@app.route('/epa')
def general({name}):
    get_{name}()
    with open (f'{name}.json', 'r') as file:
        data = json.load(file)
        return jsonify(data)   

if __name__ == '__main__':
    #ftn based
    scheduler.add_job(tempo, 'cron', month='9-12, 1-2', day_of_week='wed, thu', hour='21-23, 0-2', minute='45', timezone='America/Sao_Paulo')

    #nflfastr based
    scheduler.add_job(tempo, 'cron', month='9-12, 1-2', day_of_week='sat, sun, tue, fri', hour='0-5, 17-23', minute='*/20', timezone='America/Sao_Paulo')
    scheduler.start()
    app.run(debug=True)