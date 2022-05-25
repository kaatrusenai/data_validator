import time
from flask import Flask, render_template, request, jsonify
from data_fetcher import DataFetcher
from folium import Map, CircleMarker
from datetime import datetime
import pandas as pd
from flask_cors import CORS
import uuid
from store import local_store
from threading import Timer, Thread

app = Flask(__name__)
CORS(app)

db = DataFetcher('mongodb://campusadmin:testdev44@3.109.93.19:27017/', 'admin')


def set_timeout(key):
    local_store[key] = None
    time.sleep(25)
    if local_store.get(key) is not None:
        return local_store.get(key)
    return jsonify(key=key), 410


def remove_key(key):
    local_store.pop(key)


def get_utc_unix(date_string, time_format="%Y-%m-%dT%H:%M"):
    try:
        date = int(date_string)
        return date
    except ValueError:
        date = datetime.strptime(date_string, time_format)
        date = date.timestamp() * 1e3
        return date


def get_data(data_for: str, data_info: dict, key: str):
    start_time = get_utc_unix(data_info.get('trip-start-time'))
    end_time = get_utc_unix(data_info.get('trip-end-time'))
    device = data_info["device"]
    param = data_info["parameter"]
    query = {"srvtime": {"$gte": start_time, "$lte": end_time}}
    if data_for == 'sen':
        df = db.sensor_query(device, query, param, key)
    else:
        df = db.imu_query(device, query, param, key)
    if df is None:
        return
    if param not in df.columns:
        local_store[key] = jsonify(message='Requested parameter is not available in the requested device'), 402
        return
    df.dropna(inplace=True)
    df['label'] = pd.cut(df[param], bins=4,
                         labels=['low', 'low_med', 'high_med', 'high'],
                         duplicates='drop')
    color_dict = {
        'low': 'lightgreen',
        'low_med': 'green',
        'high_med': 'yellow',
        'high': 'red'
    }

    data_mean = round(df[param].mean(), 3)
    data_var = round(df[param].var(), 3)
    data_std = round(df[param].std(), 3)
    data_quality = round(((1 - df[param].isna().sum() / len(df)) * 100), 3)
    location = [df["lat"].median(), df["lon"].median()]
    zoom_start = 13
    if (location[0] <= 1) and (location[1] <= 1):
        location = [22.342864680671664, 78.80034230580904]
        zoom_start = 5
    data_map = Map(
        location=location,
        zoom_start=zoom_start,
        tiles='cartodbdark_matter')

    for _, row in df.iterrows():
        CircleMarker(
            location=(row.lat, row.lon),
            color=color_dict[row.label],
            radius=1.0
        ).add_to(data_map)

    data_map.save('./templates/folium.html')
    local_store[key] = {
            'device': device,
            'param': param,
            'mean': data_mean,
            'std': data_std,
            'var': data_var,
            'quality': data_quality
        }
    return


@app.route("/")
def home():
    return render_template('index.html')


@app.route("/sensor/<key>")
def sensor(key):
    if key == 'none':
        gen_key = str(uuid.uuid4())
        Thread(target=get_data, args=['sen', request.args, gen_key]).start()
        return set_timeout(gen_key)
    if local_store.get(key) is not None:
        Timer(5, remove_key, [key]).start()
        return local_store.get(key)
    return jsonify(message='not ready yet'), 409


@app.route("/imu/<key>")
def imu(key):
    if key == 'none':
        gen_key = str(uuid.uuid4())
        Thread(target=get_data, args=['acc', request.args, gen_key]).start()
        return set_timeout(gen_key)
    if local_store.get(key) is not None:
        Timer(5, remove_key, [key]).start()
        return local_store.get(key)
    return jsonify(message='not ready yet'), 409


@app.route('/map')
def folium_map():
    return render_template('folium.html')


if __name__ == "__main__":
    app.run(debug=True)
