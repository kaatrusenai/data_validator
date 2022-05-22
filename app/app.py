from flask import Flask, render_template, request, jsonify
from data_fetcher import DataFetcher
from folium import Map, CircleMarker
from datetime import datetime
import pandas as pd
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

db = DataFetcher('mongodb://campusadmin:testdev44@3.109.93.19:27017/', 'admin')


def get_utc_unix(date_string, time_format="%Y-%m-%dT%H:%M"):
    try:
        date = int(date_string)
        return date
    except ValueError:
        date = datetime.strptime(date_string, time_format)
        date = date.timestamp() * 1e3
        return date


def get_data(data_for: str, data_info: dict, return_json=False):
    start_time = get_utc_unix(data_info.get('trip-start-time'))
    end_time = get_utc_unix(data_info.get('trip-end-time'))
    device = data_info["device"]
    param = data_info["parameter"]
    query = {"srvtime": {"$gte": start_time, "$lte": end_time}}
    if data_for == 'sen':
        df = db.sensor_query(device, query, param)
    else:
        df = db.imu_query(device, query, param)
    if isinstance(df, str):
        if not return_json:
            return df
        else:
            return jsonify(message='No data found for the given time range'), 402
    if param not in df.columns:
        if not return_json:
            return render_template('error.html', message='Requested parameter is not available in the requested device')
        else:
            return jsonify(message='Requested parameter is not available in the requested device'), 402
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

    data_map.save('./static/folium.html')
    data_map.save('./templates/folium.html')
    if return_json:
        return {
            'device': device,
            'param': param,
            'mean': data_mean,
            'std': data_std,
            'var': data_var,
            'quality': data_quality
        }
    return render_template("sensor_data_map.html",
                           device=device,
                           param=param,
                           data_mean=data_mean,
                           data_std=data_std,
                           data_var=data_var,
                           data_quality=data_quality)


@app.route("/sensor_home", methods=["GET", "POST"])
def sensor_home():
    if request.method == "POST":
        return get_data('sen', request.form)

    return render_template("sensor_home.html")


@app.route("/imu_home", methods=["GET", "POST"])
def imu_home():
    if request.method == "POST":
        return get_data('acc', request.form)

    return render_template("imu_home.html")


@app.route("/")
def home():
    return render_template('index.html')


@app.route("/sensor")
def sensor():
    return get_data('sen', request.args, return_json=True)


@app.route("/imu")
def imu():
    return get_data('acc', request.args, return_json=True)


@app.route('/map')
def folium_map():
    return render_template('folium.html')
