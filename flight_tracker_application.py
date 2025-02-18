import os
import math
import json
import re
from datetime import datetime

import requests
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, dash_table
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, udf, size
from pyspark.sql.types import (StructType, StructField, StringType,
                                 TimestampType, IntegerType, FloatType,
                                 ArrayType, BooleanType)
from FlightRadar24 import FlightRadar24API


# Initialize FlightRadar24 API
flight_api = FlightRadar24API()

# Initialize Spark Session
spark = SparkSession.builder.appName("flight_tracker").getOrCreate()

# Define folder path for data files
FOLDER_PATH = os.path.join("./", "dat_files")

# Define column names for the airport data
AIRPORT_COLUMN_NAMES = [
    "Airport ID", "Name of airport", "City", "Country", "IATA", "ICAO",
    "Latitude", "Longitude", "Altitude", "Timezone", "DST",
    "Tz database timezone", "Type", "Source"
]

# Load airport data from CSV
AIRPORT_DF = pd.read_csv(
    os.path.join(FOLDER_PATH, "airports.dat"),
    delimiter=",",
    names=AIRPORT_COLUMN_NAMES
)

# Drop unnecessary columns
AIRPORT_DF = AIRPORT_DF.drop(columns=["Type", "Source"])

# Define a list of common airport codes
AIRPORT_CODES = [
    "ATL", "DFW", "DEN", "ORD", "DXB", "LAX", "IST", "LHR", "DEL", "CDG",
    "JFK", "LAS", "AMS", "MIA", "MAD", "HND", "MCO", "FRA", "CLT", "MEX",
    "SEA", "PHX", "EWR", "SFO", "BCN", "IAH", "CGK", "BOM", "YYZ", "BOS",
    "DOH", "BOG", "GRU", "SGN", "LGW", "SIN", "FLL", "JED", "MUC", "AYT",
    "SAW", "MSP", "CUN", "MNL", "CJU", "FCO", "ORY", "SYD", "LGA", "BKK"
]

# Define a dictionary mapping airport names to IATA codes
AIRPORT_DICT = {
    "Hartsfield-Jackson Atlanta International": "ATL",
    "Dallas/Fort Worth International": "DFW",
    "Denver International": "DEN",
    "O'Hare International": "ORD",
    "Dubai International": "DXB",
    "Los Angeles International": "LAX",
    "Istanbul": "IST",
    "London Heathrow": "LHR",
    "Indira Gandhi International": "DEL",
    "Charles de Gaulle": "CDG",
    "John F. Kennedy International": "JFK",
    "McCarran International": "LAS",
    "Amsterdam Schiphol": "AMS",
    "Miami International": "MIA",
    "Adolfo Suárez Madrid–Barajas": "MAD",
    "Haneda": "HND",
    "Orlando International": "MCO",
    "Frankfurt": "FRA",
    "Charlotte Douglas International": "CLT",
    "Benito Juárez International": "MEX",
    "Seattle-Tacoma International": "SEA",
    "Phoenix Sky Harbor International": "PHX",
    "Newark Liberty International": "EWR",
    "San Francisco International": "SFO",
    "Barcelona–El Prat": "BCN",
    "George Bush Intercontinental": "IAH",
    "Soekarno–Hatta International": "CGK",
    "Chhatrapati Shivaji Maharaj International": "BOM",
    "Toronto Pearson International": "YYZ",
    "Logan International": "BOS",
    "Hamad International": "DOH",
    "El Dorado International": "BOG",
    "São Paulo/Guarulhos–Governador André Franco Montoro International": "GRU",
    "Tan Son Nhat International": "SGN",
    "Gatwick": "LGW",
    "Singapore Changi": "SIN",
    "Fort Lauderdale–Hollywood International": "FLL",
    "King Abdulaziz International": "JED",
    "Munich": "MUC",
    "Antalya": "AYT",
    "Sabiha Gökçen International": "SAW",
    "Minneapolis–Saint Paul International": "MSP",
    "Cancún International": "CUN",
    "Ninoy Aquino International": "MNL",
    "Jeju International": "CJU",
    "Leonardo da Vinci–Fiumicino": "FCO",
    "Paris Orly": "ORY",
    "Sydney Kingsford Smith": "SYD",
    "LaGuardia": "LGA",
    "Suvarnabhumi": "BKK"
}

# Create a list of dictionaries for airport dropdown options
AIRPORT_OPTIONS = [
    {"label": key, "value": value} for key, value in AIRPORT_DICT.items()
]


# Utility Functions
def get_col_name(column_name: str) -> str:
    """Replace dots in column names with underscores.

    Args:
        column_name (str): The original column name.

    Returns:
        str: The modified column name.
    """
    return column_name.replace(".", "_")


def cleanup_df(df, column: str):
    """Remove rows with NaN values in the specified column.

    Args:
        df: The DataFrame to clean.
        column (str): The column to check for NaN values.

    Returns:
        The cleaned DataFrame.
    """
    previous_count = df.count()
    df = df.filter(df[column] != "NaN")
    df = df.na.drop(subset=[column])
    next_count = df.count()
    print("Before - {}, after - {}, difference - {}, %loss - {}".format(
        previous_count, next_count, previous_count - next_count,
        (previous_count - next_count) * 100.0 / previous_count
    ))
    return df


def check_valid_code(airport_iata: str) -> bool:
    """Check if the given IATA code is a valid airport code.

    Args:
        airport_iata (str): The IATA code to validate.

    Returns:
        bool: True if the code is valid, False otherwise.
    """
    selected_values = AIRPORT_DF.loc[
        AIRPORT_DF["IATA"] == airport_iata, "Latitude"].values.tolist()
    return len(selected_values) == 1


def get_flights_from_airport(airport_iata: str):
    """Retrieve flight schedules (arrivals and departures) for a given airport.

    Args:
        airport_iata (str): The IATA code of the airport.

    Returns:
        A Pandas DataFrame containing flight schedules or None if the IATA code is invalid.
    """
    if not check_valid_code(airport_iata):
        return None

    airport_details = flight_api.get_airport_details(code=airport_iata)
    keys = [
        "flight.identification.id", "flight.identification.callsign",
        "flight.airline.name", "flight.airport.origin",
        "flight.airport.destination", "flight.time.scheduled",
        "flight.time.real", "flight.time.estimated"
    ]

    arr_details = airport_details["airport"]["pluginData"]["schedule"]["arrivals"]["data"]
    arr_df = spark.createDataFrame(arr_details)
    arr_df = arr_df.withColumn("Flight_type", lit("arrivals"))

    for key in keys:
        arr_df = arr_df.withColumn(key.replace(".", "_"), col(key))

    dept_details = airport_details["airport"]["pluginData"]["schedule"]["departures"]["data"]
    dept_df = spark.createDataFrame(dept_details)
    dept_df = dept_df.withColumn("Flight_type", lit("departures"))

    for key in keys:
        dept_df = dept_df.withColumn(key.replace(".", "_"), col(key))

    df = dept_df.union(arr_df)
    df = df.toPandas()
    return df


def get_points_along_flight_path(start_coord, end_coord, num_points, start_time, end_time):
    """Generate intermediate points along a great-circle path between two coordinates.

    Args:
        start_coord (tuple): (latitude, longitude) of the starting point.
        end_coord (tuple): (latitude, longitude) of the ending point.
        num_points (int): Number of intermediate points to generate.
        start_time (int): Unix timestamp of the start time.
        end_time (int): Unix timestamp of the end time.

    Returns:
        list: A list of dictionaries, each containing 'lat', 'lon', and 'time' for a point.
    """
    start_lat, start_lon = math.radians(start_coord[0]), math.radians(start_coord[1])
    end_lat, end_lon = math.radians(end_coord[0]), math.radians(end_coord[1])

    points = []
    for i in range(num_points + 1):
        fraction = i / num_points
        intermediate_lat = start_lat + fraction * (end_lat - start_lat)
        intermediate_lon = start_lon + fraction * (end_lon - start_lon)
        intermediate_time = start_time + fraction * (end_time - start_time)
        intermediate_point = {
            "lat": round(math.degrees(intermediate_lat), 5),
            "lon": round(math.degrees(intermediate_lon), 5),
            "time": int(intermediate_time)
        }
        points.append(intermediate_point)

    return points


def get_flight_details(flight_id: str) -> dict:
    """Retrieve detailed information about a specific flight.

    Args:
        flight_id (str): The ID of the flight.

    Returns:
        dict: A dictionary containing flight details.
    """
    details = flight_api.get_flight_details(flight_id)

    data = {}
    data["id"] = details["identification"]["id"]
    data["callsign"] = details["identification"]["callsign"]

    data["aircraft_modelname"] = details["aircraft"]["model"]["text"]
    data["aircraft_modelcode"] = details["aircraft"]["model"]["code"]

    data["airline_name"] = details["airline"]["name"]

    data["origin_airport_name"] = details["airport"]["origin"]["name"]
    data["origin_airport_iata"] = details["airport"]["origin"]["code"]["iata"]
    data["origin_airport_lat"] = details["airport"]["origin"]["position"]["latitude"]
    data["origin_airport_lon"] = details["airport"]["origin"]["position"]["longitude"]
    data["origin_airport_alt"] = details["airport"]["origin"]["position"]["altitude"]
    data["origin_airport_country"] = details["airport"]["origin"]["position"]["country"]["name"]

    data["destination_airport_name"] = details["airport"]["destination"]["name"]
    data["destination_airport_iata"] = details["airport"]["destination"]["code"]["iata"]
    data["destination_airport_lat"] = details["airport"]["destination"]["position"]["latitude"]
    data["destination_airport_lon"] = details["airport"]["destination"]["position"]["longitude"]
    data["destination_airport_alt"] = details["airport"]["destination"]["position"]["altitude"]
    data["destination_airport_country"] = details["airport"]["destination"]["position"]["country"]["name"]

    data["scheduled_departure_time"] = details["time"]["scheduled"]["departure"]
    data["scheduled_arrival_time"] = details["time"]["scheduled"]["arrival"]
    data["real_departure_time"] = details["time"]["real"]["departure"]
    data["real_arrival_time"] = details["time"]["real"]["arrival"]
    data["estimated_departure_time"] = details["time"]["estimated"]["departure"]
    data["estimated_arrival_time"] = details["time"]["estimated"]["arrival"]

    data["historical_flighttime"] = details["time"]["historical"]["flighttime"]
    data["historical_delay"] = details["time"]["historical"]["delay"]

    curr_flight_schema = StructType([
        StructField("lat", FloatType(), True),
        StructField("lng", FloatType(), True),
        StructField("alt", IntegerType(), True),
        StructField("spd", IntegerType(), True),
        StructField("ts", IntegerType(), True),
        StructField("hd", IntegerType(), True)
    ])

    df = spark.createDataFrame(details["trail"], schema=curr_flight_schema)
    df = df.withColumnRenamed("ts", "time")
    sampling_interval = 10

    df = df.rdd.zipWithIndex().filter(
        lambda x: x[1] % sampling_interval == 0).map(lambda x: x[0]).toDF()
    # df.show(truncate=False)
    data["trail"] = df.toJSON().collect()

    latest_data = details["trail"][0]
    data["future_path"] = get_points_along_flight_path(
        (latest_data["lat"], latest_data["lng"]),
        (data["destination_airport_lat"], data["destination_airport_lon"]),
        10,
        latest_data["ts"], data["scheduled_arrival_time"]
    )

    return data


# Dash App
app = Dash(external_stylesheets=[dbc.themes.DARKLY])
load_figure_template("DARKLY")

TABS_STYLES = {'height': '44px'}
TAB_STYLE = {
    'borderBottom': '1px solid #d6d6d6',
    'padding': '6px',
    'fontWeight': 'bold',
    'backgroundColor': '#222222'
}
TAB_SELECTED_STYLE = {
    'borderTop': '1px solid #d6d6d6',
    'borderBottom': '1px solid #d6d6d6',
    'backgroundColor': '#119DFF',
    'color': 'white',
    'padding': '6px'
}

app.layout = html.Div([
    dcc.Tabs([
        dcc.Tab(
            label="Future Flight Planner",
            children=[
                html.H1(children='Flights Map', style={'textAlign': 'center'}),
                dcc.Graph(id='live-update-map', style={'height': '100vh', 'width': '100vw'}),
                dcc.Interval(id='interval-component', interval=60 * 60 * 1000, n_intervals=0)
            ],
            style=TAB_STYLE,
            selected_style=TAB_SELECTED_STYLE
        ),
        dcc.Tab(
            label="Individual Flight Tracker",
            children=[
                html.H1(children="Individual Flight Tracker", style={'textAlign': 'center'}),
                html.Br(),
                dcc.Dropdown(
                    AIRPORT_OPTIONS,
                    id='airport_dropdown',
                    className='darkly-dropdown',
                    style={'background-color': 'blue'}
                ),
                html.Br(),
                dash_table.DataTable(
                    id='flight_table',
                    columns=[],
                    data=[],
                    page_action='none',
                    style_table={'height': '300px', 'overflowY': 'auto'},
                    style_cell={
                        'background-color': '#222222',
                        'font-family': 'monospace',
                        'textAlign': 'center'
                    }
                ),
                html.Br(),
                dcc.Dropdown(id='flight_dropdown', className='darkly-dropdown'),
                dcc.Graph(id='flight_output', style={'width': '100vw'})
            ],
            style=TAB_STYLE,
            selected_style=TAB_SELECTED_STYLE
        )
    ], style=TABS_STYLES)
])


# Callbacks
@app.callback(
    Output('live-update-map', 'figure'),
    Input('interval-component', 'n_intervals')
)
def format_csv_and_plot(n):
    """Update the live map with flight paths and weather data.

    Args:
        n (int): The number of times the interval has passed (used to trigger the update).

    Returns:
        plotly.graph_objects.Figure: A Plotly figure showing flight paths and airport locations.
    """
    df = pd.read_csv('test.csv')

    # Format Columns
    df['flight_path'] = df['flight_path'].map(lambda x: eval(x))
    df['flight_path'] = df['flight_path'].map(
        lambda x: [(j['lat'], j['lon']) for j in x])
    df['weather_data'] = df['weather_data'].map(lambda x: eval(x))
    df['weather_data'] = df['weather_data'].map(
        lambda x: [j['val'] for j in x])
    df['weather_data'] = df['weather_data'].map(
        lambda x: ["red" if j >= 90 else ("orange" if j >= 75 else "white") for j in x])

    # Get Airports
    dept_airport_names = df.groupby('departure_airport')['departure_lat'].first().index.tolist()
    dept_aiport_lat = df.groupby('departure_airport')['departure_lat'].first().tolist()
    dept_aiport_lon = df.groupby('departure_airport')['departure_lon'].first().tolist()

    # Arrival Airports
    arr_airport_names = df.groupby('arrival_airport')['arrival_lat'].first().index.tolist()
    arr_aiport_lat = df.groupby('arrival_airport')['arrival_lat'].first().tolist()
    arr_aiport_lon = df.groupby('arrival_airport')['arrival_lon'].first().tolist()

    flight_details = df[['flight_path', 'weather_data', 'flight_iata', 'arrival_epoch', 'departure_epoch']]
    fig = go.Figure(go.Scattergeo())
    for index, row in flight_details.iterrows():
        path = row['flight_path']
        risk_colors = row['weather_data']
        flight_number = row['flight_iata']
        arrival_time = datetime.fromtimestamp(int(row['arrival_epoch'])).strftime("%Y-%m-%d %H:%M:%S")
        departure_time = datetime.fromtimestamp(int(row['departure_epoch'])).strftime("%Y-%m-%d %H:%M:%S")
        txt = f"Flight: {flight_number}<br>Departure Time: {departure_time}<br>Arrival Time {arrival_time}"
        fig.add_trace(go.Scattergeo(
            lat=[p[0] for p in path],
            lon=[p[1] for p in path],
            mode='markers + lines',
            showlegend=False,
            line=dict(color='white', width=0.5),
            marker=dict(color=risk_colors, size=7),
            text=txt,
            hoverinfo='text'
        ))
    fig.add_trace(go.Scattergeo(
        lat=dept_aiport_lat,
        lon=dept_aiport_lon,
        showlegend=False,
        marker=dict(color="#ba1fbf", symbol='square', size=7),
        hoverinfo="text",
        text=dept_airport_names
    ))

    fig.add_trace(go.Scattergeo(
        lat=arr_aiport_lat,
        lon=arr_aiport_lon,
        showlegend=False,
        marker=dict(color="#ba1fbf", symbol='square', size=7),
        hoverinfo="text",
        text=arr_airport_names
    ))

    fig.update_layout(
        geo_showland=True,
        geo_showsubunits=True,
        geo_landcolor="#45bf1f",
        geo_showcountries=True,
        geo_countrycolor='Black',
        geo_showocean=True,
        geo_oceancolor="DarkBlue",
        geo_projection_type="orthographic"
    )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    return fig


@app.callback(
    Output('flight_table', 'columns'),
    Output('flight_table', 'data'),
    Output('flight_dropdown', 'options'),
    Input('airport_dropdown', 'value')
)
def create_flight_schedule(value):
    """Create and update the flight schedule table based on the selected airport.

    Args:
        value (str): The IATA code of the selected airport.

    Returns:
        tuple: A tuple containing the columns and data for the flight table,
               and the options for the flight dropdown.
    """
    if not value:
        return [], [], ["No Flights Available"]
    df = get_flights_from_airport(value)
    df = df.drop('flight', axis=1)

    result_df = {
        'Flight ID': [], 'Flight Number': [], 'Airline': [],
        'Origin': [], 'Departure Time': [], 'Arrival Time': []
    }
    for index, row in df.iterrows():
        if not row['flight_identification_id']:
            continue
        if row['Flight_type'] == 'departures':
            continue
        result_df['Flight ID'].append(row['flight_identification_id'])
        result_df['Flight Number'].append(row['flight_identification_callsign'])
        result_df['Airline'].append(row['flight_airline_name'])
        orig = row['flight_airport_origin']
        origin_airport_name = re.search(r'^(.+?),', orig).group()[6:-1]
        result_df['Origin'].append(origin_airport_name)
        times = row['flight_time_scheduled']
        arrival_time = re.search(r'\d+', re.search(r'a(.+?),', times).group()).group()
        departure_time = re.search(r'\d+', re.search(r'd(.+?)}', times).group()).group()
        result_df['Arrival Time'].append(
            datetime.fromtimestamp(int(arrival_time)).strftime("%Y-%m-%d %H:%M:%S"))
        result_df['Departure Time'].append(
            datetime.fromtimestamp(int(departure_time)).strftime("%Y-%m-%d %H:%M:%S"))

    result_df = pd.DataFrame(result_df)
    columns = [{'name': col, 'id': col} for col in result_df.columns]
    data = result_df.to_dict('records')
    flights = []
    flight_map = result_df[['Flight ID', 'Flight Number']]
    flight_mapper = {}
    for index, row in flight_map.iterrows():
        flight_mapper[row['Flight Number']] = row['Flight ID']
    flights = [{'label': key, 'value': value} for key, value in flight_mapper.items()]
    return columns, data, flights


@app.callback(
    Output('flight_output', 'figure'),
    Input('flight_dropdown', 'value')
)
def plot_graph(value):
    """Plot the flight path of a selected flight.

    Args:
        value (str): The ID of the selected flight.

    Returns:
        plotly.graph_objects.Figure: A Plotly figure showing the flight path,
               including previous and future paths, and airport locations.
    """
    if not value:
        return go.Figure(go.Scattergeo())
    flight = get_flight_details(value)
    start_pt = (flight['origin_airport_lat'], flight['origin_airport_lon'])
    end_pt = (flight['destination_airport_lat'], flight['destination_airport_lon'])
    trail = flight['trail']
    prev_path = [(eval(l)['lat'], eval(l)['lng']) for l in trail]
    cur_location = prev_path[0]
    f_path = flight['future_path']
    future_path = [(fut['lat'], fut['lon']) for fut in f_path]
    fig = go.Figure(go.Scattergeo(
        lat=[p[0] for p in prev_path],
        lon=[p[1] for p in prev_path],
        mode='markers+lines',
        line=dict(color='white', width=0.8),
        marker=dict(color='white', size=7),
        showlegend=False,
        hoverinfo="text",
        text="Previous Path"
    ))

    fig.add_trace(go.Scattergeo(
        lat=[f[0] for f in future_path],
        lon=[f[1] for f in future_path],
        mode='markers+lines',
        line=dict(color='LightBlue', width=0.8),
        marker=dict(color='LightBlue', size=7),
        showlegend=False,
        hoverinfo="text",
        text="Future Path"
    ))

    fig.add_trace(go.Scattergeo(
        lat=[start_pt[0]],
        lon=[start_pt[1]],
        mode='markers',
        marker=dict(color='pink', symbol='square', size=10),
        showlegend=False,
        hoverinfo="text",
        hovertext=flight['origin_airport_name']
    ))

    fig.add_trace(go.Scattergeo(
        lat=[end_pt[0]],
        lon=[end_pt[1]],
        mode='markers',
        marker=dict(color='pink', symbol='square', size=10),
        showlegend=False,
        hoverinfo="text",
        text=flight['destination_airport_name']
    ))

    fig.add_trace(go.Scattergeo(
        lat=[cur_location[0]],
        lon=[cur_location[1]],
        mode='markers',
        marker=dict(color='red', size=10),
        showlegend=False,
        hoverinfo='text',
        text="Current Location"
    ))

    fig.update_layout(
        autosize=True,
        geo_center=dict(lat=cur_location[0], lon=cur_location[1]),
        geo_projection_scale=25,
        geo_showland=True,
        geo_showsubunits=True,
        geo_subunitcolor='Black',
        geo_landcolor="#45bf1f",
        geo_showcountries=True,
        geo_countrycolor='Black',
        geo_showocean=True,
        geo_oceancolor="DarkBlue"
    )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

    return fig


if __name__ == "__main__":
    # download_files()
    app.run_server(host='0.0.0.0', port=8050, debug=False)
    # get_flights_from_airport('SIN')
