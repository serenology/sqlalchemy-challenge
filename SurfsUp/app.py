# Import the dependencies.

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

import pandas as pd
import datetime as dt
from datetime import datetime, timedelta

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)


# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
app = Flask(__name__)

#################################################
# Flask Setup
#################################################




#################################################
# Flask Routes
#################################################
# Define the home route
@app.route('/')
def home():
    return (
        f"Welcome to the Climate Analysis API!<br/><br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start&gt;<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;"
    )

# Define the precipitation route
@app.route('/api/v1.0/precipitation')
def precipitation():
    # Create a session
    session = Session(engine)

    # Calculate the date one year from the last date in the dataset
    latest_date = session.query(func.max(Measurement.date)).scalar()
    latest_date = datetime.strptime(latest_date, '%Y-%m-%d')
    one_year_ago = latest_date - timedelta(days=365)

    # Query precipitation data for the last 12 months
    results = session.query(Measurement.date, Measurement.prcp)\
        .filter(Measurement.date >= one_year_ago)\
        .order_by(Measurement.date).all()

    # Convert the query results to a dictionary
    precipitation_dict = {date: prcp for date, prcp in results}

    # Close the session
    session.close()

    return jsonify(precipitation_dict)

# Define the stations route
@app.route('/api/v1.0/stations')
def stations():
    # Create a session
    session = Session(engine)

    # Query all stations
    results = session.query(Station.station).all()

    # Convert the query results to a list
    station_list = [station[0] for station in results]

    # Close the session
    session.close()

    return jsonify(station_list)

# Define the tobs route
@app.route('/api/v1.0/tobs')
def tobs():
    # Create a session
    session = Session(engine)

    # Find the most active station
    most_active_station = session.query(Measurement.station)\
        .group_by(Measurement.station)\
        .order_by(func.count(Measurement.station).desc())\
        .first()

    if most_active_station:
        # Calculate the date one year from the last date in the dataset
        latest_date = session.query(func.max(Measurement.date)).scalar()
        latest_date = datetime.strptime(latest_date, '%Y-%m-%d')
        one_year_ago = latest_date - timedelta(days=365)

        # Query temperature observations for the last 12 months for the most active station
        results = session.query(Measurement.date, Measurement.tobs)\
            .filter(Measurement.station == most_active_station[0], Measurement.date >= one_year_ago)\
            .order_by(Measurement.date).all()

        # Convert the query results to a list of dictionaries
        tobs_list = [{'Date': date, 'Temperature': tobs} for date, tobs in results]

        # Close the session
        session.close()

        return jsonify(tobs_list)
    else:
        # Close the session
        session.close()
        return jsonify({'error': 'No stations found in the dataset.'}), 404

# Define the start and start-end route
@app.route('/api/v1.0/<string:start>')
@app.route('/api/v1.0/<string:start>/<string:end>')
def temperature_stats(start, end=None):
    try:
        # Attempt to convert start and end dates to datetime objects
        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d') if end else None

        # Create a session
        session = Session(engine)

        # Query temperature statistics based on start and end dates
        query = session.query(func.min(Measurement.tobs).label('min_temperature'),
                              func.avg(Measurement.tobs).label('avg_temperature'),
                              func.max(Measurement.tobs).label('max_temperature'))\
            .filter(Measurement.date >= start_date)

        # Apply the filter only if end_date is not None
        if end_date is not None:
            query = query.filter(Measurement.date <= end_date)

        results = query.all()

        # Close the session
        session.close()

        # Convert the query results to a dictionary
        temperature_stats_dict = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d') if end_date else None,
            'min_temperature': results[0][0],
            'avg_temperature': results[0][1],
            'max_temperature': results[0][2]
        }

        return jsonify(temperature_stats_dict)

    except ValueError:
        return jsonify({'error': 'Invalid date format. Please use the format YYYY-MM-DD.'}), 400

# Run the app
if __name__ == '__main__':
    app.run(debug=True)