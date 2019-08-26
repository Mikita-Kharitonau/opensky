import os
import schedule
import time

from opensky_api import OpenSkyApi

CSV_ROW = "{}, " * 16 + "{}\n"
MINUTES_RESULTION = 2


def main():
    schedule.every(20).seconds.do(retrieve_data)
    while True:
        schedule.run_pending()


def retrieve_data():
    api = OpenSkyApi()
    s = api.get_states()
    with open(create_file_name(), 'a') as file:
        for row in [create_csv_row(state, s.time) for state in s.states]:
            file.write(row)


def create_file_name():
    minutes = int(time.strftime('%M'))
    minute_id = minutes - (minutes % MINUTES_RESULTION)
    data_id = time.strftime("%Y-%m-%d/%H/")
    directory_name = "opensky-data-{}".format(data_id)
    os.makedirs(os.path.dirname(directory_name), exist_ok=True)
    return directory_name + '/' + str(minute_id)


def create_csv_row(state_vector, time):
    return CSV_ROW.format(
        time,
        state_vector.icao24,
        state_vector.callsign,
        state_vector.origin_country,
        state_vector.time_position,
        state_vector.last_contact,
        state_vector.longitude,
        state_vector.latitude,
        state_vector.baro_altitude,
        state_vector.on_ground,
        state_vector.velocity,
        state_vector.heading,
        state_vector.vertical_rate,
        state_vector.sensors,
        state_vector.geo_altitude,
        state_vector.squawk,
        state_vector.spi,
        state_vector.position_source
    )


if __name__ == '__main__':
    main()