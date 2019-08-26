import os
import schedule
import time

from opensky_api import OpenSkyApi
from utils import create_csv_row

MINUTES_RESULTION = 5


def retrieve_data():
    api = OpenSkyApi()
    s = api.get_states()
    with open(create_output_file_name(), 'a') as file:
        for row in [create_csv_row(state, s.time) for state in s.states]:
            file.write(row)


def create_output_file_name():
    minutes = int(time.strftime('%M'))
    minute_id = minutes - (minutes % MINUTES_RESULTION)
    data_id = time.strftime("%Y-%m-%d/%H/")
    directory_name = "opensky-data-{}".format(data_id)
    os.makedirs(os.path.dirname(directory_name), exist_ok=True)
    return directory_name + '/' + str(minute_id) + '.csv'


def main():
    schedule.every(20).seconds.do(retrieve_data)
    while True:
        schedule.run_pending()


if __name__ == '__main__':
    main()