CSV_ROW = "{}, " * 16 + "{}\n"


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
