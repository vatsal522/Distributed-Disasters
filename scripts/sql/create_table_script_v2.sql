USE autonomous_vehicle_system;

CREATE TABLE vehicles (
    vehicle_id SERIAL PRIMARY KEY,
    model TEXT NOT NULL,
    manufacturer TEXT NOT NULL,
    autonomy_level INT CHECK (autonomy_level BETWEEN 0 AND 5),
    battery_level DECIMAL(5, 2) CHECK (battery_level >= 0 AND battery_level <= 100),
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE control_commands (
    command_id VARCHAR PRIMARY KEY,
    vehicle_id INT NOT NULL REFERENCES vehicles(vehicle_id),
    timestamp TIMESTAMPTZ DEFAULT now(),
    command_type TEXT NOT NULL,
    details TEXT
);

CREATE TABLE collision_warnings (
    warning_id VARCHAR PRIMARY KEY,
    vehicle_id INT NOT NULL REFERENCES vehicles(vehicle_id),
    timestamp TIMESTAMPTZ DEFAULT now(),
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    severity TEXT NOT NULL
);

CREATE TABLE routes (
    route_id VARCHAR PRIMARY KEY,
    vehicle_id INT NOT NULL REFERENCES vehicles(vehicle_id),
    origin_latitude DECIMAL(9, 6) NOT NULL,
    origin_longitude DECIMAL(9, 6) NOT NULL,
    destination_latitude DECIMAL(9, 6) NOT NULL,
    destination_longitude DECIMAL(9, 6) NOT NULL,
    route_points JSONB NOT NULL -- Storing route points as JSON for flexibility
);


CREATE TABLE vehicle_status (
    vehicle_id INT NOT NULL REFERENCES vehicles(vehicle_id),
    timestamp TIMESTAMPTZ NOT NULL,
    speed DECIMAL(5, 2) NOT NULL CHECK (speed >= 0),
    direction TEXT NOT NULL,
    proximity_alert TEXT,
    road_condition TEXT,
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    PRIMARY KEY (vehicle_id, timestamp)
);


CREATE TABLE road_sensors (
    sensor_id SERIAL PRIMARY KEY,
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    sensor_type TEXT NOT NULL,

    status TEXT NOT NULL,
    last_updated TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE traffic_signals (
    signal_id SERIAL PRIMARY KEY,
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    status TEXT NOT NULL,
    last_updated TIMESTAMPTZ DEFAULT now()
);


