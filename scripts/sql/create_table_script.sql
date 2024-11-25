-- Create the database
-- CREATE DATABASE autonomous_vehicle_system;

-- Use the database
USE autonomous_vehicle_system;

CREATE TABLE vehicles (
    vehicle_id SERIAL PRIMARY KEY,
    model TEXT NOT NULL,
    manufacturer TEXT NOT NULL,
    autonomy_level INT CHECK (autonomy_level BETWEEN 0 AND 5),
    battery_level DECIMAL(5, 2) CHECK (battery_level >= 0 AND battery_level <= 100),
    current_location TEXT NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE control_commands (
    command_id SERIAL PRIMARY KEY,
    vehicle_id INT NOT NULL REFERENCES vehicles(vehicle_id),
    timestamp TIMESTAMPTZ DEFAULT now(),
    command_type TEXT NOT NULL,
    details TEXT
);

CREATE TABLE collision_warnings (
    warning_id SERIAL PRIMARY KEY,
    vehicle_id INT NOT NULL REFERENCES vehicles(vehicle_id),
    timestamp TIMESTAMPTZ DEFAULT now(),
    location TEXT NOT NULL,
    severity TEXT NOT NULL
);

CREATE TABLE routes (
    route_id SERIAL PRIMARY KEY,
    vehicle_id INT NOT NULL REFERENCES vehicles(vehicle_id),
    origin TEXT NOT NULL,
    destination TEXT NOT NULL,
    route_points JSONB NOT NULL -- Storing route points as JSON for flexibility
);



CREATE TABLE vehicle_status (
    vehicle_id INT NOT NULL REFERENCES vehicles(vehicle_id),
    timestamp TIMESTAMPTZ NOT NULL,
    speed DECIMAL(5, 2) NOT NULL CHECK (speed >= 0),
    direction TEXT NOT NULL,
    proximity_alert BOOLEAN DEFAULT FALSE,
    road_condition TEXT,
    next_destination TEXT,
    PRIMARY KEY (vehicle_id, timestamp)
);


CREATE TABLE road_sensors (
    sensor_id SERIAL PRIMARY KEY,
    location TEXT NOT NULL,
    sensor_type TEXT NOT NULL,
    status TEXT NOT NULL,
    last_updated TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE traffic_signals (
    signal_id SERIAL PRIMARY KEY,
    location TEXT NOT NULL,
    status TEXT NOT NULL,
    last_updated TIMESTAMPTZ DEFAULT now()
);


