CREATE KEYSPACE autonomous_vehicle_system 
  WITH REPLICATION = { 
   'class' : 'NetworkTopologyStrategy', 
   'datacenter1' : 2
};


USE autonomous_vehicle_system;

CREATE TABLE control_commands (
    command_id TEXT PRIMARY KEY,
    vehicle_id INT,
    timestamp TIMESTAMP,
    command_type TEXT,
    details TEXT
);