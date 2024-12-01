import osmnx as ox
import pandas as pd

# Specify the state or city for which you want traffic signal data
state_or_city = "San Francisco, USA"

# Extract the road network for the specified location
road_network = ox.graph_from_place(state_or_city, network_type="drive")

# Get intersections (nodes where roads meet)
intersections = ox.graph_to_gdfs(road_network, nodes=True, edges=False)

# Extract latitude and longitude of intersections
traffic_signal_data = intersections[['x', 'y']].rename(columns={'x': 'Latitude', 'y': 'Longitude'})

edges = ox.graph_to_gdfs(road_network, nodes=False, edges=True)

# Extract latitude and longitude of road segments
road_coordinates = []
for _, row in edges.iterrows():
    if 'geometry' in row:
        coords = list(row['geometry'].coords)
        for coord in coords:
            road_coordinates.append((coord[1], coord[0]))  # (latitude, longitude)

road_coordinates_df = pd.DataFrame(road_coordinates, columns=['Latitude', 'Longitude'])
intersection_coordinates_df = traffic_signal_data[['Latitude', 'Longitude']].reset_index(drop=True)

# Save the data to a CSV file
intersection_coordinates_df.to_csv("intersection_coordinates.csv", index=False)
print("Data saved to intersection_coordinates.csv")

road_coordinates_df.to_csv("road_coordinates.csv", index=False)
print("Data saved to road_coordinates.csv")
