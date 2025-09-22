import requests
from google.transit import gtfs_realtime_pb2

# Create feed object
feed = gtfs_realtime_pb2.FeedMessage()

# Fetch the real-time feed
response = requests.get("https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm")

# Parse protobuf
feed.ParseFromString(response.content)

# Print some info
for entity in feed.entity:
    fields_dict = {f.name: getattr(entity, f.name) for f in entity.DESCRIPTOR.fields}
    print(fields_dict)
    if entity.HasField("alert"):
        alert = entity.alert
        print(alert.__class__.__name__)  # optional: show type
        print({f.name: getattr(alert, f.name) for f in alert.DESCRIPTOR.fields})