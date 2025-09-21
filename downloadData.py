import zipfile

with zipfile.ZipFile("nyct_gtfs_bdfm.zip", "r") as zip_ref:
    zip_ref.extractall("gtfs_bdfm")
print("GTFS files extracted to gtfs_bdfm/")