from places_id_collector import collect_places_id
from reviews_and_data_collector import collect_reviews_and_data

region_ids = ["2"]
categories = ["grocery", "supermarket", "food_hypermarket"]

for region_id in region_ids:
    for category in categories:
        collect_places_id(region_id, category)

for region_id in region_ids:
    for category in categories:
        collect_reviews_and_data(region_id, category)
