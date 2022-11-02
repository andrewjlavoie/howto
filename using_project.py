#import libraries and config
import pandas as pd
import seaborn as sns
import pymongo
import pprint
import matplotlib.pyplot as plt

import config

client = pymongo.MongoClient(config.mdb_uri)
collection = client['ship_analysis']['data']

#build dataframe with ALL documents in collection
shipsdf = pd.DataFrame(collection.find())

#look at first 5
shipsdf.head()

'''
	_id	ground_truth	geolocation	mil_flag
0	6232083a2021ddaba70c72f8	{'detections': {'_id': 6230b116b18de021fa4b9a4...	{'type': 'Point', 'coordinates': [45.716253541...	NaN
1	6232083a2021ddaba70c72fe	{'detections': {'_id': 6230b116b18de021fa4b9a5...	{'type': 'Point', 'coordinates': [44.258190966...	True
2	6232083a2021ddaba70c7301	{'detections': {'_id': 6230b116b18de021fa4b9a5...	{'type': 'Point', 'coordinates': [43.882614123...	NaN
3	6232083a2021ddaba70c730e	{'detections': {'_id': 6230b116b18de021fa4b9aa...	{'type': 'Point', 'coordinates': [41.644388286...	NaN
4	6232083a2021ddaba70c7317	{'detections': {'_id': 6230b116b18de021fa4b9ab...	{'type': 'Point', 'coordinates': [42.865094707...	NaN
'''

#build aggregation to filter and project what we want the dataframe to look like
filtered_documents = collection.aggregate([
    {
        '$match': { 
            'mil_flag': True,
        }
    }, {
        '$project': {
            'label': '$ground_truth.detections.label', 
            'ship_size': '$ground_truth.detections.Ship_size',
            'ship_loc': '$ground_truth.detections.Ship_location',
            'ship_area': '$ground_truth.detections.Ship_area', 
            'geoloc_coords': '$geolocation.coordinates'
        }
    }
])

shipsdf = pd.DataFrame(filtered_documents)

#look at first 5
shipsdf.head()

'''
_id	label	ship_size	ship_loc	ship_area	geoloc_coords
0	6232083a2021ddaba70c72fe	Other Warship	medium	nearland	561.0	[44.25819096624126, 35.03209280658446]
1	6232083a2021ddaba70c7367	Other Warship	big	nearland	1710.0	[45.785783672689846, 30.570193564676345]
2	6232083a2021ddaba70c7356	Submarine	big	nearland	2352.0	[42.28212537509297, 41.17917052536899]
3	6232083a2021ddaba70c7390	Submarine	big	nearland	2540.0	[44.33692996596404, 37.77588873078249]
4	6232083a2021ddaba70c7395	Submarine	big	nearland	2375.0	[45.669487572214315, 32.245409969028614]
'''