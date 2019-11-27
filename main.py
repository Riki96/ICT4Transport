from MongoDBClass import MyMongoDB
import datetime
from pytz import timezone

cities = ['Torino','New York City','Amsterdam']

DB = MyMongoDB()
# print(DB.db.list_collection_names())
# DB.list_documents()
# DB.list_cities()
DB.sort_collection()

start = datetime.datetime(2017,12,1)
end = datetime.datetime(2017,12,31,23,59,59)

start_ny = datetime.datetime(2017,12,1, tzinfo=timezone('US/Eastern'))
end_ny = datetime.datetime(2017,12,31,23,59,59, tzinfo=timezone('US/Eastern'))

DB.analyze_cities(cities, start, end, start_ny, end_ny)
# exit()
# DB.statistics(cities, start, end)
# DB.CDF(start,end,cities)
# DB.CDF_weekly(start, end, cities)
# DB.density_grid(start, end)
# DB.OD_matrix(start, end)
# DB.filtering(start, end)

