from MongoDBClass import MyMongoDB
import datetime
from pytz import timezone

cities = ['Torino','New York City','Amsterdam']

DB = MyMongoDB()
# print(DB.db.list_collection_names())
# DB.list_documents()
DB.list_cities()
# DB.sort_collection()

start = datetime.datetime(2017,10,1)
end = datetime.datetime(2017,10,31,23,59,59)

start_ny = datetime.datetime(2017,10,1, tzinfo=timezone('US/Eastern'))
end_ny = datetime.datetime(2017,10,31,23,59,59, tzinfo=timezone('US/Eastern'))

# DB.analyze_cities(start, end, start_ny, end_ny, cities)
# exit()
#DB.statistics(start, end,start_ny, end_ny, cities)
# DB.CDF(start,end,start_ny,end_ny,cities)
# DB.CDF_weekly(start, end, start_ny,end_ny, cities)
#DB.system_utilization(start, end,start_ny,end_ny ,cities)
# DB.system_utilization_filtered(start, end,start_ny,end_ny ,cities)
# DB.density_grid(start, end)
DB.OD_matrix(start, end)
# DB.filtering(start, end)

