import pymongo as pm
from pprint import pprint
import datetime
from scipy import stats
import numpy as np
import time
import matplotlib.pyplot as plt

class MyMongoDB:
	def __init__(self):
		client = pm.MongoClient('bigdatadb.polito.it',
			ssl=True,
			authSource = 'carsharing',
			tlsAllowInvalidCertificates=True
			)
		self.db = client['carsharing']
		self.db.authenticate('ictts', 'Ictts16!')

		self.per_bk = self.db['PermanentBookings']
		self.per_pk = self.db['PermanentParkings']
		self.act_bk = self.db['ActiveBookings']
		self.act_pk = self.db['ActiveParkings']

		self.obj = {}

	def list_documents(self):
		"""
			Count how many docs are present in each collection
		"""
		self.n_per_bk = self.per_bk.find({}).count()
		self.n_per_pk = self.per_pk.find({}).count()
		self.n_act_bk = self.act_bk.find({}).count()
		self.n_act_pk = self.act_pk.find({}).count()
		print(n_per_bk, n_per_pk, n_act_bk, n_act_pk)

	def list_cities(self):
		"""
			List all cities of the Database
		"""
		self.list_cities = self.per_bk.distinct('city')
		pprint(sorted(self.list_cities))

	def sort_collection(self):
		"""
			Sort the collection to see when it started/ended
			//TODO//
				Check it is the fastest method and see if it is correct
			//TODO//
		"""
		pprint(self.per_bk.index_information())
		init_sort = self.per_pk.find({'init_time':{'$lt':1481650748}}).sort(
			'init_time',1)
		for i in init_sort:
			pprint(i)
			self.initial = i['init_time']
			break

		sort_init = self.per_pk.find({'final_time':{'$gt':1481650748}}).sort(
			'final_time',-1)
		for i in sort_init:
			pprint(i)
			self.final = i['final_time']
			break

	def analyze_cities(self, cities, start_date, end_date):
		"""
			For OUR cities, check how many cars are available, how many bookings
			have been recorded during a period and if there is an alterate transport method
		"""
		unix_start = time.mktime(start_date.timetuple())
		unix_end = time.mktime(end_date.timetuple())

		for c in cities:
			print(c)
			avb_cars = self.act_pk.count({'city':c})
			# print('%f cars in %s'%(avb_cars, c))
			bk_in_date = self.per_bk.count({'city':c, 'init_time':{'$gte':unix_start,'$lt':unix_end}})
			# print('%f cars from %s to %s in %s'%(bk_in_date, start_date, end_date, c))
			alt_trans = self.per_bk.count({'city':c,'public_transport.duration':{'$ne':-1}})
			# print('%f bookings with alternative mode transportation in %s'%(alt_trans,c))
			tmp_obj = {
				'AvailableCars':avb_cars,
				'BookingsDate':bk_in_date,
				'AltTransport':alt_trans
			}
			self.obj[c] = tmp_obj
			# self.obj[c]['BookingsDate'] = bk_in_date
			# self.obj[c]['AltTransport'] = alt_trans

		pprint(self.obj)

	

if __name__ == '__main__':

	cities = ['Torino','New York City','Amsterdam']

	DB = MyMongoDB()
	# DB.list_documents()
	# DB.list_cities()

	start = datetime.date(2017,12,1)
	end = datetime.date(2017,12,31)

	DB.analyze_cities(cities, start, end)
	# data = [1, 4, 8]


