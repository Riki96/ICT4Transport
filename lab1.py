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
		self.n_per_bk = self.per_bk.find({}).count()
		self.n_per_pk = self.per_pk.find({}).count()
		self.n_act_bk = self.act_bk.find({}).count()
		self.n_act_pk = self.act_pk.find({}).count()
		print(n_per_bk, n_per_pk, n_act_bk, n_act_pk)

	def list_cities(self):
		self.list_cities = self.per_bk.distinct('city')
		pprint(sorted(self.list_cities))

	def sort_collection(self):
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

	def CDF(self, start, end, cities):
		unix_start = time.mktime(start.timetuple())
		unix_end = time.mktime(end.timetuple())
		fig, axs = plt.subplots(2)
		start_time = time.time()
		for c in cities:
			print(c)
			duration_parking = (self.per_pk.aggregate([
				{
					'$match':{
						'city':c,
						'init_time':{'$gte':unix_start,'$lte':unix_end}}
					},
				{
					'$project':{
						'duration':{
							'$subtract':['$final_time','$init_time']
						}
					},
				},
				# {
				# 	'$sort':{'duration':1}
				# }
				]))

			duration_booking = self.per_bk.aggregate([
				{
					'$match':{
						'city':'Torino',
						'init_time':{'$gte':unix_start,'$lte':unix_end}}
					},
				{
					'$project':{
						'duration':{
							'$subtract':['$final_time','$init_time']
						}
					},
				},
				# {
				# 	'$sort':{'duration':1}
				# }
				])
			lst_parking = []
			lst_booking = []

			for i in duration_parking:
				lst_parking.append(i['duration'])
				
			for i in duration_booking:
				lst_booking.append(i['duration'])

			lst_parking = np.array(lst_parking)/60
			lst_booking = np.array(lst_booking)/60

			p = 1. * np.arange(len(lst_parking)) / (len(lst_parking)-1)
			
			axs[0].plot(np.sort(lst_parking),p)
			axs[0].grid()
			
			p = 1. * np.arange(len(lst_booking)) / (len(lst_booking)-1)
			axs[1].plot(np.sort(lst_booking),p)
			axs[1].grid()
		# plt.grid()
		# print(time.time() - start_time)
		plt.show()

if __name__ == '__main__':

	cities = ['Torino','New York City','Amsterdam']

	DB = MyMongoDB()
	# DB.list_documents()
	# DB.list_cities()

	start = datetime.date(2017,12,1)
	end = datetime.date(2017,12,31)

	# DB.analyze_cities(cities, start, end)
	DB.CDF(datetime.date(2017,10,1),datetime.date(2017,10,31),cities)
	# data = [1, 4, 8]


