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

		self.per_bk_enj = self.db['enjoy_PermanentBookings']
		self.per_pk_enj = self.db['enjoy_PermanentParkings']
		self.act_bk_enj = self.db['enjoy_ActiveBookings']
		self.act_pk_enj = self.db['enjoy_ActiveParkings']

		self.container = {}

	def list_documents(self):
		"""
			Count how many docs are present in each collection
		"""
		self.n_per_bk = self.per_bk.find({}).count()
		self.n_per_pk = self.per_pk.find({}).count()
		self.n_act_bk = self.act_bk.find({}).count()
		self.n_act_pk = self.act_pk.find({}).count()

		self.n_per_bk_enj = self.per_bk_enj.find({}).count()
		self.n_per_pk_enj = self.per_pk_enj.find({}).count()
		self.n_act_bk_enj = self.act_bk_enj.find({}).count()
		self.n_act_pk_enj = self.act_pk_enj.find({}).count()

		print(self.n_per_bk, self.n_per_pk, self.n_act_bk, self.n_act_pk)
		print(self.n_per_bk_enj, self.n_per_pk_enj, self.n_act_bk_enj, self.n_act_pk_enj)

	def list_cities(self):
		"""
			List all cities of the Database
		"""
		self.list_cities = self.per_bk.distinct('city')
		self.list_cities_enj = self.per_pk_enj.distinct('city')
		pprint(sorted(self.list_cities))
		pprint(sorted(self.list_cities_enj))

		for l in self.list_cities:
			print('City:{} - Number:{}'.format(l,self.per_bk.count({'city':l})))

		for l in self.list_cities_enj:
			print('City:{} \t Number:{}'.format(l,self.per_bk_enj.count({'city':l})))

	def sort_collection(self):
		"""
			Sort the collection to see when it started/ended
			//TODO//
				ASK PROFESSOR
			//TODO//
		"""
		# pprint(self.per_bk.index_information())
		init_sort = self.per_pk.find({'init_time':{'$lt':1481650748}}).sort(
			'init_time',1)
		
		print(list(init_sort)[0]['init_date'])

		sort_init = self.per_pk.find({'init_time':{'$gt':1500000000}}).sort(
			'init_time',-1)
		print(list(sort_init)[0]['final_date'])


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
			if c == 'Torino':
				avb_cars_enj = self.act_pk_enj.count({'city':c})
			else:
				avb_cars_enj = ''
			print('%d cars in %s'%(avb_cars, c))
			bk_in_date = self.per_bk.count({
				'city':c,
				'init_time':{'$gte':unix_start,'$lte':unix_end}})
			# print('%f cars from %s to %s in %s'%(bk_in_date, start_date, end_date, c))
			pub_trans = self.per_bk.count({
				'city':c,
				'public_transport.duration':{'$ne':-1}})
			drv_trans = self.per_bk.count({
				'city':c,
				'driving.duration':{'$ne':-1}})
			walk_trans = self.per_bk.count({
				'city':c,
				'walking.duration':{'$ne':-1}})
			# print('%f bookings with alternative mode transportation in %s'%(alt_trans,c))
			tmp_obj = {
				'AvailableCars':avb_cars,
				'AvailableCars_enj':avb_cars_enj,
				'BookingsDate':bk_in_date,
				'PublicTransport':pub_trans,
				'Walking':walk_trans,
				'Driving':drv_trans
			}
			self.container[c] = tmp_obj

		pprint(self.container)

	def CDF(self, start, end, cities):
		"""
			Calculate the CDF for the cities over the duration of parkings and bookings
		"""
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
				}
				]))

			duration_booking = self.per_bk.aggregate([
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
				}
				])
			lst_parking = []
			lst_booking = []

			for i in duration_parking:
				lst_parking.append(i['duration'])
				
			for i in duration_booking:
				lst_booking.append(i['duration'])

			lst_parking = np.array(lst_parking)
			lst_booking = np.array(lst_booking)

			p = 1. * np.arange(len(lst_parking)) / (len(lst_parking)-1)
			
			axs[0].plot(np.sort(lst_parking),p)
			axs[0].grid()
			axs[0].set_xscale('log')
			
			p = 1. * np.arange(len(lst_booking)) / (len(lst_booking)-1)
			axs[1].plot(np.sort(lst_booking),p)
			axs[1].grid()
			axs[0].set_xscale('log')
		plt.show()

	def CDF_weekly(self, start, end, cities):
		"""	
			Calculate the CDF over the duratio of parking and booking aggregating for
			day of the week (or by week)
		"""
		unix_start = time.mktime(start.timetuple())
		unix_end = time.mktime(end.timetuple())
		# fig, axs = plt.subplots(2)
		plt.figure()
		for d in range(7):
			print(d)
			duration_parking = (self.per_pk.aggregate([
				{
					'$match':{
						'city':'Torino',
						'init_time':{'$gte':unix_start,'$lte':unix_end}
						}
					},
				{
					'$project':{
						'duration':{
							'$subtract':['$final_time','$init_time'],
						},
						'dayOfWeek':{'$dayOfWeek':'$init_date'}
					},
				},
				{
					'$match':{
						'dayOfWeek':d
					}
				}
				]))

			lst_parking_d = []
			for i in duration_parking:
				# print(i)
				lst_parking_d.append(i['duration'])

			lst_parking_d = np.array(lst_parking_d)/60
			p = 1. * np.arange(len(lst_parking_d)) / (len(lst_parking_d)-1)
			# axs[0].plot(np.sort(lst_parking_d),p)
			# axs[0].grid()	
			plt.plot(np.sort(lst_parking_d),p,label=d)
			plt.grid()
			plt.legend()
		plt.show()
								
	def clean_dataset(self):
		aggr_by_location = self.per_pk.aggregate([{
				'$match':{'city':'Torino'}
			},
			{
				'$project':{
					'loc.coordinates':1,
					'plate':1,
					# 'final_time':1,
					# 'init_time':1,
					'duration':{
						'$subtract':['$final_time','$init_time']
					}
				}
			},
			{
				'$group':{
					'_id':'$loc.coordinates',
					'plates':{
						'$push':'$plate'
						},
					'durations':{
						'$push':'$duration'
						}
					},
				},
			{
				'$limit':200
			},
			# {
			# 	'$addFields':{
			# 	'tot':
			# 		{
			# 			'$size':'$plates'}
			# 			}
			# },
			# {
			# 	'$sort':
			# 		{
			# 			'tot':1}
			# },
			],allowDiskUse=True)

		pprint(list(aggr_by_location))
if __name__ == '__main__':

	cities = ['Torino','New York City','Amsterdam']

	DB = MyMongoDB()
	# DB.list_documents()
	# DB.list_cities()
	# DB.sort_collection()

	start = datetime.date(2017,12,1)
	end = datetime.date(2017,12,31)
	DB.clean_dataset()
	# DB.analyze_cities(cities, start, end)
	# DB.CDF(datetime.date(2017,10,1),datetime.date(2017,10,31),cities)
	# DB.CDF_weekly(datetime.date(2017,10,1),datetime.date(2017,10,31),cities)


