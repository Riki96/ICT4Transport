import pymongo as pm
from pprint import pprint
import datetime
from scipy import stats
import numpy as np
import time
import matplotlib.pyplot as plt
import statistics
import pandas 

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
			axs[1].set_xscale('log')
		plt.show()

	def CDF_weekly(self, start, end, cities):
		"""	
			Calculate the CDF over the duratio of parking and booking aggregating for
			day of the week (or by week)
		"""
		unix_start = time.mktime(start.timetuple())
		unix_end = time.mktime(end.timetuple())
		fig, axs = plt.subplots(3)
		# plt.figure()
		cnt = 0
		for c in cities:
			duration_parking = (self.per_pk.aggregate([
				{
					'$match':{
						'city':c,
						'init_time':{'$gte':unix_start,'$lte':unix_end}
						}
					},
				{
					'$project':{
						'duration':{
							'$subtract':['$final_time','$init_time'],
						},
						"weekOfMonth": {'$floor': 
							{
								'$divide': [{'$dayOfMonth': "$init_date"}, 7]}
							}
					},
				},
				{
					'$group':{	
						'_id':'$weekOfMonth',
						'd':{'$push':'$duration'}
					}
				},
				{
					'$limit':100
				}
				]))

			# print(list(duration_parking))
			# exit()
			for i in duration_parking:
				p = 1. * np.arange(len(i['d'])) / (len(i['d'])-1)
				axs[cnt].plot(np.sort(i['d']),p,label=i['_id'])
			axs[cnt].grid()
			axs[cnt].set_xscale('log')

			cnt += 1
		# fig.grid()
		# fig.legend()
		# fig.xscale('log')
		axs[2].legend()
		plt.show()

	def statistics(self, cities, start, end, bk=True):
		unix_start = time.mktime(start.timetuple())
		unix_end = time.mktime(end.timetuple())
		fig, (ax1) = plt.subplots(4, sharey=True)
		# print(len(ax1),len(ax2))
		# exit()
		for c in cities:
			print(c)
			stats_parking = self.per_bk.aggregate([
				{
					'$match':{
						'city':c,
						'init_time':{
							'$gte':unix_start,
							'$lte':unix_end
						}
					}
				},
				{
					'$project':{
						'init_date':1,
						'duration':{
							'$subtract':['$final_time','$init_time']
						},
						'day':{'$dayOfMonth':'$init_date'}
					}
				},
				{
					'$match':{
						'duration':{
							'$lte':180*60,
							'$gte':3*60
						}
					}
				},
				{
					'$group':{
						'_id':'$day',
						'arr':{'$push':'$duration'},
						'avg':{'$avg':'$duration'},
						'std':{'$stdDevSamp':'$duration'},
						'total':{'$sum':1},
					}
				},
				{
					'$sort':{
						'_id':1
					}
				}
			])
			# print(len(list(stats_parking)))
			stats = []
			# plt.figure()
			for i in stats_parking:
				med = statistics.median(i['arr'])
				perc_25 = np.percentile(np.array(i['arr']),25)
				stats.append((i['avg'],i['std'],med,perc_25))

			ax1[0].plot([x[0] for x in stats], label=c)
			ax1[0].set_title('Average')
			ax1[1].plot([x[1] for x in stats], label=c)
			ax1[1].set_title('StandardDeviation')
			ax1[2].plot([x[2] for x in stats], label=c)
			ax1[2].set_title('Median')
			ax1[3].plot([x[3] for x in stats], label=c)
			ax1[3].set_title('Percentile')
		
			# stats_parking = self.per_pk.aggregate([
			# 	{
			# 		'$match':{
			# 			'city':c,
			# 			'init_time':{
			# 				'$gte':unix_start,
			# 				'$lte':unix_end
			# 			}
			# 		}
			# 	},
			# 	{
			# 		'$project':{
			# 			'init_date':1,
			# 			'duration':{
			# 				'$subtract':['$final_time','$init_time']
			# 			},
			# 			'day':{'$dayOfMonth':'$init_date'}
			# 		}
			# 	},
			# 	{
			# 		'$group':{
			# 			'_id':'$day',
			# 			'arr':{'$push':'$duration'},
			# 			'avg':{'$avg':'$duration'},
			# 			'std':{'$stdDevSamp':'$duration'},
			# 			'total':{'$sum':1},
			# 			# 'field':{'$sum':{'$cond':[{'$lt':['$field',5000]},1,0]}}
			# 		}
			# 	},
			# 	{
			# 		'$sort':{
			# 			'_id':1
			# 		}
			# 	}
			# ])
			# # print(len(list(stats_parking)))
			# stats = []
			# # plt.figure()
			# for i in stats_parking:
			# 	# print(float(sum(i['arr']) / len(i['arr'])))
			# 	med = statistics.median(i['arr'])
			# 	perc_25 = np.percentile(np.array(i['arr']),25)
			# 	stats.append((i['avg'],i['std'],med,perc_25))

			# ax2[0].plot([x[0] for x in stats], label=c)
			# ax2[1].plot([x[1] for x in stats], label=c)
			# ax2[2].plot([x[2] for x in stats], label=c)
			# ax2[3].plot([x[3] for x in stats], label=c)

		# fig.grid()
		for i in range(4):
			ax1[i].grid()
			# ax2[i].grid()

		# plt.legend()
		# plt.grid()
		plt.show()

	def filtering(self, start, end):
		unix_start = time.mktime(start.timetuple())
		unix_end = time.mktime(end.timetuple())
		data = self.per_bk.aggregate([
			{'$match':{
				'init_time':{'$gte':unix_start,'$lte':unix_end}}
			},
			{'$project': {
				'_id':0,
				'city':1,
				'moved': { '$ne': [ # moved => origin!=destination
					{'$arrayElemAt': [ "$origin_destination.coordinates", 0]},
					{'$arrayElemAt': [ "$origin_destination.coordinates", 1]}]},
				'booking': { '$divide': [ { '$subtract': ["$final_time", "$init_time"] }, 60 ] }, #booking time 
				}
			},
			{'$match':{
				'moved':True,  		# ONLY CARS THAT HAVE BEEN MOVED
				'$and':[ 			# ONLY BOOKINGS IN BETWEEN 2min and 3h
					{'booking':{ '$lte': 180}},
					{'booking':{ '$gte': 2}}]
				}
			},
			# {'$group':{
			# 	'_id':'$city',
			# # 	'tot_rentals':{'$sum':1},
			# 	# 'avg_book_time':{'$avg':'$booking'}
			# 	}
			# },
			{'$project': {
				'city':1,
				'booking':1,
				# 'eff_rental': {'$subtract': ['$booking', '$rental']}
				}
			}
			],allowDiskUse=True)
		
		# pprint(list(data))
		return data

	def density(self, start, end, city):
		"""
			TODO: 	Plot in minutes
					Divide in grid
		"""
		# import csv

		unix_start = time.mktime(start.timetuple())
		unix_end = time.mktime(end.timetuple())		
		parked_cars = self.per_pk.aggregate([
			{
				'$match':{
					'city': city,
					'init_time':{
							'$gte':unix_start,
							'$lte':unix_end
						}
				}
			},
			# {
			# 	'$limit':10
			# },
			{
				'$project':{
					'init_date':1,
					'loc.coordinates':1,
					'duration':{
						'$subtract':['$final_time','$init_time']
					}
				}
			},
			{
				'$match':{
					'duration':{
						'$gte':3*60,
						'$lte':180*60
					}
				}
			},
			{
				'$group':{
					# '_id':{'c':'$loc.coordinates','h':{'$hour':'$init_date'}},
					# 'time':{'$push':'$init_date'},
					# 'timeDay':{'$hour':'$init_date'}
					'_id':{'$hour':'$init_date'},
					'coords':{'$push':'$loc.coordinates'},
					# 'p':{'$push':'$plate'}
				}
			},
			])

		# print((list(parked_cars)))
		# exit()
		table = []
		# tz = pytz.timezone('Italy/Rome')
		for i in parked_cars:
			for j in i['coords']:
				# print(j)
				# exit()
				# dt = j + datetime.timedelta(hours=1)
				# table.append([i['_id'][1],i['_id'][0],j])
				table.append([str(i['_id'])+':00',j[1],j[0]])
		
		table = pandas.DataFrame(table, columns=['Hour','Latitude','Longitude'])
		table.to_csv('coordinates.csv')
		# print(len(table))

			
	def density_grid(self, start, end, lat_min=45.01089, long_min=7.60679):

		unix_start = time.mktime(start.timetuple())
		unix_end = time.mktime(end.timetuple())
		z = np.linspace(0,0.05,5)
		# print(z)
		# exit()
		# print(lat_min)
		# exit()
		table = {
		}
		# for i in range(len(z)):
		for i in z:
			# print(i)
			# for j in range(len(z)):
			for j in z:
				parked_at = self.per_pk.aggregate([
					{
						'$geoNear':{
							'near':{
								'type':'Point',
								'coordinates':[(long_min+i), (lat_min+j)]
							},
							'spherical':True,
							'key':'loc',
							'distanceField':'dist.calculated',
							'maxDistance':500
						}
					},
					{
						'$match':
						{
							'city':'Torino',
							'init_time':{
								'$gte':unix_start,
								'$lte':unix_end
							}
						}
					},
					{
						'$group':{
							# '_id':'$plate',
							# 'coordinates':{'$push':'$loc.coordinates'}
							'_id':'$loc.coordinates'
						}
					}
				])
				# print(list(parked_at))
				# print('\n----\n')
				key = str(np.round(lat_min+j, decimals=5)) + ' - ' + str(np.round(long_min + i,decimals=5))
				# print([*table])
				# if key in [*table]:
					
				# else:
					# table[key] = []
				n = (len(list(parked_at)))
				# print(n)
				table[key] = n

		# print(table)
		# t = pandas.DataFrame(table)
		# table.to_csv('near_at.csv')
		# print(t)
		pprint(table)

if __name__ == '__main__':

	cities = ['Torino','New York City','Amsterdam']

	DB = MyMongoDB()
	# DB.list_documents()
	# DB.list_cities()
	# DB.sort_collection()

	start = datetime.datetime(2017,10,1)
	end = datetime.datetime(2017,10,31,23,59,59)
	# DB.clean_dataset()
	# DB.analyze_cities(cities, start, end)
	# DB.CDF(datetime.date(2017,10,1),datetime.date(2017,10,31),cities)
	# DB.CDF_weekly(start, end, cities)
	DB.density_grid(start, end)

