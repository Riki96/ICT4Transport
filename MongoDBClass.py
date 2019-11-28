import pymongo as pm
from pprint import pprint
import datetime
from scipy import stats
import numpy as np
import time
import matplotlib.pyplot as plt
import statistics
import pandas
import seaborn as sb
from matplotlib import dates

class MyMongoDB:
	def __init__(self):
		client = pm.MongoClient('bigdatadb.polito.it',
			ssl=True,
			authSource = 'carsharing',
			tlsAllowInvalidCertificates=True
			)
		# sb.set()
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
		for collection in self.db.list_collection_names():
			print('Collection {} has {} documents'.format(collection, self.db[collection].find({}).count()))

		# print(self.n_per_bk, self.n_per_pk, self.n_act_bk, self.n_act_pk)
		# print(self.n_per_bk_enj, self.n_per_pk_enj, self.n_act_bk_enj, self.n_act_pk_enj)
		# print()

	def list_cities(self):
		"""
			List all cities of the Database
		"""
		self.list_cities = self.per_bk.distinct('city')
		self.list_cities_enj = self.per_pk_enj.distinct('city')
		print(sorted(self.list_cities), len(self.list_cities))
		print(sorted(self.list_cities_enj), len(self.list_cities_enj))

		for l in self.list_cities:
			print('City:{} - Number:{}'.format(l,self.per_bk.count({'city':l})))

		for l in self.list_cities_enj:
			print('City:{} - (Enjoy) Number:{}'.format(l,self.per_bk_enj.count({'city':l})))

	def sort_collection(self):
		"""
			Sort the collection to see when it started/ended
		"""
		# pprint(self.per_bk.index_information())
		init_sort = self.per_pk.find({'init_time':{'$lt':1481650748}}).sort(
			'init_time',1).limit(1)
		
		# print(list(init_sort)[0]['init_date'])
		for i in init_sort:
			print(i['city'], i['init_date'])

		sort_init = self.per_pk.find({'init_time':{'$gt':1500000000}}).sort(
			'init_time',-1).limit(1)
		# print(list(sort_init)[0]['final_date'])
		for i in sort_init:
			print(i['city'], i['final_date'])


	def analyze_cities(self, start, end, start_ny, end_ny, cities):
		"""
			For OUR cities, check how many cars are available, how many bookings
			have been recorded during a period and if there is an alterate transport method
		"""
		for c in cities:
			print(c)
			if c == 'New York City':
				start_date = start_ny
				end_date = end_ny
			else:
				start_date = start
				end_date = end

			unix_start = time.mktime(start_date.timetuple())
			unix_end = time.mktime(end_date.timetuple())
			cars = 0
			avb_cars_bk = (self.act_bk.aggregate([
				{
					'$match':{
						'city':c
					}
				},
				{
					'$group':{
						'_id':'$plate'
					}
				},
				{
					'$count':'plate'
				}
				]))
			for a in avb_cars_bk:
				cars += a['plate']

			avb_cars_pk = (self.act_pk.aggregate([
				{
					'$match':{
						'city':c
					}
				},
				{
					'$group':{
						'_id':'$plate'
					}
				},
				{
					'$count':'plate'
				}
				]))

			for a in avb_cars_pk:
				cars += a['plate']

			cars_enj = 0
			if c == 'Torino':
				avb_cars_bk_enj = self.per_bk_enj.aggregate([
				{
					'$match':{
						'city':c
					}
				},
				{
					'$group':{
						'_id':'$plate'
					}
				},
				{
					'$count':'plate'
				}
				])
				for a in avb_cars_bk_enj:
					cars_enj += a['plate']

				# avb_cars_pk_enj = (self.act_pk_enj.aggregate([
				# {
				# 	'$match':{
				# 		'city':c
				# 	}
				# },
				# {
				# 	'$group':{
				# 		'_id':'$plate'
				# 	}
				# },
				# {
				# 	'$count':'plate'
				# }
				# ]))
				# for a in avb_cars_pk_enj:
				# 	cars_enj += a['plate']
			else:
				avb_cars_bk_enj = ''
				avb_cars_pk_enj = ''

			# print('%d cars in %s'%(avb_cars, c))
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
				'AvailableCars':cars,
				'AvailableCars_enj':cars_enj,
				'BookingsDate':bk_in_date,
				'PublicTransport':pub_trans,
				'Walking':walk_trans,
				'Driving':drv_trans
			}
			self.container[c] = tmp_obj

		pprint(self.container)

	def CDF(self, start_date, end_date, start_ny, end_ny, cities):
		"""
			Calculate the CDF for the cities over the duration of parkings and bookings.
		"""

		#TODO: Insert also enjoy_data

		sb.set_style('darkgrid')
		fig, axs = plt.subplots(2)
		fig.set_figheight(6)
		fig.set_figwidth(15)
		start_time = time.time()
		for c in cities:
			print(c)
			if c == 'New York City':
				start_date = start_ny
				end_date = end_ny
			else:
				start_date = start_date
				end_date = end_date

			unix_start = time.mktime(start_date.timetuple())
			unix_end = time.mktime(end_date.timetuple())

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
			cnt = 0
			for i in duration_parking:
				lst_parking.append(i['duration'])
			
			for i in duration_booking:
				lst_booking.append(i['duration'])

			# print(cnt)
			lst_parking = np.array(lst_parking)
			lst_booking = np.array(lst_booking)

			p = 1. * np.arange(len(lst_parking)) / (len(lst_parking)-1)
			
			axs[0].plot(np.sort(lst_parking),p, label=c)
			# axs[0].grid()
			axs[0].set_xscale('log')
			# axs[0].savefig('Plots/CDF of Parking Duration for {}'.format())
			
			p = 1. * np.arange(len(lst_booking)) / (len(lst_booking)-1)
			axs[1].plot(np.sort(lst_booking),p, label=c)
			# axs[1].grid()
			axs[1].set_xscale('log')

		axs[0].set_title('CDF Parking/Booking Duration')
		# axs[0].set('Duration (s)')
		plt.xlabel('Duration (s)')

		# axs[0].set_title('CDF Booking Duration')
		plt.legend()
		plt.gcf().autofmt_xdate()
		fig.savefig('Plots/CDF', dpi=600)
		# axs[0].savefig('Plots/CDF Booking')
		# plt.show()

	def CDF_weekly(self, start_date, end_date, start_ny, end_ny, cities):
		"""	
			Calculate the CDF over the duratio of parking and booking aggregating for
			day of the week (or by week)
		"""
		sb.set_style('darkgrid')
		for c in cities:
			print(c)
			if c == 'New York City':
				start_date = start_ny
				end_date = end_ny
			else:
				start_date = start_date
				end_date = end_date

			unix_start = time.mktime(start_date.timetuple())
			unix_end = time.mktime(end_date.timetuple())

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
							},
					},
				},
				{
					'$group':{	
						'_id':'$weekOfMonth',
						'd':{'$push':'$duration'}
					}
				},
				{
					'$sort':{
						'_id':1
					}
				}
				]))

			# cdf_parking = []
			plt.figure(figsize=(15,6))
			for i in duration_parking:
				p = 1. * np.arange(len(i['d'])) / (len(i['d'])-1)
				week = int(i['_id']) + 1
				plt.plot(np.sort(np.array(i['d'])),p, label='Week {}'.format(week))

			
			# plt.figsize()
			plt.legend()
			plt.xlabel('Duration (s)')
			plt.gcf().autofmt_xdate()
			# plt.grid()
			plt.xscale('log')
			plt.title('CDF of Parking Duration for {}'.format(c))
			plt.savefig('Plots/Weekly_CDF_of_Parking_Duration_for_{}'.format(c), dpi=600)

			duration_booking = (self.per_bk.aggregate([
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
					'$sort':{
						'_id':1
					}
				}
				]))

			plt.figure(figsize=(15,6))
			for i in duration_booking:
				p = 1. * np.arange(len(i['d'])) / (len(i['d'])-1)
				week = int(i['_id']) + 1
				plt.plot(np.sort(np.array(i['d'])),p, label='Week {}'.format(week))

			plt.legend()
			plt.xlabel('Duration (s)')
			plt.gcf().autofmt_xdate()
			# plt.grid()
			plt.xscale('log')
			plt.title('CDF of Booking Duration for {}'.format(c))
			plt.savefig('Plots/Weekly_CDF_of_Booking_Duration_for_{}'.format(c), dpi=600)

			# print(list(duration_parking))
			# exit()

	def statistics(self, start_date, end_date, start_ny, end_ny, cities, days=[d for d in range(1,31+1)]):
		"""
			Calculate meaningful statistics on the data. Statistics chosen are 
			Average, Median, Standard Deviation and 75th Percentile
		"""

		#TODO: Insert right filtering and create meaningful plot.
		
		sb.set_style('darkgrid')
		for c in cities:
			print(c)
			if c == 'New York City':
				start_date = start_ny
				end_date = end_ny
			else:
				start_date = start_date
				end_date = end_date

			unix_start = time.mktime(start_date.timetuple())
			unix_end = time.mktime(end_date.timetuple())

			stats_booking = self.per_bk.aggregate([
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
			for i in stats_booking:
				med = statistics.median(i['arr'])
				perc_75 = np.percentile(np.array(i['arr']),75)
				stats.append((i['avg'],i['std'],med,perc_75))

			plt.figure(figsize=(15,6))	
			plt.plot([x[0] for x in stats], label='Average', linewidth=2)
			# plt.set_title('Average')
			plt.plot([x[1] for x in stats], label='Standard Deviation')
			# plt.set_title('StandardDeviation')
			plt.plot([x[2] for x in stats], linestyle='--', marker='*', label='Median')
			# plt.set_title('Median')
			plt.plot([x[3] for x in stats],'-.', marker='o', label='75th Percentile')
			plt.title('City of {}'.format(c))
			
			plt.legend()
			plt.gcf().autofmt_xdate()
			# plt.grid()
			plt.xticks(np.arange(31), days, rotation=30)
			plt.ylabel('Duration (s)')
			# plt.savefig('Plots/City of {} - Bookings Statistics.png'.format(c), dpi=300)
			plt.savefig('Plots/City_of_{}_-_Bookings_Statistics.png'.format(c), dpi=600)
		
			stats_parking = self.per_pk.aggregate([
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
							'$lte':600*60,
							'$gte':2*60
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
						# 'field':{'$sum':{'$cond':[{'$lt':['$field',5000]},1,0]}}
					}
				},
				{
					'$sort':{
						'_id':1
					}
				}
			])
			stats = []
			# plt.figure()
			for i in stats_parking:
				med = statistics.median(i['arr'])
				perc_75 = np.percentile(np.array(i['arr']),75)
				stats.append((i['avg'],i['std'],med,perc_75))

			plt.figure(figsize=(15,6))
			plt.plot([x[0] for x in stats], label='Average')
			# plt.set_title('Average')
			plt.plot([x[1] for x in stats], label='Standard Deviation')
			# plt.set_title('StandardDeviation')
			plt.plot([x[2] for x in stats], linestyle='--', marker='*', label='Median')
			# plt.set_title('Median')
			plt.plot([x[3] for x in stats],'-.', label='75th Percentile')
			plt.title('City of {}'.format(c))
			
			plt.legend()
			plt.ylabel('Duration (s)')
			plt.gcf().autofmt_xdate()
			# plt.grid()
			plt.xticks(np.arange(31), days, rotation=30)
			# plt.savefig('Plots/City of {} - Parkings Statistics.png'.format(c), dpi=300)
			plt.savefig('Plots/City_of_{}_-_Parkings_Statistics.png'.format(c), dpi=600)			

	def density(self, start, end, city):
		"""
			Derive the density of parkings in the Torino area.
					
		"""

		#TODO: Insert right filtering.

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
		table = []
		for i in parked_cars:
			for j in i['coords']:
				table.append([str(i['_id'])+':00',j[1],j[0]])
		
		table = pandas.DataFrame(table, columns=['Hour','Latitude','Longitude'])
		table.to_csv('coordinates.csv')
			
	def density_grid(self, start, end, lat_min=45.01089, long_min=7.60679):
		"""
			Develop a new grid system for Torino with each area colored for how many parkings are present
		"""

		#TODO: Insert right filtering and find a system to color the grid area.

		unix_start = time.mktime(start.timetuple())
		unix_end = time.mktime(end.timetuple())
		z = np.linspace(0,0.1,15)
		table = {}

		for i in z:
			print(i)
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
							'maxDistance':550
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
							'_id':{'loc':'$loc.coordinates', 'h':{'$hour':'$init_date'}}
						}
					}
				])
				key = str(np.round(lat_min+j, decimals=5)) + ' - ' + str(np.round(long_min + i,decimals=5))
				# n = (len(list(parked_at)))
				# table[key] = n
				for p in parked_at:
					# print(p)
					h = p['_id']['h']
					k = key + ' - ' + str(h)
					if k in list(table.keys()):
						# print(table[k])
						table[k] = table[k] + len(p)
					else:
						table[k] = len(p)
					# print(table)
					# exit()
		k = []
		pprint(table)
		for x,y in table.items():
			k.append((x.split(' - ')[0], x.split(' - ')[1], x.split(' - ')[2], y))
		t = pandas.DataFrame(k, columns=['Latitude', 'Longitude','Hour', 'Value'])
		t.to_excel('near_at.xlsx')

	def OD_matrix(self, start, end, lat_min=45.01089, long_min=7.60679):
		"""
			Create an Origin-Destination matrix to visualize where rentals start and end 
			per time.
		"""

		#TODO: Find a visualization method and find a way to use timestamp

		unix_start = time.mktime(start.timetuple())
		unix_end = time.mktime(end.timetuple())
		
		od = self.per_bk.aggregate([
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
				'$project':{
					'_id':0,
					'init_time':1,
					'final_time':1,
					'moved': { '$ne': [ # moved => origin!=destination
						{'$arrayElemAt': [ "$origin_destination.coordinates", 0]},
						{'$arrayElemAt': [ "$origin_destination.coordinates", 1]}]
						},
					'origin': {'$arrayElemAt': ['$origin_destination.coordinates', 0]},
					'dest': {'$arrayElemAt': ['$origin_destination.coordinates', 1]}
				}
			},
			{
				'$match':{
					'moved':True
				}
			},
			{
				'$project':{
					'moved':0
				}
			},
			# {
			# 	'$limit':10
			# }
		])
		OD = np.zeros((15*15,15*15), dtype='int32')
		for o in od:
			O, D = closest_to(o['origin'], o['dest'])
			# print(O,D)
			OD[O, D] += 1

		# exit()
		OriginDestination = pandas.DataFrame(OD)
		OriginDestination.to_excel('OriginDestination_Matrix.xlsx')

	def filtering(self, start, end):
		unix_start = time.mktime(start.timetuple())
		unix_end = time.mktime(end.timetuple())		
		duration_parking = self.per_pk.aggregate([
			{
				'$match':{
					'city':'Torino',
					'init_time':{'$gte':unix_start,'$lte':unix_end}}
				},
			{
				'$project':{
					'duration':{
						'$subtract':['$final_time','$init_time']
					},
					'init_time':1,
					'plate':1
				},
			},
			])
		plt.plot()
		duration_parking = (self.per_pk.aggregate([
			{
				'$match':{
					'city':'Torino',
					'init_time':{'$gte':unix_start,'$lte':unix_end}}
				},
			{
				'$project':{
					'duration':{
						'$subtract':['$final_time','$init_time']
					},
					'init_time':1,
					'plate':1
				},
			},
			{
				'$match':{
					'duration':{
						'$gte':5,
						'$lte':10*60
					}
				}
			},
			{
				'$project':{
					'd':{
						'$divide':['$init_time',60]
					},
					'plate':1
					# 'r':{
					# 	'$trunc':'$d'
					# }
				}
			},
			{
				'$project':{
					'r':{
							'$trunc':'$d'
						},
						'plate':1
				}
			},
			{
				'$group':{
					'_id':'$r',
					'arr':{'$push':'$plate'}
				}
			},
			{
				'$sort':{
					'_id':1
				}
			}
					
			]))
		plt.plot([len(x['arr']) for x in duration_parking])
		plt.show()



def closest_to(O, D, lat_min=45.01089, long_min=7.60679):
	z = np.linspace(0,0.1,15)
	lo = np.array([x + long_min for x in z])
	la = np.array([x + lat_min for x in z])
	# print(lo)
	# print(la)
	O_Long = np.abs(lo - O[0]).argmin()
	O_Lat = np.abs(la - O[1]).argmin()
	Area_O = 15*O_Long + O_Lat

	D_Long = np.abs(lo - D[0]).argmin()
	D_Lat = np.abs(la - D[1]).argmin()
	Area_D = 15*D_Long + D_Lat

	return Area_O, Area_D