import pymongo as pm
from pprint import pprint
import datetime
from scipy import stats
import numpy as np
import time
import matplotlib.pyplot as plt
import pandas as pd
import json
import copy

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
		print(sorted(self.list_cities))

	def visualization(self,start,end,cities):
		unix_start = time.mktime(start.timetuple())
		unix_end = time.mktime(end.timetuple())

		# 0->Turin
		# 1->NYC
		# 2->Amsterdam
		bk_collector = {'Torino':[],'New York City':[],'Amsterdam':[]}

		for city in cities:		
			for day in range(31):
				tmp = {day:[]}
				bk_collector[city].append(tmp)
				for hour in range(24):
					for minute in range(60):
						tmp = {'%02d:%02d'%(hour,minute):0}
						bk_collector[city][day].update(tmp)
				bk_collector[city][day].pop(day)
			bk_filtered = copy.deepcopy(bk_collector)
			pk_collector = copy.deepcopy(bk_collector)
			pk_filtered = copy.deepcopy(bk_collector)

		st_time = time.strftime("%H:%M:%S", time.gmtime())
		print('----------------------------------')
		print('Started analyzing Database at', st_time)
		for city in cities: 
			#---------------------------------------------------------------------------------------
			# BOOKINGS
			#---------------------------------------------------------------------------------------
			print(' ---> Analyzing bookings of',city,'at:',time.strftime("%H:%M:%S", time.gmtime()))
			temp_bk = self.per_bk.aggregate([{
				'$match':
					{
					'city':city,
					'init_time':{'$gte':unix_start,'$lte':unix_end}
					}
				},
				{
				'$project':{
					'init_date':1,
					'init_time':1,
					'city':1,
					'duration': { '$divide': [ { '$subtract': ["$final_time", "$init_time"] }, 60 ] },
					'minute':{'$minute':'$init_date'},
					'hour':{'$hour':'$init_date'},
					'day':{'$dayOfMonth':'$init_date'},
					}
				},
				{
				'$group':{
					'_id':{
					'M':'$minute',
					'H':'$hour',
					'D':'$day'
					},
					'count':{'$sum':1}
					}
				}
				])
			temp_bk = list(temp_bk)

			temp_filtered_bk = self.per_bk.aggregate([{
				'$match':
					{
					'city':city,
					'init_time':{'$gte':unix_start,'$lte':unix_end}
					}
				},
				# FILTER PART -------------------------------------------------------
				{
				'$project':{
					'init_date':1,
					'init_time':1,
					'city':1,
					'duration': { '$divide': [ { '$subtract': ["$final_time", "$init_time"] }, 60 ] },  
					'minute':{'$minute':'$init_date'}, # no useful for filtering
					'hour':{'$hour':'$init_date'}, # no useful for filtering
					'day':{'$dayOfMonth':'$init_date'}, # no useful for filtering
					'dist_lat':{'$abs':{'$subtract': [{'$arrayElemAt':[{'$arrayElemAt': [ "$origin_destination.coordinates", 0]}, 0]}, {'$arrayElemAt':[{'$arrayElemAt': [ "$origin_destination.coordinates", 1]}, 0]}]}},
					'dist_long':{'$abs':{'$subtract': [{'$arrayElemAt':[{'$arrayElemAt': [ "$origin_destination.coordinates", 0]}, 1]}, {'$arrayElemAt':[{'$arrayElemAt': [ "$origin_destination.coordinates", 1]}, 1]}]}},
					}
				},
				{'$match':{
					'$or':[
						{'dist_long':{'$gte':0.0003}},
						{'dist_lat':{'$gte':0.0003}},
						],
					'duration':{'$lte':180,'$gte':2},
					}
				},
				# FILTER PART ENDS -------------------------------------------------------
				{
				'$group':{
					'_id':{
					'M':'$minute',
					'H':'$hour',
					'D':'$day'
					},
					'count':{'$sum':1},
					}
				}
				])
			temp_filtered_bk = list(temp_filtered_bk)

			for c in temp_filtered_bk:
				bk_filtered[city][c['_id']['D']]['%02d:%02d'%(c['_id']['H'],c['_id']['M'])] += c['count']
			for d in temp_bk:
				bk_collector[city][d['_id']['D']]['%02d:%02d'%(d['_id']['H'],d['_id']['M'])] += d['count']
				
			#---------------------------------------------------------------------------------------
			# PARKINGS
			#---------------------------------------------------------------------------------------
			print(' ---> Analyzing parkings of',city,'at:',time.strftime("%H:%M:%S", time.gmtime()))
			print()
			temp_pk = self.per_pk.aggregate([{
				'$match':
					{
					'city':city,
					'init_time':{'$gte':unix_start,'$lte':unix_end}
					}
				},
				{
				'$project':{
					'init_date':1,
					'init_time':1,
					'city':1,
					# 'duration': { '$divide': [ { '$subtract': ["$final_time", "$init_time"] }, 60 ] },
					'minute':{'$minute':'$init_date'},
					'hour':{'$hour':'$init_date'},
					'day':{'$dayOfMonth':'$init_date'},
					}
				},
				{
				'$group':{
					'_id':{
					'M':'$minute',
					'H':'$hour',
					'D':'$day'
					},
					'count':{'$sum':1}
					}
				}
				])
			temp_pk = list(temp_pk)

			temp_filt_pk = self.per_pk.aggregate([{
				'$match':
					{
					'city':city,
					'init_time':{'$gte':unix_start,'$lte':unix_end}
					}
				},
				{
				'$project':{
					'init_date':1,
					'init_time':1,
					'final_time':1,
					'city':1,
					'plate':1,
					'loc':1,
					'duration': { '$divide': [ { '$subtract': ["$final_time", "$init_time"] }, 60 ] },
					}
				},
				{
				'$match':{
					'duration':{'$gte':1}
					}
				},
				{
				'$group':{
					'_id':{'plate':'$plate'},
					'count':{'$sum':1},
					'initials':{'$push':'$init_time'},
					'finals':{'$push':'$final_time'},
					'coordinates':{'$push':'$loc.coordinates'}
					}
				},
				])
			temp_filt_pk = list(temp_filt_pk)
			# pprint(temp_filt_pk)


			for e in temp_pk:
				pk_collector[city][e['_id']['D']]['%02d:%02d'%(e['_id']['H'],e['_id']['M'])] += e['count']

			for f in temp_filt_pk:
				plate = f['_id']['plate']
				coordinates = f['coordinates']
				start_time, final_time = np.sort(f['initials']), np.sort(f['finals'])
				 
				# FILTERING
				# print('------------------------------------')
				# print(len(start_time))
				new_st_time = []
				new_fn_time = []
				CNT = 0
				for i in range(1,len(start_time)):
					pk_duration = start_time[i]-final_time[i-1]
					distance = abs(coordinates[i][0] - coordinates[i-1][0] + coordinates[i][1] - coordinates[i-1][1])
					if distance > 0.0005 and pk_duration >= 300:
						new_st_time.append(start_time[CNT])
						new_fn_time.append(final_time[i-1])
						CNT = i
				# print('st_time',len(new_st_time))

				for index in range(1,len(new_st_time)):
					day = int(datetime.datetime.utcfromtimestamp(new_st_time[index]).strftime('%d'))
					hour = int(datetime.datetime.utcfromtimestamp(new_st_time[index]).strftime('%H'))
					minute = int(datetime.datetime.utcfromtimestamp(new_st_time[index]).strftime('%M'))

					pk_duration = new_st_time[index] - new_fn_time[index-1]
					if pk_duration<300:
						print(datetime.datetime.utcfromtimestamp(new_st_time[index-1]).strftime('%d %H:%M'),'-->',datetime.datetime.utcfromtimestamp(new_fn_time[index-1]).strftime('%d %H:%M'))
						print(datetime.datetime.utcfromtimestamp(new_st_time[index]).strftime('%d %H:%M'),'-->',datetime.datetime.utcfromtimestamp(new_fn_time[index]).strftime('%d %H:%M'))
						print('gianni:',pk_duration,day,hour,minute)
					
					pk_filtered[city][day]['%02d:%02d'%(hour,minute)] += 1
					# print(pk_filtered[city][day]['%02d:%02d'%(hour,minute)])


		fs_time = time.strftime("%H:%M:%S", time.gmtime()) 
		print('Finished analyzing Database at', fs_time)
		print('----------------------------------')
		n_bk = {'Torino':[],'New York City':[],'Amsterdam':[]}
		n_pk = copy.deepcopy(n_bk)
		n_filt_bk = copy.deepcopy(n_bk)
		n_filt_pk = copy.deepcopy(n_bk)

		for city in cities:
			plot_tot_bk, plot_tot_pk, plot_filt_tot_pk, plot_filt_tot_bk = [], [], [], []
			for x in range(1,31):
				plot_bk = list(bk_collector[city][x].values())
				plot_pk = list(pk_collector[city][x].values())

				n_bk[city].append(sum(plot_bk))
				n_pk[city].append(sum(plot_pk))
				# if x==1:
					# plt.plot(plot_bk,'r', label='Not filtered data')
					# plt.plot(plot_pk,'r', label='Not filtered data on day: %d'%x)
				# else:
					# plt.plot(plot_bk,'r')
					# plt.plot(plot_pk,'r')

				plot_filt_bk = list(bk_filtered[city][x].values())
				plot_filt_pk = list(pk_filtered[city][x].values())

				n_filt_bk[city].append(sum(plot_filt_bk))
				n_filt_pk[city].append(sum(plot_filt_pk))
				# if x==1:
				# 	plt.plot(plot_filt_bk,'b', label='Filtered data')
					# plt.plot(plot_filt_pk,'b', label='Filtered data on day: %d'%x)
				# else:
				# 	plt.plot(plot_filt_bk,'b')
					# plt.plot(plot_filt_pk,'b')
				for y in range(len(plot_pk)):
					plot_tot_bk.append(plot_bk[y])
					plot_tot_pk.append(plot_pk[y])
					plot_filt_tot_bk.append(plot_filt_bk[y])
					plot_filt_tot_pk.append(plot_filt_pk[y])
			plt.figure()
			plt.title('Bookings in '+str(city))
			plt.plot(plot_tot_bk, 'r', label='Not filtered Data')
			plt.plot(plot_filt_tot_bk, 'b', label='Filtered Data')
			plt.legend()

			plt.figure()
			plt.title('Parkings in '+str(city))
			plt.plot(plot_tot_pk, 'r', label='Not filtered Data')
			plt.plot(plot_filt_tot_pk, 'b', label='Filtered Data')
			plt.legend()
			plt.show()

			# plt.figure()
			# plt.plot(n_bk[city])
			# plt.plot(n_filt_bk[city])
			# plt.show()

			plt.figure()
			plt.plot(n_pk[city])
			plt.plot(n_filt_pk[city])
			plt.show()

		for city in cities:
			tot_bk, tot_filt_bk = sum(n_bk[city]), sum(n_filt_bk[city])
			tot_pk, tot_filt_pk = sum(n_pk[city]), sum(n_filt_pk[city])

			print('♣ BOOKINGS: Percentage of filtered data for',city,'is',round(100*(tot_bk - tot_filt_bk)/tot_bk,2),'%')
			print('♠ PARKINGS: Percentage of filtered data for',city,'is',round(100*(tot_pk - tot_filt_pk)/tot_pk,2),'%')
			print('+++++++++++++++++++++++++++++++++++++++++')

if __name__ == '__main__':

	cities = ['Torino','New York City','Amsterdam']

	DB = MyMongoDB()
	# DB.list_documents()
	# DB.list_cities()

	start = datetime.date(2017,10,1)
	end = datetime.date(2017,10,31)

	# DB.analyze_cities(cities, start, end)
	# DB.CDF(datetime.date(2017,10,1),datetime.date(2017,10,31),cities)
	# DB.CDF_weekly(datetime.date(2017,10,1),datetime.date(2017,10,31),cities)
	# DB.optional(start, end)
	DB.visualization(start, end, cities)
	# DB.filtering(start, end)
