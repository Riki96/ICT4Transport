import pymongo as pm
from pprint import pprint
import datetime
from scipy import stats
import numpy as np
import time
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

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

		pprint(self.obj)

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

			lst_parking = np.array(lst_parking)/60
			lst_booking = np.array(lst_booking)/60

			p = 1. * np.arange(len(lst_parking)) / (len(lst_parking)-1)
			
			axs[0].plot(np.sort(lst_parking),p)
			axs[0].grid()
			
			p = 1. * np.arange(len(lst_booking)) / (len(lst_booking)-1)
			axs[1].plot(np.sort(lst_booking),p)
			axs[1].grid()

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

	def system_utilization(self,start, end,startNY,endNY cities):
		#numero2
	
		collections = ['parkings','bookings']
		rentals_hour = {}

		#DAYS OF THE YEAR
		#1 OTTOBRE = 274
		#31 OTTOBRE = 304

		for c in cities:
			if c == 'New York City':
				unix_start = time.mktime(startNY.timetuple())
				unix_end = time.mktime(endNY.timetuple())
			else:
				unix_start = time.mktime(start.timetuple())
				unix_end = time.mktime(end.timetuple())


			rentals_hour[c]={}

			for a in collections:
				#creating dict for each city
				
				print ('===========')
				print (c) 
				if a == 'parkings':

					tmp_selection = self.per_pk.aggregate([
					{
						'$match':{
							'city':c,
							'init_time':{'$gte':unix_start,'$lte':unix_end}
							}
						},

							{ "$group": {
		        						"_id":{'hour':{'$hour':'$init_date'},'day':{'$dayOfYear':'$init_date'},'plate':'$plate'},
		        						"duration":{'$push':{'$divide':[{'$subtract':['$final_time','$init_time']},3600]}}	
		        						}
		        				}
		            						
							]
							)
				elif a == 'bookings':
					tmp_selection = self.per_bk.aggregate([
					{
						'$match':{
							'city':c,
							'init_time':{'$gte':unix_start,'$lte':unix_end}
							}
						},

							{ "$group": {
		        						"_id":{'hour':{'$hour':'$init_date'},'day':{'$dayOfYear':'$init_date'},'plate':'$plate'},
		        						"duration":{'$push':{'$divide':[{'$subtract':['$final_time','$init_time']},3600]}}	
		        						}
		        				}
		            						
							]
							)

				hour = []
				dayOfYear =[]
				duration =[]
				plate =[]

				for i in tmp_selection:
					hour.append(i['_id']['hour'])
					dayOfYear.append(i['_id']['day'])
					plate.append(i['_id']['plate'])
					duration.append(list(map(int,i['duration'])))


				dataframe = pd.DataFrame({
				 						'hour': hour,
				 						'day':dayOfYear,
				 						'plate':plate,						
				 						'duration':duration})
			
				
				dataframe = dataframe.sort_values(by=['plate', 'day', 'hour'])


				oct_matrix = np.zeros((30,24)) 
			
				for index, row in dataframe.iterrows():
					summ = sum(row['duration'])	
					dataframe.at[index,'duration'] = summ
					
				

					oct_matrix[dataframe.loc[index,'day']-274][dataframe.loc[index,'hour']] += 1

					if dataframe.loc[index,'duration'] > 1:
						is_inside = 0

						for i in range (dataframe.loc[index,'duration']-1):#-1 perche prima ora già considerata
							#considering also the duration
							try:
								oct_matrix[dataframe.loc[index,'day']-274][dataframe.loc[index,'hour']+(i+1)] += 1
							except IndexError:
								is_inside = 1
								cnt = dataframe.loc[index,'duration']-(i+1)
								for j in range(cnt):
									try:
										oct_matrix[dataframe.loc[index,'day']-273][j] += 1
										
									except IndexError:
										break
								if is_inside:
									break
				
				rentals_hour[c][a] = {}
				dayOfWeek = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
				oct_matrix = np.delete(oct_matrix, (0), axis=0)
				oct_matrix = np.delete(oct_matrix, (-1), axis=0)
				oct_matrix = np.delete(oct_matrix, (-1), axis=0)

				i = 0
				for Day in dayOfWeek:
					#rentals_hour[c][a][Day]= np.zeros(shape=(1,24))
					rentals_hour[c][a][Day]= np.zeros(24)

			
					for k in range(3):
						rentals_hour[c][a][Day]+= oct_matrix[(i+(k*7)),:]

					rentals_hour[c][a][Day]/=3

					i+=1
				



				print ('************')
				print ('city: '+c+'\tcollection: '+a)
				print (rentals_hour[c][a]['Monday'])
				print ('=============')

			
			#PLOTTING
			x = list(range(24))
			x_lab = ['hour 0','hour 1','hour 2','hour 3','hour 4','hour 5','hour 6','hour 7','hour 8','hour 9','hour 10','hour 11','hour 12', 'hour 13','hour 14','hour 15','hour 16','hour 17','hour 18','hour 19','hour 20','hour 21','hour 22','hour 23']

			#plotting parkings
			plt.figure(figsize = [10.5,6.5] )
			plt.title('Average of parkings per hour of the day: '+c)
			plt.grid()
			for day in dayOfWeek:

				plt.plot(rentals_hour[c]['parkings'][day],label = day)
	
			plt.xticks(x, x_lab,rotation = 60)
			plt.legend()
			plt.savefig(c+'_Parkings_vs_hour.png')
			plt.close()

			plt.figure(figsize = [10.5,6.5] )
			plt.title('Average of bookings per hour of the day: '+c)
			plt.grid()
			for day in dayOfWeek:

				plt.plot(rentals_hour[c]['bookings'][day],label = day)

			plt.xticks(x, x_lab,rotation = 60)
			plt.legend()
			plt.savefig(c+'_Bookings_vs_hour.png')
			plt.close()


	
								

if __name__ == '__main__':

	cities = ['Torino','New York City','Amsterdam']
	


	DB = MyMongoDB()
	# DB.list_documents()
	# DB.list_cities()

	#start = datetime.date(2017,10,1)
	#end = datetime.date(2017,10,31)
	start = datetime.date(2017,10,1)
	end = datetime.date(2017,10,31)

	# DB.analyze_cities(cities, start, end)
	# DB.CDF(datetime.date(2017,10,1),datetime.date(2017,10,31),cities)
	#DB.CDF_weekly(datetime.date(2017,10,1),datetime.date(2017,10,31),cities)
	DB.system_utilization(start, end, cities)

