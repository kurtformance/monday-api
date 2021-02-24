
import sys
sys.path.append("/root")
import module
from module import CONFIG as CFG
import requests
import json
import pandas as pd
import pdb
import MySQLdb
import _mysql_exceptions
import logging
import traceback
from datetime import datetime
import math
from unidecode import unidecode
from checkdigit import upc as upc_check
from unidecode import unidecode
import argparse


campaign_insert_sql = "insert into monday_campaign (campaign_name, start_date, end_date, impressions, budget, client_id, board_name, group_name, last_updated, campaign_id) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
campaign_update_sql = "update monday_campaign set campaign_name=%s, start_date=%s, end_date=%s, impressions=%s, budget=%s, client_id=%s, board_name=%s, group_name=%s, last_updated=%s where campaign_id = %s"

campaign_attr_insert_sql = "insert into monday_campaign_attribute (attr_name, attr_value, campaign_id) values (%s,%s,%s)"
campaign_attr_update_sql = "update monday_campaign_attribute set attr_name=%s, attr_value=%s where campaign_id=%s"

campaign_upc_insert_sql = "insert into monday_campaign_upc (upc, campaign_id) values (%s, %s)"
campaign_upc_delete_sql = "delete from monday_campaign_upc where campaign_id=%s"

board_query_ids = []
board_query_ids.append(CFG.MONDAY_CURRENT_PROJECTS_BOARD_ID)
board_query_ids.append(CFG.MONDAY_RFP_BOARD_ID)
board_query_ids.append(CFG.MONDAY_ARCHIVED_PROJECTS_BOARD_ID)

class monday_api(object):


	def __init__(self):
		logfile='/tmp/' + str(self.__class__.__name__)+ '-' + str(datetime.now().timestamp()) +'.log'
		logging.basicConfig(filename=logfile, level=logging.DEBUG, format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
		logging.info("%s __init__" % self.__class__.__name__)
		self.db = MySQLdb.connect(host=CFG.DB_HOST, user=CFG.DB_USER, passwd=CFG.DB_PWD, db=CFG.DB_DATABASE, use_unicode=True)
		self.cur = self.db.cursor(MySQLdb.cursors.DictCursor)
		self.api_url = "https://api.monday.com/v2"
		self.api_key = CFG.MONDAY_API_KEY
		self.auth = {'Authorization':self.api_key}
		
		self.full_refresh=False
	
		self.column_id_mapping = {'client':'client','retailers':'text8','status':'status1','start_date':'date5','end_date':'date4','budget':'budget','impressions_text':'text','impressions':'numbers2'}

		self.board_column_mapping = {}
		self.column_id_mapping = {}
		self.column_id_mapping['client']='client'
		self.column_id_mapping['retailers']='text8'
		self.column_id_mapping['status']='status1'
		self.column_id_mapping['start_date']='date5'
		self.column_id_mapping['end_date']='date4'
		self.column_id_mapping['budget']='budget'
		self.column_id_mapping['impressions_text']='text'
		self.column_id_mapping['impressions']='numbers2'
		self.column_id_mapping['rfp_form_impressions']='text2'
		self.column_id_mapping['upcs'] = 'long_text9'
		self.column_id_mapping['last_updated'] = 'last_updated'

		self.board_column_mapping[CFG.MONDAY_CURRENT_PROJECTS_BOARD_ID] = self.column_id_mapping.copy()
		self.board_column_mapping[CFG.MONDAY_ARCHIVED_PROJECTS_BOARD_ID] = self.column_id_mapping.copy()

		
		self.column_id_mapping['client']='dropdown'
		self.column_id_mapping['end_date']='date'
		self.column_id_mapping['start_date']='date4'
		self.column_id_mapping['impressions']='numbers06'
		self.column_id_mapping['budget']='numbers0'
		self.column_id_mapping['retailers']='form_retailers'
		self.column_id_mapping['status']='status5'
		self.column_id_mapping['timeline']='timeline'
		self.board_column_mapping[CFG.MONDAY_RFP_BOARD_ID] = self.column_id_mapping.copy()

#		self.column_id_mapping['retailers']=['text8','form_retailers']
#		self.column_id_mapping['status']=['status1']
#		self.column_id_mapping['start_date']=['date5','date4']
#		self.column_id_mapping['end_date']=['date4','date']
#		self.column_id_mapping['budget']=['budget','numbers0']
#		self.column_id_mapping['rfp_form_budget']=['form_budget']
#		self.column_id_mapping['impressions_text']=['text']
#		self.column_id_mapping['impressions']=['numbers2','numbers06']
#		self.column_id_mapping['rfp_form_impressions']=['text2']
#		self.column_id_mapping['upcs'] =[ 'long_text9','text89']

	def _remove_non_ascii(self, text):
		return unidecode(unicode(text, encoding = "utf-8"))


	def fetch_all_boards(self):
		query = '{ boards {id name} }'
		boards_result = requests.post(url=self.api_url, json={'query':query}, headers=self.auth)

		logging.debug(boards_result.json())
		
		return boards_result.json()


	def _fetch_client_by_name(self, client_name):
		result = self.cur.execute("select * from client where upper(client_name) = %s", (client_name.upper(),))
		if result:
			client = self.cur.fetchone()
		else:
			client = self._add_client(client_name)
		return client


	def _fetch_item_value(self, item, field_name):
		for column_val in item['column_values']:
			for val in self.column_id.values():
				if column_val['id'] == val:
					return column_val['text']


	def _fetch_item_field(self, board_id, item, field_name):
		#logging.debug("field_name:%s" % field_name)
		column_id_mapping = self.board_column_mapping[int(board_id)]
		for column_val in item['column_values']:
			#logging.debug("%s =? %s" % (column_val, column_id_mapping[field_name]))
			if column_val['id'] == self.column_id_mapping[field_name]:
				return column_val['text']

	def _add_client(self, client_name):
		self.cur.execute("insert into client (client_name) values (%s)", (client_name,))
		client_id = self.cur.lastrowid
		self.db.commit()
		return {'client_id':client_id, 'client_name':client_name}
		


	def fetch_current_projects(self, persist=False):
		query = '{ boards (ids:[%s,%s,%s]) {id name groups {id title} items { id name board {id name} group {id title} column_values{title id type text} } } }' % (CFG.MONDAY_CURRENT_PROJECTS_BOARD_ID,CFG.MONDAY_RFP_BOARD_ID,CFG.MONDAY_ARCHIVED_PROJECTS_BOARD_ID)
		pdb.set_trace()
		result = requests.post(url=self.api_url, json={'query':query}, headers=self.auth)
		logging.debug(result.json())

		data = result.json()

		self.cur.execute("select last_updated from update_tracker where table_name = 'monday_campaign'")
		last_run_date = self.cur.fetchone()['last_updated']


		campaigns = []
		for board in data['data']['boards']:
			for item in board['items']:
				campaign = {}
				campaign_id = item['id']
				campaign_name = item['name']
				if campaign_name:
					campaign_name = unidecode(campaign_name)
				logging.debug("processing campaign_name:%s", (campaign_name,))
				board_id = item['board']['id']
				board_name = item['board']['name']
				if board_name:
					board_name = board_name.encode('ascii',errors='ignore')

				group_name=item['group']['title']
				if group_name:
					group_name = group_name.encode('ascii',errors='ignore')


				# Data from attributes
				"""
					Only process campaigns which have changed since the last db update from Monday
					Can be overridden with the --full_campaign=True command line argument
				"""
				if not self.full_refresh:
					last_updated = self._fetch_item_field(board_id, item,'last_updated')
					last_updated_date = datetime.strptime(last_updated,'%Y-%m-%d %H:%M:%S UTC')
					if last_updated_date <= last_run_date:
						logging.info("Skipping campaign %s.  No changes found since last load." % (campaign_name,))
						continue
					last_updated_str = last_updated_date.strftime('%Y-%m-%d %H:%M:%S')

				else:
					last_updated_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


				"""
					Parse fields from monday columns
				"""
				timeline = self._fetch_item_field(board_id, item,'timeline')
				if timeline:
					start_date = timeline.split(" - ")[0]
					end_date = timeline.split(" - ")[1]
				else:
					start_date = self._fetch_item_field(board_id, item,'start_date')
					if not start_date:
						start_date = None
					end_date = self._fetch_item_field(board_id, item,'end_date')
					if not end_date:
						end_date = None

				impressions = self._fetch_item_field(board_id,item,'impressions')
				if impressions and impressions.isdigit():
					impressions = int(impressions)
				else:
					impressions = None

				budget = self._fetch_item_field(board_id,item,'budget')
				if budget and budget.replace('.','',1).isdigit():
					budget = float(budget)
				else:
					budget = None

				# Lookup client by name
				client_name = self._fetch_item_field(board_id,item,'client')
				client_id = None
				if client_name:
					client = self._fetch_client_by_name(client_name)
					if client:
						client_id = client['client_id']

				# Parse UPCs
				upcs_text =  self._fetch_item_field(board_id,item,'upcs')
				upcs = []
				if upcs_text:
					for upc in upcs_text.split(","):
						upcs.append(upc)
				print("campaign_name:%s" % (campaign_name,))
				campaign_name = campaign_name.encode('utf-8').decode('utf-8')

				campaign['campaign_id'] = campaign_id
				campaign['campaign_name'] = campaign_name
				campaign['start_date'] = start_date
				campaign['end_date'] = end_date
				campaign['impressions'] = impressions
				campaign['budget'] = budget
				campaign['board_name'] = board_name
				campaign['last_updated'] = last_updated_str

				bind = []
				bind.append(campaign_name)
				bind.append(start_date)
				bind.append(end_date)
				bind.append(impressions)
				bind.append(budget)
				bind.append(client_id)
				bind.append(board_name)
				bind.append(group_name)
				bind.append(last_updated_str)
				bind.append(campaign_id)


				if persist:
					print("campaign_id:%s" % (campaign_id,))
					logging.debug("campaign_id:%s" % (campaign_id,))
					try:
						self.cur.execute(campaign_insert_sql, tuple(bind))					
					except _mysql_exceptions.IntegrityError:
						logging.debug("Duplicate key found.  Updating campaign record")
						print("Duplicate key found.  Updating campaign record")
						self.cur.execute(campaign_update_sql, tuple(bind))					
					except Exception as e:
						print(bind)
						logging.debug(bind)
						track = traceback.format_exc()
						print(track)
						logging.error(track)
					
					for upc in upcs:
						upc = upc.zfill(12)
						if upc_check.upc_check(upc) == True:
							None
						else:
							upc = upc[1:]+upc_check.upc_calculate(upc[1:])

						try:
							self.cur.execute(campaign_upc_insert_sql, (upc,int(campaign_id)))
						except _mysql_exceptions.IntegrityError:
							logging.debug("Dupe key in campaign_upc. Continuing")
						except Exception as e:
							track = traceback.format_exc()
							print(track)
							logging.error(track)


				campaign_attrs = []
				for attr in item['column_values']:
					campaign_attr = {}
					attr_name = attr['title']
					logging.debug("attr_name:%s" % (attr_name,))
					attr_value = attr['text']
					logging.debug("attr_value:%s" % (attr_value,))
					if attr_value:
						attr_value = attr_value.encode('ascii',errors='ignore')
					attr_bind = []
					attr_bind.append(attr_name)
					attr_bind.append(attr_value)
					attr_bind.append(campaign_id)
					campaign_attr['attr_name'] = attr_name
					campaign_attr['attr_value'] = attr_value
					campaign_attrs.append(campaign_attr)
					if persist:
						try:
							self.cur.execute(campaign_attr_insert_sql, tuple(attr_bind))					
						except _mysql_exceptions.IntegrityError:
							logging.debug("Duplicate key found.  Updating campaign_attr record")
							self.cur.execute(campaign_attr_update_sql, tuple(attr_bind))
						except Exception as e:
							print("attr_name:%s" % (attr_name,))
							print("attr_value:%s" % (attr_value,))
							track = traceback.format_exc()
							print(track)
							logging.error(track)
				campaign['attributes'] = campaign_attrs

				campaigns.append(campaign)

				logging.debug("finished processing campaign_name:%s", (campaign_name,))


		if persist:
			self.cur.execute("update update_tracker set last_updated = now() where table_name = 'monday_campaign'")
			self.db.commit()


		return campaigns





if __name__ == "__main__":
	parser = argparse.ArgumentParser(prog=sys.argv[0])
	parser.add_argument("--full_refresh","-f", help="Force a full refresh ignoring update date",type=bool, default=False)
	ag = parser.parse_args()
	
	api = monday_api()
	if ag.full_refresh:
		print("full_refresh is True. Ignoring update date.")
		api.full_refresh=True

	api.fetch_current_projects(persist=True)



