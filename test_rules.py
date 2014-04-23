import os,sys
import gzip, json
import csv
import re
import dateutil

class GnipDataProcessor(object):
	def __init__(self, i_path, o_file, chunk_size=50):
			self.count = 0
			self.path = i_path
			self.output_file = csv.writer(open(os.path.join(self.path, o_file), 'w+'))
			self.output_file.writerow(['latitude','longitude','created_at'])
			self.chunk = []
			self.chunk_size = chunk_size
			self.terms = ['sandy','frankenstorm','superstorm','hurricane']
			self.hashtags = ['#sandy', '#sandyhelp', '#sandyrelief', '#superstorm', '#superstormsandy', '#frankenstorm', '#frankenstormsandy', '#frankenstormlive', '#tropicalstorm', '#tropicalstormsandy', '#fucksandy', '#hurricane', '#hurricanesandy', '#SandyNYC', '#recovery', '#CTSandy']
			self.mentions = ['@corybooker', '@NYGovCuomo', '@govchristie', '@craigatfema', '@fdny', '@PANYNJ', '@nycmayorsoffice', '@JimmyVanBramer', '@nycgov', '@NotifyNYC', '@nycoem', '@jcp_l', '@FDNY', '@311NYC', '@njbeachreport', '@sbagov', '@hudnews', '@NJ_TRANSIT', '@Amtrak', '@PepcoConnect', '@ConEdison', '@NOAA', '@RedCross', '@NWS', '@FEMA', '@MikeBloomberg', '@GovMalloyOffice', '@dawnzimmernj', '@NJNationalGuard', '@CTNationalGuard', '@NationalGuardNY', '@readydotgov', '@usNWSgov', '@NHC_Atlantic', '@femaregion2', '@femaregion1', '@NWSNewYorkNY', '@NewYorkCares', '@redcrossny', '@redcrossmetroN', '@CMCGovernment', '@NJOEM2010', '@CityofNewarkNJ', '@camdencountynj', '@CityofHoboken', '@RedCrossNorthNJ', '@ACElecConnect', '@PSEGNews', '@ctdemhs', '@CTRedCross', '@CTLightandPower', '@UnitedIllum']
			self.users = ['corybooker', 'NYGovCuomo', 'govchristie', 'craigatfema', 'fdny', 'PANYNJ', 'nycmayorsoffice', 'JimmyVanBramer', 'nycgov', 'NotifyNYC', 'nycoem', 'jcp_l', 'FDNY', '311NYC', 'njbeachreport', 'sbagov', 'hudnews', 'NJ_TRANSIT', 'Amtrak', 'PepcoConnect', 'ConEdison', 'NOAA', 'RedCross', 'NWS', 'FEMA', 'MikeBloomberg', 'GovMalloyOffice', 'dawnzimmernj', 'NJNationalGuard', 'CTNationalGuard', 'NationalGuardNY', 'readydotgov', 'usNWSgov', 'NHC_Atlantic', 'femaregion2', 'femaregion1', 'NWSNewYorkNY', 'NewYorkCares', 'redcrossny', 'redcrossmetroN', 'CMCGovernment', 'NJOEM2010', 'CityofNewarkNJ', 'camdencountynj', 'CityofHoboken', 'RedCrossNorthNJ', 'ACElecConnect', 'PSEGNews', 'ctdemhs', 'CTRedCross', 'CTLightandPower', 'UnitedIllum']
			self.terms_dict = dict((terms,0) for terms in self.terms)
			self.hashtags_dict = dict((hashtags,0) for hashtags in self.hashtags)
			self.mentions_dict = dict((mentions.lower(),0) for mentions in self.mentions)
			self.users_dict = dict((users.lower(),0) for users in self.users)

			# print self.terms_dict
			# print self.hashtags_dict
			# print self.mentions_dict
			# print self.users_dict
			
			# exit(0)

	def all_files(self):
			for path, dirs, files in os.walk(self.path):
				for f in files:
					yield os.path.join(path, f)
	
	def iter_files(self):
			file_generator = self.all_files()
	
			for f in file_generator:
				try:
					gfile = gzip.open('./'+f)
					for line in gfile:
						self.process_line(line)
					gfile.close()
				except:
					pass
			if self.chunk != []:
				self.process_chunk()
				
	def process_line(self, line):
			try:
				if len(self.chunk) > self.chunk_size:
					self.process_chunk()
					self.chunk = []
				if line.strip() != "":
					data = json.loads(line)
					self.chunk.append(data)
			except:
				print "error storing chunk \n"
				print line
				raise
			
	def process_chunk(self):
			for item in self.chunk:
				try:
					if 'body' in item:
						term = is_term_in_text(item['body'], self.terms)
						userid = item['actor']['preferredUsername'].lower()
						if term:
							self.terms_dict[term] += 1
						if userid in self.users_dict:
							self.users_dict[userid] += 1
						if item['twitter_entities']['user_mentions'] != []:
							for each in item['twitter_entities']['user_mentions']:
								each_mention = each['screen_name']
								mention = '@' + each_mention.lower()
								if mention in self.mentions_dict:
									self.mentions_dict[mention] += 1
						if item['twitter_entities']['hashtags'] != []:
							for each in item['twitter_entities']['hashtags']:
								hashtag = '#' + each['text']
								if hashtag in self.hashtags_dict:
									self.hashtags_dict[hashtag] += 1
						if 'geo' in item:
							if item['geo']['type'] == 'Point':
								self.output_file.writerow([item['geo']['coordinates'][0], item['geo']['coordinates'][1], item['postedTime']])


				except Exception as e:
					print "here", e, item
		
			self.count += self.chunk_size
	
			if self.count % 10000 == 0:
				print self.terms_dict
				print self.hashtags_dict
				print self.mentions_dict
				print self.users_dict
		
				print self.count

	
def is_term_in_text(body, list_text):
		for text in list_text:
			if re.search(re.compile(r'\b%s\b' % text), body, re.IGNORECASE):
				return text
		return False

if __name__ == '__main__':
    total = len(sys.argv)

    if total < 2:
        print "Utilization: python test_rules.py.py <input_dir> <output_geo_file>"
        exit(0)

    csv = GnipDataProcessor(str(sys.argv[1]),str(sys.argv[2]), chunk_size=500)
    csv.iter_files()
    data = {
    	'terms_dict' : csv.terms_dict,
	    'hashtags_dict' : csv.hashtags_dict,
	    'mentions_dict': csv.mentions_dict,
	    'user_dict' : csv.users_dict
    }
    data_to_file = json.dumps(data)

    f = open(os.path.join(str(sys.argv[1]), 'output.txt'),'w')
    f.write(data_to_file)
    f.close()

    print csv.terms_dict
    print csv.hashtags_dict
    print csv.mentions_dict
    print csv.users_dict
    print csv.count