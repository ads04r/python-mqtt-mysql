#!/usr/bin/python3

import paho.mqtt.client as mqtt
import pymysql, json, os, sys, time, datetime

config_path = os.path.abspath(os.path.dirname(sys.argv[0]) + '/config.json')
if not(os.path.exists(config_path)):
	sys.stderr.write("Cannot find config file " + config_path + "\n")
	sys.exit(1)

def callback(client, userdata, message):

	global config
	global queries

	topic = message.topic
	payload = str(message.payload.decode('utf-8'))
	filter = config['mqtt']['topic_prefix']
	if not(topic.startswith(filter)):
		return

	topic = topic.replace(filter, '')
	parse = topic.split('/')
	if not(parse[0] in config['types']):
		return

	ds = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
	type = config['types'][parse[0]]
	if type == 'on_off':
		if payload == 'on':
			payload = '1'
		if payload == 'off':
			payload = '0'
		if payload == 'open':
			payload = '1'
		if payload == 'close':
			payload = '0'
		if payload == 'closed':
			payload = '0'
	query = "insert ignore into " + config['mysql']['table'] + " (meter_zone, meter_type, period, value) values ('" + topic + "', '" + type + "', '" + ds + "', '" + payload + "');"
	queries.append(query)

with open(config_path) as data:
	config = json.load(data)
	data.close()

config['types'] = {}
queries = []

db = pymysql.connect(config['mysql']['host'], config['mysql']['username'], config['mysql']['password'], config['mysql']['database'])
query = "select distinct meter_zone, meter_type from readings where meter_zone like '%/%';";
cursor = db.cursor()
cursor.execute(query)
for row in cursor.fetchall():
	parse = row[0].split('/')
	key = parse[0]
	if key in config['types']:
		continue
	config['types'][key] = row[1]
db.close()

client = mqtt.Client("mqtt2mysql")
client.on_message = callback
client.connect(config['mqtt']['host'])
client.loop_start()
client.subscribe('#')
dtnext = datetime.datetime.now() + datetime.timedelta(seconds=60)
while True:
	while datetime.datetime.now() < dtnext:
		pass
	q = queries
	queries = []
	dtnext = datetime.datetime.now() + datetime.timedelta(seconds=60)
	if len(q) > 0:
		try:
			db = pymysql.connect(config['mysql']['host'], config['mysql']['username'], config['mysql']['password'], config['mysql']['database'], autocommit=True)
			cursor = db.cursor()
			for query in q:
				cursor.execute(query)
			db.close()
		except:
			queries = q + queries

client.loop_stop()

