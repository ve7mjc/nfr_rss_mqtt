#!./venv/bin/python

# scrape Nanaimo Fire Rescue Incidents RSS feed to format

import paho.mqtt.client as mqtt

import feedparser # rss feed parser
import time # sleep
from datetime import datetime
from time import mktime
from dateutil.parser import parse # for ISO datetime
from time import strftime

from pygelf import GelfUdpHandler
import logging
import configparser
import json

# configuration file
configfile='nfr.cfg'
config = configparser.ConfigParser()
config.read(configfile)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
if ("general" in config) and ("gelf_logging_host" in config["general"]) and ("gelf_logging_port" in config["general"]):
    logger.addHandler(GelfUdpHandler(host=config["general"]["gelf_logging_host"], port=int(config["general"]["gelf_logging_port"]), _app_name="nfr_rss"))
    print("Logging GELF to", config["general"]["gelf_logging_host"])
else:
    print("NOTICE: Centralized logging is not configured!")

pointerfile='pointer.dat'

# retrieve pointer to last id received
last_id = 0
try:
    with open(pointerfile, 'r') as f:
        last_id = int(f.read())
except FileNotFoundError:
    print("Pointer file not found! Assuming 0")

def writePointer(id):
    id = int(id) # to be sure
    global last_id
    last_id = id
    with open(pointerfile, 'w') as f:
        f.write(str(id))
    return

broker_address="127.0.0.1" 
url = "https://www.nanaimo.ca/fire_rescue_incidents/Rss"
rss_fetch_time_secs = 60

mqttc = mqtt.Client("nfr-rss") #create new instance

incidents = {}
message_queue = []

def send_queued_messages():
    global message_queue
    for msg in message_queue:
        mqttc.publish("irc/channel/scanbc-data/msg", msg)
    message_queue = []

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    # 0: Connection successful 
    # 1: Connection refused - incorrect protocol version 
    # 2: Connection refused - invalid client identifier 
    # 3: Connection refused - server unavailable 
    # 4: Connection refused - bad username or password 
    # 5: Connection refused - not authorised 
    # 6-255: Currently unused   
    
    if rc==0:
        logger.info("Connected to MQTT broker")
        send_queued_messages()
    else:
        logger.error("Error connecting to MQTT broker")

mqttc.on_connect = on_connect

if "mqtt" not in config:
    print("ERROR: Missing MQTT config!")
    exit()

mqttc.username_pw_set(config['mqtt']['username'], config['mqtt']['password'])
mqttc.connect(config['mqtt']['host']) #connect to broker
mqttc.loop_start()

def scrape_id(event):
    id = event["link"].strip("https://www.nanaimo.ca/fire_rescue_incidents/details/")
    return int(id)
    
def scrape_type(event):
    # title is "CIVIC_ADDRESS - CALL_TYPE"
    type = event["title"].split(" - ")
    return type[1].strip()
    
def scrape_date(event):
    # RSS provides `updated` with an ASCII date
    # updated_parsed (provided by feedparser lib)
    date_obj = parse(event["updated"])
    return date_obj

def has_apparatus(event):
    # for some reason, content nested as an array member
    if "apparatus were deployed:" in event['content'][0]['value']:
        return True

def scrape_apparatus(event):
    apparatus_text = ""
    if has_apparatus(event):
        data = event['content'][0]['value']
        pos_start = data.index('<ul>') + 4
        pos_end = data.index('</ul>')
        apparatus_list = data[pos_start:pos_end]
        apparatus_list = apparatus_list.split('<li>')
        for apparatus in apparatus_list:
            apparatus = apparatus.replace('</li>','').strip()
            apparatus_text += apparatus + ", "
    return apparatus_text.strip(", ")
    
def scrape_civic_address(event):
    # title is "CIVIC_ADDRESS - CALL_TYPE"
    type = event["title"].split(" - ")
    return type[0].strip()

    if mqttc.connected:
        for msg in message_queue:
            mqttc.publish("irc/channel/scanbc-data/msg",msg)
        message_queue = []

logger.info("Started NFR RSS Feed Parser - VE7MJC")

while True:
    
    # retrieve RSS objects
    d = feedparser.parse(url)
    
    incidents = d["entries"]
    incidents.reverse()
    
    for event in incidents:

        incident = {}
        incident["id"] = scrape_id(event)

        if incident["id"] > last_id:
        #if incident["id"] not in incidents:
            # new incident
            logger.info("new NFR event received: %s" % json.dumps(event))
            
            print("new incident detected")
            incident["date"] = scrape_date(event)
            incident["type"] = scrape_type(event)
            incident["civic_address"] = scrape_civic_address(event)
            # incidents[incident["id"]] = incident

            msg = "NFR " + str(incident["id"]) + " " + scrape_date(event).strftime("%m-%d %H:%M:%S") + " | " + scrape_type(event) + " | " + scrape_civic_address(event) + " | UNITS " + scrape_apparatus(event)
            logger.info("Sent NFR Incident: %s" % msg)
            message_queue.append(msg)
            writePointer(str(incident["id"]))
            print(msg)

    # send messages and then sleep
    send_queued_messages()
    time.sleep(rss_fetch_time_secs)
    
# entry
#   title
#   link_href =""/ (seems to have details/{id}
#   id (same url)
#   updated
#   content type="html"
#   author (City of Nanaimo)
#   georss:line

#<entry>
#<title>700 BLOCK CENTRE ST - Medical Aid</title>
#<link href="https://www.nanaimo.ca/fire_rescue_incidents/details/35949332"/>
#<id>
#https://www.nanaimo.ca/fire_rescue_incidents/details/35949332
#</id>
#<updated>2020-01-18T12:10:46-08:00</updated>
#<content type="html">
#Nanaimo, BC: Medical Aid incident occurred at or near the address 700 BLOCK CENTRE ST on Saturday, January 18, 2020. The following apparatus were deployed: <ul> <li>R1</li> </ul>
#</content>
#<author>
#<name>City of Nanaimo</name>
#</author>
#<georss:line>49.151447 -123.929576 49.149433 -123.929573</georss:line>
#</entry>

# d.feed.get('title', 'No title')

# d['feed']['title']
