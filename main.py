#!/usr/bin/env python3

import time
import json
import yaml
import threading

from datetime import datetime
from datetime import timedelta
from time import sleep


# add in stomp lib
import stomp

# add in the mqtt client
import paho.mqtt.client as mqtt


class Listener(stomp.ConnectionListener):
    _mq: stomp.Connection

    def __init__(self, mq: stomp.Connection, settings, durable=False):
        self.settings=settings
        self._mq = mq
        self.is_durable = durable
        self.SPW5261=False
        self.SPW5261BR=False
        self.SPW5264=False
        self.SPW5264BR=False
        self.down=False
        self.changeTime=None
        self.trainList=[]
        self.results=[]
        self.stats={}
        self.avgTime=-1
        
        self.downState=None
        
        self.mqttclient = mqtt.Client()

        self.mqttclient.connect(settings["mqttBroker"], settings["mqttPort"], 60)
        self.mqttclient.loop_start()

        self.loadStats()

        self.thread = threading.Thread(target=self.thread_function)
        self.thread.start()
        
    def thread_function(self):
        while True:
			
            tmp_downstate=self.downState
            print("CHECK")
            if tmp_downstate!=None:
                print("DOWN: "+str(tmp_downstate))
                currentTime=datetime.now()
                timestamp=tmp_downstate["timestamp"]
                timeLeft=tmp_downstate["timeLeft"]

                # get time between now and when the timestamp was created
                diffTime=(currentTime-timestamp).total_seconds()
                # we now know how many seconds to subtract from the timeLeft
                tmp_timeLeft=timeLeft - diffTime
                # generate the estimated time up
                estTimeUp=(currentTime+timedelta(seconds=tmp_timeLeft))
                print("EST TIME UP: "+str(estTimeUp))
                     

                # and publish the message
                self.mqttclient.publish("traintrack/timeToOpen", json.dumps(
                        {
                            "timeStamp": str(currentTime.timestamp()),
                            "timeLeft": int(tmp_timeLeft),
                            "estTimeUp": str(estTimeUp)
                        }))  
                        
            time.sleep(10) 
			
    def loadStats(self):
        
        try:
        
            with open("data.json") as f:
                self.stats = json.load(f)
        except FileNotFoundError:
            pass
        
    def saveStats(self):
        
        with open('data.json', 'w') as f:
            json.dump(self.stats, f, indent=4)
        
    def on_message(self, frame):
        headers, message_raw = frame.headers, frame.body
        parsed_body = json.loads(message_raw)

        for curmsg in parsed_body:
            
            if "CA_MSG" in curmsg:
                CA_MSG=curmsg["CA_MSG"]
                area_id=CA_MSG["area_id"]
                
                msg_type=CA_MSG["msg_type"]
                
                if msg_type=="CA" and area_id=="BP":
                    to=CA_MSG["to"]
                    # Poole to Waymouth
                    # "5253",
                    # "5261",
                    # Weymouth to poole.
                    # "5264",
                    # "5268",
                    brlist=["5253", "5261", "5264", "5268"]
                    
                    if to in brlist:
                        # train detected in watch zone
                        train=CA_MSG["descr"]
                
                        if not train in self.trainList:
                            print("Train detected "+train)
                            self.trainList.append(train) 
                
            if "SF_MSG" in curmsg:
                SF_MSG=curmsg["SF_MSG"]
                
                msg_type=SF_MSG["msg_type"]
                area_id=SF_MSG["area_id"]
                
                # SF - Signalling Update 
                # BP - https://wiki.openraildata.com/index.php?title=BP
                if msg_type=="SF" and area_id=="BP":

                    # Address of the signalling data being updated. 
                    # For an SF_MSG, this is the individual signalling element.
                    # For an SG_MSG or SH_MSG, this is the starting address for the four bytes supplied in the data field. 
                    address=int(SF_MSG["address"], 16)
                    data=int(SF_MSG["data"], 16)
                    
                    # from:
                    # https://wiki.openraildata.com/index.php?title=BP
                    # Text file attached
                    #                
                    #008   0   SPW5261BR
                    #      1   SPW5271
                    #      2   SPW5270
                    #      3   SPW5269
                    #      4   SPW5268
                    #      5   SPW5267
                    #      6   SPW5264
                    #      7   SPW5261
                    #009   0   SPW5665
                    #      1   SPW5663
                    #      2   SPW5659
                    #      3   SPW5260
                    #      4   SPW5255
                    #      5   SPW5264BR
                    
                    # so we need address 8
                    # and we want 5261, 5261BR and 5264
                    if address==8:
                        
                        if (data & 0x80):
                            self.SPW5261=True
                        else:
                            self.SPW5261=False
                    
                        if (data & 0x40):
                            self.SPW5264=True
                        else:
                            self.SPW5264=False

                        if (data & 0x1):
                            self.SPW5261BR=True  
                        else:
                            self.SPW5261BR=False
                            
                    # so we need address 9
                    # and we want SPW5264BR
                    if address==9:
                        
                        if (data & 0x20):
                            self.SPW5264BR=True
                        else:
                            self.SPW5264BR=False
                    
                    print("SPW5261: "+str(self.SPW5261)+", SPW5261BR: "+str(self.SPW5261BR)+
                          ", SPW5264: "+str(self.SPW5264)+", SPW5264BR: "+str(self.SPW5264BR))
      
                    newDown=False
                    if (self.SPW5261 or self.SPW5264 or self.SPW5261BR or self.SPW5264BR):
                        newDown=True
                        
                    currentTime=datetime.now()
                        
                    if self.down != newDown:
                        # change detected
                        print("Change detected....")
                        
                        # if up, 
                        if newDown==False and self.changeTime!=None:
                            
                            downLength= currentTime - self.changeTime
                            
                            downTime=int(downLength.total_seconds())
                            
                            errorTime=self.avgTime - downTime
                        
                            
                            print("Barrier down for "+str(downTime)+", avgTime: "+str(self.avgTime)+
                                        ", error: "+str(errorTime)+", trains: "+str(self.trainList))
                            
                            # add to list
                            self.results.append({
                                    "timeStamp": str(currentTime.timestamp()),
                                    "trainList": self.trainList,
                                    "downTime": downTime
                                })
                            
                            # for each train we have seen...                            
                            for curTrain in self.trainList:
                                
                                newStat={}
                                newStat["totalTime"]=0
                                newStat["statCount"]=0
                                newStat["downTimes"]=[]
                                
                                if curTrain in self.stats:
                                    newStat=self.stats[curTrain]
                            
                                # calculate the updated stats
                                newStat["totalTime"]=newStat["totalTime"]+downTime
                                newStat["statCount"]=newStat["statCount"]+1
                                newStat["downTimes"].append({
                                        "downTime": int(downTime),
                                        "timeStamp": str(currentTime.timestamp())
                                    })                                
                                self.stats[curTrain]=newStat
                                
                            # on state change, clear active list
                            self.trainList=[]
                        
                            # save the stats
                            self.saveStats()
                        

                                

                        self.down= newDown;
                        self.changeTime=currentTime

                    if newDown==True:
                        # just gone down, so how long will it be down for 
                        self.avgTime=-1
                        
                        for curTrain in self.trainList:
                            print("looking for train "+str(curTrain))
                            if curTrain in self.stats:
                                thisStat = self.stats[curTrain]
                                print("train "+str(thisStat))
                                
                                thisAvgTime=thisStat["totalTime"]/thisStat["statCount"]

                                if thisAvgTime>self.avgTime:
                                    self.avgTime=int(thisAvgTime)
                                    
                        print("Average Time: "+str(self.avgTime))
                        
                    # republish the current state, we always do this, even on no change
                    # to make sure all are updated about it
                    self.mqttclient.publish("traintrack/woolcrossing", str(self.down).lower())       
                        
                    
                    # and publish more detailed stats
                    if self.down and self.avgTime>0:
                        
                        # get the time between now and when the state changed, 
                        # then subtract it from how long we expect the state to remain. 
                        diffTime=(currentTime-self.changeTime).total_seconds()
                        tmp_timeLeft=self.avgTime - diffTime
                        
                        print("Time left: "+str(tmp_timeLeft))
                        
                        self.downState={"timestamp": currentTime,
                        				"timeLeft": tmp_timeLeft}

                        
                        # and publish the message
                        #self.mqttclient.publish("traintrack/timeToOpen", json.dumps(
                        #                        {
                        #                            "timeStamp": str(currentTime.timestamp()),
                        #                            "timeLeft": int(tmp_timeLeft)
                        #                        }))
                    else:
                     	self.downState = None
                     	
      
        if self.is_durable:
            # Acknowledging messages is important in client-individual mode
            self._mq.ack(id=headers["ack"], subscription=headers["subscription"])


    def on_error(self, frame):
        print('received an error {}'.format(frame.body))

    def on_disconnected(self):
        print('disconnected, reconnecting')
        
        connected=False
        
        while connected==False:
        
            try:
		    	# Connect to feed
                self.connect()
                connected=True
            except stomp.exception.ConnectFailedException as e:
                print(e)
                time.sleep(120)
                pass
			

    def connect(self):
        print("ON CONNECT")
        self._mq.connect(self.settings["username"], self.settings["password"], wait=True)
        # Determine topic to subscribe
        topic = "/topic/TD_WESS_SIG_AREA"
    
        # Subscription
        subscribe_headers = {
            "destination": topic,
            "id": 1,
        }
        
        subscribe_headers["ack"] = "auto"
    
        self._mq.subscribe(**subscribe_headers)


if __name__ == "__main__":
    

    # load in the settings
    with open("settings.yaml") as f:
        settings = yaml.safe_load(f)
        
    # https://stomp.github.io/stomp-specification-1.2.html#Heart-beating
    # We're committing to sending and accepting heartbeats every 5000ms
    connection = stomp.Connection([('publicdatafeeds.networkrail.co.uk', 61618)], keepalive=True, heartbeats=(5000, 5000))
    
    lst=Listener(connection, settings)
    
    connection.set_listener('', lst)
    

    # and connect
    lst.on_disconnected()

    while True:
        sleep(10)

