import json
import time
import threading
import paho.mqtt.client as mqtt

BROKER_IP="localhost"
BROKER_PORT=1883
RHT_FORWARD_N_MSG=5

chamber_default_readings="{\"HI\": null, \"TI\": null, \"HE\": null, \"TE\": null, \"MC\": 0, \"MV\": 0, \"MP\": 0}"
chamber_default_regulator="{\"SP\": 0, \"HI\": 0, \"EN\": null}"
lab_default_readings="{\"HL\": null, \"TL\": null}"

class Device_counter:
    def __init__(self):
        self.counter=0

    def increment(self)->bool:
        self.counter+=1
        if self.counter>=RHT_FORWARD_N_MSG:
            self.counter=0
            return True
        return False

class Support:
    def __init__(self):
        self.chambers={}
        self.labs={}
        self.mutex=threading.Lock()
        self.client=mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect=self.__on_connect_cb

        self.client.connect(host=BROKER_IP, port=BROKER_PORT, keepalive=60)
        self.client.loop_start()

    def __on_connect_cb(self,client, userdata, flags, reason_code, properties):
        if reason_code==0:
            topic="chambers/+/misc/conn_stat"
            self.client.subscribe(topic)
            self.client.message_callback_add(sub=topic,callback=self.__on_chamber_CS_cb)
    
            topic="chambers/+/readings"
            self.client.subscribe(topic)
            self.client.message_callback_add(sub=topic,callback=self.__on_chamber_readings_cb)

            topic="lab/+/misc/conn_stat"
            self.client.subscribe(topic)
            self.client.message_callback_add(sub=topic,callback=self.__on_lab_CS_cb)
    
            topic="lab/+/readings"
            self.client.subscribe(topic)
            self.client.message_callback_add(sub=topic,callback=self.__on_lab_readings_cb)

    def __on_chamber_CS_cb(self, client, userdata, msg):
        try:
            id=int(msg.topic.split('/')[1])
            payload=json.loads(msg.payload.decode("utf-8"))
            status=payload.get("CS","")

            clean=False
            with self.mutex:
                if status=="Offline":
                    clean=True
                    if id in self.chambers:
                        del self.chambers[id]

                elif status=="Online":
                    if id not in self.chambers:
                        self.chambers[id]=Device_counter()

            if clean:
                self.client.publish(topic=f"chambers/{id}/readings",payload=chamber_default_readings,retain=False)
                self.client.publish(topic=f"chambers/{id}/RHT_graph",payload=chamber_default_readings,retain=False)
                self.client.publish(topic=f"chambers/{id}/regulator",payload=chamber_default_regulator,retain=False)

        except Exception as e:
            print(f"Exception on_chamb_CS: {e}")

    def __on_chamber_readings_cb(self,client,userdata,msg):
        try:
            forward=False
            with self.mutex:
                id=int(msg.topic.split('/')[1])
                if id not in self.chambers:
                    self.chambers[id]=Device_counter()
                forward=self.chambers[id].increment()

            if forward:
                payload:dict=json.loads(msg.payload)
                forward_payload:dict={}
                forward_payload["HI"]=round(payload.get("HI", 0), 2)
                forward_payload["TI"]=round(payload.get("TI", 0), 2)
                forward_payload["HE"]=round(payload.get("HE", 0), 2)
                forward_payload["TE"]=round(payload.get("TE", 0), 2)
                self.client.publish(topic=f"chambers/{id}/RHT_graph",payload=json.dumps(forward_payload),retain=False)

        except Exception as e:
            print(f"Exception on_chamber_readings: {e}")

    def __on_lab_CS_cb(self, client, userdata, msg):
        try:
            id=int(msg.topic.split('/')[1])
            payload=json.loads(msg.payload.decode("utf-8"))
            status=payload.get("CS","")

            clean=False
            with self.mutex:
                if status=="Offline":
                    clean=True
                    if id in self.labs:
                        del self.labs[id]

                elif status=="Online":
                    if id not in self.labs:
                        self.labs[id]=Device_counter()

            if clean:
                self.client.publish(topic=f"lab/{id}/readings",payload=lab_default_readings,retain=False)
                self.client.publish(topic=f"lab/{id}/RHT_graph",payload=lab_default_readings,retain=False)

        except Exception as e:
            print(f"Exception on_lab_CS: {e}")

    def __on_lab_readings_cb(self,client,userdata,msg):
        try:
            forward=False
            with self.mutex:
                id=int(msg.topic.split('/')[1])
                if id not in self.labs:
                    self.labs[id]=Device_counter()
                forward=self.labs[id].increment()

            if forward:
                payload:dict=json.dumps(msg.payload)
                forward_payload:dict={}
                forward_payload["HL"]=round(payload.get(key="HL", default=0), 2)
                forward_payload["TL"]=round(payload.get(key="TL", default=0), 2)
                self.client.publish(topic=f"lab/{id}/RHT_graph",payload=json.dumps(forward_payload),retain=False)

        except Exception as e:
            print(f"Exception on_lab_readings: {e}")

    def finish(self):
        self.client.loop_stop()
        self.client.disconnect()

if __name__=="__main__":
    try:
        service=Support()

        while True:
            time.sleep(10)

    except KeyboardInterrupt:
        pass
    finally:
        service.finish()