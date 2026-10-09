import paho.mqtt.client as mqtt
import json
import sys
import time
import threading
from datetime import datetime
from pathlib import Path

BROKER_IP="LCSRP5"
BROKER_PORT=1883
CHAMBER_DISCOVERY_TIME=2
BASE_DIR=Path("/home/lcsuser/data_partition/LCS_CSVs")

chamber_default_readings={"HI": None, "TI": None, "HE": None, "TE": None, "MC": 0.0, "MV": 0.0, "MP": 0.0}
chamber_default_regulator={"SP": 0.0, "HI": 0.0, "EN": None}
lab_default_readings={"HL": None, "TL": None}

def get_readings_topic(id:int)->str:
    return f"chambers/{id}/readings"

def get_regulator_get_topic(id:int)->str:
    return f"chambers/{id}/regulator/get"


class Chamber_profile:
    def __init__(self, chamber_id:int=0, is_lab:bool=False):
        self.file_name:str="None"
        self.__file_name_base:str=""
        self.__file_path:Path=None
        self.is_lab=is_lab

        #logger settings (from MQTT)
        self.incl_RHT_chamber=False
        self.incl_RHT_aux=False
        self.incl_REG=False
        self.incl_CURSEN=False
        self.enabled=False
        self.max_records=10000
        self.save_interval=1
        self.file_postfix:str=""

        #regulator
        self.reg_H=None
        self.reg_SP=None

        #internal
        self.__record_index=0
        self.__file_index=0
        self.__reading_counter=0

        if self.is_lab:
            self.logger_confirm_topic=f"lab/logger/get"
        else:
            self.chamber_id=chamber_id
            self.logger_confirm_topic=f"chambers/{chamber_id}/logger/get"

        self.mutex=threading.Lock()
        BASE_DIR.mkdir(parents=True, exist_ok=True)

    def update_settings(self, settings:dict):
        with self.mutex:
            if self.is_lab:
                self.file_postfix=settings.get("FP", self.file_postfix)
                self.save_interval=settings.get("SI", self.save_interval)
            else:
                self.file_postfix=settings.get("FP", self.file_postfix)
                self.incl_REG=self.__get_TF(settings.get("IR", self.incl_REG))
                self.incl_RHT_chamber=self.__get_TF(settings.get("IC", self.incl_RHT_chamber))
                self.incl_RHT_aux=self.__get_TF(settings.get("IA", self.incl_RHT_aux))
                self.incl_CURSEN=self.__get_TF(settings.get("IM", self.incl_CURSEN))
                self.save_interval=settings.get("SI", self.save_interval)

            if self.save_interval<=0:
                self.save_interval=1
                        
            self.max_records=settings.get("MR", self.max_records)
            if self.max_records<=0:
                self.max_records=1
            
            self.enabled=self.__get_TF(settings.get("EN", False)) #change in any above should disable the logger
            self.__update_save_process()

    def __generate_filename(self):
        self.file_name=self.__file_name_base
        if self.__file_index>0:
            self.file_name+=f"_{self.__file_index}"
        self.file_name+=".csv"
        self.__file_path=BASE_DIR/self.file_name

    def __update_save_process(self):
        self.__record_index=0
        self.__file_index=0
        self.__reading_counter=0

        if self.enabled:
            #create filename base
            timestamp=datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            if self.is_lab:
                self.__file_name_base=f"{timestamp}_LAB142"
            else:
                self.__file_name_base=f"{timestamp}_CH{self.chamber_id}"

            postfix=(self.file_postfix or "").replace(" ","_")
            if postfix:
                self.__file_name_base+=f"_{postfix}"
            self.__generate_filename()
        else:
            #self.file_name=""
            self.__file_name_base=""

    def get_settings(self)->dict:
        with self.mutex:
            if self.is_lab:
                return{
                    "EN":self.__get_ONOFF(self.enabled),
                    "FP":self.file_postfix,
                    "FN":self.file_name,
                    "SI":self.save_interval,
                    "MR":self.max_records
                }
            else:
                return{
                    "EN":self.__get_ONOFF(self.enabled),
                    "FP":self.file_postfix,
                    "FN":self.file_name,
                    "IR":self.__get_ONOFF(self.incl_REG),
                    "IC":self.__get_ONOFF(self.incl_RHT_chamber),
                    "IA":self.__get_ONOFF(self.incl_RHT_aux),
                    "IM":self.__get_ONOFF(self.incl_CURSEN),
                    "SI":self.save_interval,
                    "MR":self.max_records
                }

    def __get_ONOFF(self, value:bool=False)->str|None:
        if value is None:
            return None
        if value:
            return "ON"
        else:
            return "OFF"

    def __get_TF(self, value:str="OFF")->bool|None:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if value=="OFF":
            return False
        elif value=="ON":
            return True
        return None
    
    def update_regulator(self, regulator:dict):
        with self.mutex:
            self.reg_H=regulator.get("HI",self.reg_H)
            self.reg_SP=regulator.get("SP", self.reg_SP)


    def save_data(self, readings:dict)->bool:
        '''saves readings to apropriate CSV if aplicable (otherwise no-op). 
        Returns True if sending logger_settings to MQTT is required.'''
        file_change=False
        with self.mutex:
            if self.enabled:

                #check save_interval
                if self.__reading_counter!=(self.save_interval-1):
                    self.__reading_counter+=1
                    return file_change
                else:
                    self.__reading_counter=0

                    #check if the file is not filled
                    if self.__record_index==self.max_records:
                        self.__record_index=0
                        self.__file_index+=1

                    if self.__record_index==0:
                        self.__generate_filename()
                        file_change=True

                    #open the file
                    with open(self.__file_path, "a") as file:
                        if file_change:
                            file.write(self.__get_header())

                        file.write(self.__get_readings(readings))
                        self.__record_index+=1
        return file_change

    def __get_header(self)->str:
        header="No.;Timestamp;"

        if self.is_lab:
            header+="Lab RH [%];Lab T [°C];"
        else:
            if self.incl_RHT_chamber:
                header+="Chamb. RH [%]; Chamb. T [°C];"
            if self.incl_RHT_aux:
                header+="Aux. RH [%]; Aux. T [°C];"
            if self.incl_CURSEN:
                header+="Memb. C [A];Memb. V [V];Memb. P [W];"
            if self.incl_REG:
                header+="Set RH [%]; Hist. [%];"
        return header.rstrip(";")+"\n"

    def __get_readings(self, readings:dict)->str:
        line=f"{self.__record_index+1};{datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]};"
        if self.is_lab:
            line+=f"{readings.get("HL")};{readings.get("TL")};"
        else:
            if self.incl_RHT_chamber:
                line+=f"{readings.get("HI")};{readings.get("TI")};"
            if self.incl_RHT_aux:
                line+=f"{readings.get("HE")};{readings.get("TE")};"
            if self.incl_CURSEN:
                line+=f"{readings.get("MC")};{readings.get("MV")};{readings.get("MP")};"
            if self.incl_REG:
                line+=f"{self.reg_SP};{self.reg_H};"
        return line.rstrip(";")+"\n"

class Logger_controler:
    def __init__(self):
        self.chambers={}
        self.lab=Chamber_profile(is_lab=True)
        self.client=mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self.normal_ops_flg=False
        self.connect_event=threading.Event()
        self.client.on_connect=self.__on_connect_cb

    def __on_readings_cb(self, client, userdata, msg):
        try:
            id=int(msg.topic.split('/')[1])
            chamb:Chamber_profile=self.chambers.get(id, None)
            if chamb is not None:
                if chamb.save_data(json.loads(msg.payload.decode("utf-8"))):
                    #confirmation to MQTT (if needed)
                    payload=chamb.get_settings()
                    self.client.publish(topic=chamb.logger_confirm_topic, payload=json.dumps(payload), retain=True)

        except Exception as e:
            print(f"Debug - on_readings: {e}")
            return

    def __on_lab_readings_cb(self, client, userdata, msg):
        try:
            if self.lab.save_data(json.loads(msg.payload.decode("utf-8"))):
                #confirmation to MQTT (if needed)
                payload=self.lab.get_settings()
                self.client.publish(topic=self.lab.logger_confirm_topic, payload=json.dumps(payload), retain=True)

        except Exception as e:
            print(f"Debug - on_lab_readings: {e}")
            return

    def __on_lab_log_set_cb(self, client, userdata, msg):
        try:
            payload=json.loads(msg.payload.decode("utf-8"))
            self.lab.update_settings(payload)

            #confirmation
            payload=self.lab.get_settings()
            self.client.publish(topic=self.lab.logger_confirm_topic, payload=json.dumps(payload), retain=True)

        except Exception as e:
            print(f"Debug - on_lab_log_set: {e}")
            return

    def __on_reg_get_cb(self, client, userdata, msg):
        try:
            id=int(msg.topic.split('/')[1])
            chamb=self.chambers.get(id, None)
            if chamb is not None:
            
                payload=json.loads(msg.payload.decode("utf-8"))
                chamb.update_regulator(payload)

                #no confirm
        
        except Exception as e:
            print(f"Debug - on_reg_get: {e}")
            return

    def __on_log_set_cb(self, client, userdata, msg):
        try:
            id=int(msg.topic.split('/')[1])
            chamb=self.chambers.get(id, None)
            if chamb is not None:

                payload=json.loads(msg.payload.decode("utf-8"))
                chamb.update_settings(payload)

                #confirmation
                payload=chamb.get_settings()
                self.client.publish(topic=chamb.logger_confirm_topic, payload=json.dumps(payload), retain=True)

            elif chamb is None:
                chamb=Chamber_profile(id)
                self.chambers[id]=chamb

        except Exception as e:
            print(f"Debug - on_log_set: {e}")
            return

    def __on_chamb_offline(self, client, userdata, msg):
        try:
            id=int(msg.topic.split('/')[1])
            chamb=self.chambers.get(id, None)
            if chamb is not None:
                payload=json.loads(msg.payload.decode("utf-8"))

                status=payload.get("CS", " ")
                if status=="Offline":
                    #cleaning the topics
                    self.client.publish(topic=f"chambers/{id}/readings" , payload=json.dumps(chamber_default_readings), retain=False)
                    self.client.publish(topic=f"chambers/{id}/RHT_graph", payload=json.dumps(chamber_default_readings), retain=False)
                    self.client.publish(topic=f"chambers/{id}/regulator", payload=json.dumps(chamber_default_regulator), retain=False)

        except Exception as e:
            print(f"Debug - on_chamb_offline: {e}")
            return
        
    def __on_lab_offline(self, client, userdata, msg):
        try:
            payload=json.loads(msg.payload.decode("utf-8"))

            status=payload.get("CS", " ")
            if status=="Offline":
                #cleaning the topics
                self.client.publish(topic=f"lab/readings" , payload=json.dumps(lab_default_readings), retain=False)
                self.client.publish(topic=f"lab/RHT_graph", payload=json.dumps(lab_default_readings), retain=False)

        except Exception as e:
            print(f"Debug - on_lab_offline: {e}")
            return

    def __discover_lab_cb(self, client, userdata, msg):
        '''loads retained settings at script startup'''
        try:
            payload=json.loads(msg.payload.decode("utf-8"))
            self.lab.update_settings(payload)

        except Exception as e:
            print(f"Debug - dicovery: {e}")
            return

    def __discover_chambers_cb(self, client, userdata, msg):
        try:
            id=int(msg.topic.split('/')[1])
            if id not in self.chambers:
                chamb=Chamber_profile(id)

                payload=json.loads(msg.payload.decode("utf-8"))
                chamb.update_settings(payload)

                self.chambers[id]=chamb

        except Exception as e:
            print(f"Debug - dicovery: {e}")
            return

    def __on_connect_cb(self, client, userdata, flags, reason_code, properties):
        if self.normal_ops_flg:
            self.start_normal_operations()
        else:
            self.connect_event.set()

    def connect(self):
        self.client.connect(host=BROKER_IP, port=BROKER_PORT, keepalive=60)
        self.client.loop_start()

    def finish(self):
        self.client.loop_stop()
        self.client.disconnect()

    def discover_chambers(self, timeout:int=2):
        self.connect_event.wait()

        discover_topic="chambers/+/logger/get"
        lab_get_topic="lab/logger/get"

        self.client.subscribe(discover_topic)  
        self.client.message_callback_add(sub=discover_topic, callback=self.__discover_chambers_cb)

        self.client.subscribe(lab_get_topic)  
        self.client.message_callback_add(sub=lab_get_topic, callback=self.__discover_lab_cb)

        time.sleep(timeout)

        self.client.unsubscribe(discover_topic)
        self.client.message_callback_remove(sub=discover_topic)

        self.client.unsubscribe(lab_get_topic)
        self.client.message_callback_remove(sub=lab_get_topic)

    def start_normal_operations(self):
        self.normal_ops_flg=True
        self.connect_event=None

        self.client.subscribe("chambers/+/readings")    
        self.client.message_callback_add(sub="chambers/+/readings", callback=self.__on_readings_cb)
        self.client.subscribe("chambers/+/regulator/get")
        self.client.message_callback_add(sub="chambers/+/regulator/get", callback=self.__on_reg_get_cb)
        self.client.subscribe("chambers/+/logger/set")
        self.client.message_callback_add(sub="chambers/+/logger/set", callback=self.__on_log_set_cb)
        self.client.subscribe("chambers/+/misc/conn_stat")
        self.client.message_callback_add(sub="chambers/+/misc/conn_stat", callback=self.__on_chamb_offline)

        self.client.subscribe("lab/logger/set")
        self.client.message_callback_add(sub="lab/logger/set", callback=self.__on_lab_log_set_cb)
        self.client.subscribe("lab/readings")
        self.client.message_callback_add(sub="lab/readings", callback=self.__on_lab_readings_cb)
        self.client.subscribe("lab/misc/conn_stat")
        self.client.message_callback_add(sub="lab/misc/conn_stat", callback=self.__on_lab_offline)

if __name__=="__main__":
    try:
        ctrl=Logger_controler()
        ctrl.connect()
        ctrl.discover_chambers(timeout=CHAMBER_DISCOVERY_TIME)
        ctrl.start_normal_operations()

        while True:
            time.sleep(10)

    except KeyboardInterrupt:
        pass

    finally:
        ctrl.finish()

