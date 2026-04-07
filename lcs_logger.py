import paho.mqtt.client as mqtt
import json
import time
from pathlib import Path
import re
from dataclasses import dataclass
import csv
from datetime import datetime
import threading

BROKER="LCSRP5"
PORT=1883
CHAMBER_NUM=6
LOGGER_SETTINGS_TOPIC="chambers/+/logger/set"
READINGS_TOPIC="chambers/+/readings"
REGULATOR_TOPIC="chambers/+/regulator/get"

CSV_HEADER_NO_REG=["Num.", "Timestamp","RH_int [%]","T_int [°C]","RH_ext [%]","T_ext [°C]","I_memb [A]","U_memb [V]","P_memb [W]"]
CSV_HEADER_REG=["Set RH [%]"," Hist. [%]"]

pwd_prefix=r"/home/lcsuser/data_partition/LCS_CSVs"

@dataclass
class logger_t:
    dev_id: int
    topic_receive: str
    topic_send: str
    mutex: threading.Lock= None
    state: bool=False
    prev_state: bool=False
    include_reg: bool=False
    reg_sp: float=0.0
    reg_H: float=0.0
    file_handle=None
    csv_writer=None
    filename: str=r""
    file_postfix: str=r""
    max_records: int=10
    save_interval: int=1
    save_counter: int=1
    counter: int=0
    restarts: int=0

    def __post_init__(self):
        self.mutex=threading.Lock()

loggers: dict[int, logger_t]={}

def init_loggers(client):
    
    for dev_id in range(CHAMBER_NUM):
        loggers[dev_id]=logger_t(
            dev_id=dev_id,
            topic_receive=f"chambers/{dev_id}/logger/set",
            topic_send=f"chambers/{dev_id}/logger/get"
        )
        
        
        #default values
        logger_json={
            "FN": loggers[dev_id].filename,
            "MR": loggers[dev_id].max_records,
            "SI": loggers[dev_id].save_interval,
            "EN": "ON" if loggers[dev_id].state == True else "OFF",
            "IR": "ON" if loggers[dev_id].state == True else "OFF",
        }
        json_payload = json.dumps(logger_json)

        result = client.publish(topic=(loggers[dev_id].topic_send), payload=json_payload, retain=True)

        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            print(f"Failed to send message: {result.rc}")

def open_new_file(dev_id):
    mnt_path=Path(pwd_prefix).resolve()

    if loggers[dev_id].restarts==0:
        new_path=f"{datetime.now().strftime("%Y-%m-%d_%H:%M:%S")}_ch{dev_id}"

        if loggers[dev_id].file_postfix:
            new_path+=f"_{loggers[dev_id].file_postfix}"

        new_path+=".csv" 
        loggers[dev_id].filename=new_path  

    else:
        new_path=Path(loggers[dev_id].filename).stem
        new_path=re.sub(r'_\d+$', f'', new_path)
        new_path+=f"_{loggers[dev_id].restarts}.csv"

    new_path=mnt_path / new_path

    if loggers[dev_id].file_handle:
        loggers[dev_id].file_handle.close()
        
    loggers[dev_id].file_handle=open(new_path, "a", newline='', encoding="utf-8-sig")
    loggers[dev_id].csv_writer=csv.writer(loggers[dev_id].file_handle)
    
    #header
    if loggers[dev_id].include_reg==True:
        loggers[dev_id].csv_writer.writerow(CSV_HEADER_NO_REG+CSV_HEADER_REG)
        
    elif loggers[dev_id].include_reg==False:
        loggers[dev_id].csv_writer.writerow(CSV_HEADER_NO_REG)
        
    loggers[dev_id].file_handle.flush()

    return new_path.relative_to(mnt_path).as_posix()

        
def logger_msg_received_callback(client, userdata, message):
    try:
        split_topic=message.topic.split('/')
        dev_id=int(split_topic[1])
        payload=json.loads(message.payload.decode())

        with loggers[dev_id].mutex:#mutex lock

            if "FP" in payload:
                loggers[dev_id].file_postfix=payload["FP"]
                if loggers[dev_id].state==True:
                    stop_recording(dev_id)

            if "MR" in payload:
                loggers[dev_id].max_records=int(payload["MR"])

            if "SI" in payload:
                loggers[dev_id].save_interval=int(payload["SI"])

            if "IR" in payload:
                loggers[dev_id].include_reg=True if payload.get("IR")=="ON" else False
                if loggers[dev_id].state==True:
                    stop_recording(dev_id)

            if "EN" in payload:
                loggers[dev_id].prev_state=loggers[dev_id].state
                loggers[dev_id].state=True if payload.get("EN")=="ON" else False

                if loggers[dev_id].state==True:
                    if loggers[dev_id].prev_state==False:
                        start_recording(dev_id)

                elif loggers[dev_id].state==False:
                    if loggers[dev_id].prev_state==True:
                        stop_recording(dev_id)

            #confirm receiving the new settings
            logger_json={
                "FN": loggers[dev_id].filename,
                "FP": loggers[dev_id].file_postfix,
                "MR": loggers[dev_id].max_records,
                "SI": loggers[dev_id].save_interval,
                "EN": "ON" if loggers[dev_id].state==True else "OFF",
                "IR": "ON" if loggers[dev_id].include_reg==True else "OFF",
            }
        
            #print(f"responded to {loggers[dev_id].topic_send}:  {json.dumps(logger_json)}")

            result=client.publish(loggers[dev_id].topic_send, json.dumps(logger_json), retain=True)
            
            if result.rc!=mqtt.MQTT_ERR_SUCCESS:
                print(f"Failed to send message: {result.rc}")

    except Exception as e:
        print(f"cfg_cb except: {e}")
    

def start_recording(dev_id):
    loggers[dev_id].prev_state=False
    loggers[dev_id].state=True
    loggers[dev_id].restarts=0
    loggers[dev_id].counter=0
    loggers[dev_id].save_counter=1
    loggers[dev_id].filename=open_new_file(dev_id)

def stop_recording(dev_id):
    loggers[dev_id].prev_state=True
    loggers[dev_id].state=False
    if loggers[dev_id].file_handle:
        loggers[dev_id].file_handle.close()
    
def saver_msg_received_callback(client, userdata, message):
    try:
        split_topic=message.topic.split('/')
        dev_id=int(split_topic[1])
        logger=loggers[dev_id]
        with logger.mutex:#mutex lock
            if logger.state==True:
                if logger.save_counter>=logger.save_interval:
                    logger.save_counter=1

                    readings=json.loads(message.payload.decode())
                    csv_line_no_reg=[logger.counter+1, datetime.now().strftime("%H:%M:%S.%f")[:-3]]+list(readings.values())

                    if logger.counter>=logger.max_records:
                        logger.restarts+=1    
                        logger.counter=0
                        open_new_file(dev_id)
                    
                    if logger.include_reg==False:
                        logger.csv_writer.writerow(csv_line_no_reg)
                    else:
                        logger.csv_writer.writerow(csv_line_no_reg+[logger.reg_sp, logger.reg_H])
                    logger.file_handle.flush()
                    logger.counter+=1
                    #print(f"saved {time.time()}")

                else:
                    logger.save_counter+=1

    except Exception as e:
        print(f"sav_cb except: {e}")

def reg_msg_received_callback(client, usedata, message):
    try:
        split_topic=message.topic.split('/')
        dev_id=int(split_topic[1])

        payload=json.loads(message.payload.decode())

        with loggers[dev_id].mutex:
            if "SP" in payload:
                loggers[dev_id].reg_sp=float(payload['SP'])
            if "HI" in payload:
                loggers[dev_id].reg_H=float(payload['HI'])

    except Exception as e:
        print(f"reg_cb except: {e}")


#MQTT config
try:
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.message_callback_add(LOGGER_SETTINGS_TOPIC, logger_msg_received_callback)
    client.message_callback_add(REGULATOR_TOPIC, reg_msg_received_callback)
    client.message_callback_add(READINGS_TOPIC, saver_msg_received_callback)

    client.connect(BROKER, PORT, 60)
    #print(f"Connected client to {BROKER}...")
    
    
    client.subscribe(LOGGER_SETTINGS_TOPIC)
    client.subscribe(REGULATOR_TOPIC)
    client.subscribe(READINGS_TOPIC)

    init_loggers(client)


    client.loop_start()
    #print("start")
    while True:
        time.sleep(1)

except (KeyboardInterrupt, SystemExit):
    raise

except Exception as e:
    print(f"Fatal error: {e}")
    
finally:
    #print("stopping")
    client.loop_stop()
    client.disconnect()

    for dev_id in range(CHAMBER_NUM):

        logger_json={
            "FN": loggers[dev_id].filename,
            "MR": loggers[dev_id].max_records,
            "SI": loggers[dev_id].save_interval,
            "EN": "OFF",
            "IR": "OFF",
        }
        result=client.publish(loggers[dev_id].topic_send, json.dumps(logger_json), retain=True)
        if result.rc!=mqtt.MQTT_ERR_SUCCESS:
            print(f"Failed to send message: {result.rc}")

        if loggers[dev_id].file_handle:
            loggers[dev_id].file_handle.close()
