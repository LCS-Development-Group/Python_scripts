import paho.mqtt.client as mqtt
import json
import time
from pathlib import Path
import re
from dataclasses import dataclass
import csv
from datetime import datetime

BROKER="LCSRP5"
PORT=1883
CHAMBER_NUM=10
LOGGER_SETTINGS_TOPIC="chambers/+/logger/set"
READINGS_TOPIC="chambers/+/readings"
REGULATOR_TOPIC="chambers/+/regulator/get"

CSV_HEADER_NO_REG=["Num.", "Timestamp [s] (since ","RH_int [%]","T_int [°C]","RH_ext [%]","T_ext [°C]","I_memb [A]","U_memb [V]","P_memb [W]"]
CSV_HEADER_REG=["Set RH [%]"," Hist. [%]"]
csv_line_no_reg=[None]*len(CSV_HEADER_NO_REG)
csv_line_reg=[None]*(len(CSV_HEADER_REG)+len(CSV_HEADER_NO_REG))


@dataclass
class logger_t:
    dev_id: int
    topic_receive: str
    topic_send: str
    state: bool=False
    prev_state: bool=False
    include_reg: bool=False
    reg_sp: float=0.0
    reg_H: float=0.0
    file_handle=None
    csv_writer=None
    filename: str=r"D:/Desktop/test/def.csv"
    max_records: int=10
    save_interval: int=1
    save_counter: int=1
    counter: int=0
    restarts: int=0
    timestamp_start: float=0.0

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
    path=Path(loggers[dev_id].filename).resolve()
    stem=path.stem
    suffix=path.suffix
    directory=path.parent

    print("opening new file")
    directory.mkdir(parents=True, exist_ok=True)

    if loggers[dev_id].restarts > 0:
        new_path=directory / f"{stem}_{loggers[dev_id].restarts}{suffix}"
    else:
        match = re.search(r"(.*)\((\d+)\)$", stem)
        if match:
            base_stem=match.group(1).strip()
            counter=int(match.group(2))+1
        else:
            base_stem=stem
            counter=1

        new_path=path
        while new_path.exists():
            new_path=directory / f"{base_stem}({counter}){suffix}"
            counter+=1

    if loggers[dev_id].file_handle:
        loggers[dev_id].file_handle.close()
        
    loggers[dev_id].file_handle=open(new_path, "a", newline='', encoding="utf-8-sig")
    loggers[dev_id].csv_writer=csv.writer(loggers[dev_id].file_handle)
    
    #header
    loggers[dev_id].timestamp_start=round(time.time(), 3)
    header=list(CSV_HEADER_NO_REG)
    header[1]=f"{header[1]}{loggers[dev_id].timestamp_start}"

    if loggers[dev_id].include_reg==True:
        loggers[dev_id].csv_writer.writerow(header+CSV_HEADER_REG)
        
    elif loggers[dev_id].include_reg==False:
        loggers[dev_id].csv_writer.writerow(header)
        
    loggers[dev_id].file_handle.flush()

    print(f"Ch_{dev_id} logging to: {new_path}")
    return new_path.as_posix()

        

def logger_msg_received_callback(client, userdata, message):
    try:
        split_topic=message.topic.split('/')
        dev_id=int(split_topic[1])
        payload=json.loads(message.payload.decode())
        #print(f"update for ch_{dev_id}: {payload}")

        if "FN" in payload:
            loggers[dev_id].filename=payload["FN"]
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
            "MR": loggers[dev_id].max_records,
            "SI": loggers[dev_id].save_interval,
            "EN": "ON" if loggers[dev_id].state==True else "OFF",
            "IR": "ON" if loggers[dev_id].include_reg==True else "OFF",
        }
       
        #print(f"responded to {loggers[dev_id].topic_send}:  {json.dumps(logger_json)}")

        result=client.publish(loggers[dev_id].topic_send, json.dumps(logger_json))
        
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

        if loggers[dev_id].state==True:
            if loggers[dev_id].save_counter>=loggers[dev_id].save_interval:
                loggers[dev_id].save_counter=1

                readings=json.loads(message.payload.decode())
                csv_line_no_reg=[loggers[dev_id].counter+1, round(time.time()-loggers[dev_id].timestamp_start, 3)]+list(readings.values())

                if loggers[dev_id].counter>=loggers[dev_id].max_records:
                    loggers[dev_id].restarts+=1    
                    loggers[dev_id].counter=0
                    open_new_file(dev_id)
                
                if loggers[dev_id].include_reg==False:
                    loggers[dev_id].csv_writer.writerow(csv_line_no_reg)
                else:
                    loggers[dev_id].csv_writer.writerow(csv_line_no_reg+csv_line_reg)

                loggers[dev_id].file_handle.flush()
                loggers[dev_id].counter+=1
                #print(f"saved {time.time()}")

            else:
                loggers[dev_id].save_counter+=1

    except Exception as e:
        print(f"sav_cb except: {e}")

def reg_msg_received_callback(client, usedata, message):
    try:
        split_topic=message.topic.split('/')
        dev_id=int(split_topic[1])

        payload=json.loads(message.payload.decode())
        if "SP" in payload:
            loggers[dev_id].reg_sp=float(payload['SP'])
            csv_line_reg[0]=loggers[dev_id].reg_sp
        if "HI" in payload:
            loggers[dev_id].reg_H=float(payload['HI'])
            csv_line_reg[1]=loggers[dev_id].reg_H

    except Exception as e:
        print(f"reg_cb except: {e}")


#MQTT config
try:
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.message_callback_add(LOGGER_SETTINGS_TOPIC, logger_msg_received_callback)
    client.message_callback_add(REGULATOR_TOPIC, reg_msg_received_callback)
    client.message_callback_add(READINGS_TOPIC, saver_msg_received_callback)

    client.connect(BROKER, PORT, 60)
    print(f"Connected client to {BROKER}...")
    
    
    client.subscribe(LOGGER_SETTINGS_TOPIC)
    client.subscribe(REGULATOR_TOPIC)
    client.subscribe(READINGS_TOPIC)

    init_loggers(client)


    client.loop_start()
    print("start")
    while True:
        time.sleep(1)

except KeyboardInterrupt:
    print("stopping")
    client.loop_stop()
    client.disconnect()

    for dev_id in range(CHAMBER_NUM):
        if loggers[dev_id].file_handle:
            loggers[dev_id].file_handle.close()