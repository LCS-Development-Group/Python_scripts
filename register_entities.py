import paho.mqtt.client as mqtt
import json
import sys
import time

BROKER="LCSRP5"
PORT=1883

sensor_tmpl={
    "sen": [
        ("T_int",       "TI", "\u00b0C"),
        ("RH_int",      "HI", "%"),
        ("T_ext",       "TE", "\u00b0C"),
        ("RH_ext",      "HE", "%"),
        ("Memb_cur",    "MC", "A"),
        ("Memb_vol",    "MV", "V"),
        ("Memb_pow",    "MP", "W")],
    "log": [
        ("File_name",   "FN", "")],
}
chamber_default_readings={"HI": None, "TI": None, "HE": None, "TE": None, "MC": 0.0, "MV": 0.0, "MP": 0.0}
chamber_default_regulator={"SP": 0.0, "HI": 0.0, "EN": None}
chamber_default_logger={"EN": "OFF", "FP": "", "FN": "", "IR": "OFF", "IC": "OFF", "IA": "OFF", "IM": "OFF", "SI": 1, "MR": 10000}
chamber_default_nickname={"CN": "None"}
chamber_default_connstat={"CS": "Offline"}

numbers_tmpl={
    "reg": [
        ("SP","SP", "%", 0, 100, 0.5),
        ("Hist","HI", "%", 0, 20, 0.1)],
    "log": [
        ("logger_max_records","MR", "", "5000", "50000", "5000"),
        ("logger_save_interval","SI", "s", "1", "60", "1")]
}

switch_tmpl={
    "reg": [
        ("reg_en","EN")],
    "log": [
        ("logger_state","EN"),
        ("include_regulator","IR"),
        ("include_chamber","IC"),
        ("include_aux","IA"),
        ("include_membrane","IM")]
}

text_tmpl={
    "log": [
        ("logger_filepostfix", "FP")],
}

MISC_chamber_nick=("chamber_nick", "CN")
MISC_conn_status=("Conn_status", "CS")


lab_sensor_tmpl={
    "sen": [
    ("T_room",       "TL", "\u00b0C"),
    ("RH_room",      "HL", "%"),
    ("T_RP5",       "TR", "\u00b0C")]
}

class Registerer:
    def __init__(self):
        self.client=mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self.client.connect(host=BROKER, port=PORT)
        self.client.loop_start()

    def close(self):
        time.sleep(1)
        self.client.loop_stop()
        self.client.disconnect()

    def __send_config(self, type_t:str, payload: dict):
        cfg_t=f"homeassistant/{type_t}/{payload["unique_id"]}/config"
        self.client.publish(cfg_t, json.dumps(payload), retain=True)
        print(cfg_t)

    def __generate_sensor(self, name: str, stat_t:str, json_code: str, unique_id: str, dev:dict,
        unit:str=None, state_class:str=None)->dict:
        payload={
            "name": name,
            "stat_t": stat_t,
            "val_tpl": f"{{{{value_json.{json_code}}}}}",
            "unique_id": unique_id,
            "dev": dev}
        if unit is not None:
            payload["unit_of_meas"]=unit
        if state_class is not None:
            payload["state_class"]=state_class
        return payload

    def __generate_number(self, name: str, stat_t:str, cmd_t:str, json_code: str, unique_id: str, dev:dict,
        unit:str=None, min:float=None, max:float=None, step:float=None)->dict:
        payload={
            "name": name, 
            "stat_t": stat_t, 
            "cmd_t": cmd_t, 
            "unique_id": unique_id, 
            "dev": dev, 
            "val_tpl": f"{{{{value_json.{json_code}}}}}", 
            "cmd_tpl": f"{{\"{json_code}\": {{{{ value }}}} }}",
            "mode": "box"}
        if unit is not None:
            payload["unit_of_meas"]=unit
        if min is not None:
            payload["min"]=min
        if max is not None:
            payload["max"]=max
        if step is not None:
            payload["step"]=step
        return payload

    def __generate_switch(self, name: str, stat_t:str, cmd_t:str, json_code: str, unique_id: str, dev:dict)->dict:
        payload={
            "name": name, 
            "stat_t": stat_t, 
            "cmd_t": cmd_t, 
            "unique_id": unique_id, 
            "dev": dev, 
            "val_tpl": f"{{{{value_json.{json_code}}}}}", 
            "cmd_tpl": f"{{\"{json_code}\": \"{{{{ value }}}}\" }}",
            "payload_on": "ON", 
            "payload_off": "OFF"}
        return payload

    def __generate_text(self, name: str, stat_t:str, cmd_t:str, json_code: str, unique_id: str, dev:dict, retain:bool=None)->dict:
        payload={
            "name": name, 
            "stat_t": stat_t, 
            "cmd_t": cmd_t, 
            "unique_id": unique_id, 
            "dev": dev, 
            "val_tpl": f"{{{{value_json.{json_code}}}}}", 
            "cmd_tpl": f"{{\"{json_code}\": \"{{{{ value }}}}\" }}"}
        if retain is not None:
            payload["ret"]=retain

        return payload

    def register_chamber(self, chamber_id:int):
        ch_dev={"ids": [f"ch{chamber_id}"], "name": f"Chamber {chamber_id}"}
        print(f"\nRegistering Chamber {chamber_id}:")

        '''sensors'''
        stat_t=f"chambers/{chamber_id}/readings"
        stat_t2=f"chambers/{chamber_id}/RHT_graph"
        uid_pref=f"ch{chamber_id}_sen_"
        for name, code, unit in sensor_tmpl["sen"]:
            if code.startswith("M"):
                temp_t=stat_t
            else:
                temp_t=stat_t2

            self.__send_config("sensor",self.__generate_sensor(name=name, stat_t=temp_t, json_code=code, unique_id=uid_pref+code, dev=ch_dev, unit=unit, state_class="measurement"))
        
        #initial values (retained)
        payload=json.dumps(chamber_default_readings)
        self.client.publish(topic=stat_t, payload=payload, qos=0, retain=True)
        self.client.publish(topic=stat_t2, payload=payload, qos=0, retain=True)

        '''regulator'''
        stat_t=f"chambers/{chamber_id}/regulator/get"
        cmd_t=f"chambers/{chamber_id}/regulator/set"
        uid_pref=f"ch{chamber_id}_reg_"
        for name, code, unit, min, max, step in numbers_tmpl["reg"]:
            self.__send_config("number",self.__generate_number(name=name, stat_t=stat_t, cmd_t=cmd_t, json_code=code, unique_id=uid_pref+code, dev=ch_dev, unit=unit,
            min=min, max=max, step=step))

        for name, code in switch_tmpl["reg"]:
            self.__send_config("switch",self.__generate_switch(name=name, stat_t=stat_t, cmd_t=cmd_t, json_code=code, unique_id=uid_pref+code, dev=ch_dev))        

        #initial values (retained)
        self.client.publish(topic=stat_t, payload=json.dumps(chamber_default_regulator), qos=0, retain=True)

        '''logger'''
        stat_t=f"chambers/{chamber_id}/logger/get"
        cmd_t=f"chambers/{chamber_id}/logger/set"
        uid_pref=f"ch{chamber_id}_log_"
        for name, code in switch_tmpl["log"]:
            self.__send_config("switch",self.__generate_switch(name=name, stat_t=stat_t, cmd_t=cmd_t, json_code=code, unique_id=uid_pref+code, dev=ch_dev)) 

        for name, code, unit, min, max, step in numbers_tmpl["log"]:
            self.__send_config("number",self.__generate_number(name=name, stat_t=stat_t, cmd_t=cmd_t, json_code=code, unique_id=uid_pref+code, dev=ch_dev, unit=unit,
            min=min, max=max, step=step))

        for name, code in text_tmpl["log"]:
            self.__send_config("text",self.__generate_text(name=name, stat_t=stat_t, cmd_t=cmd_t, json_code=code, unique_id=uid_pref+code, dev=ch_dev)) 

        for name, code, unit in sensor_tmpl["log"]:
            self.__send_config("sensor",self.__generate_sensor(name=name, stat_t=stat_t, json_code=code, unique_id=uid_pref+code, dev=ch_dev))

        #initial values (retained)
        self.client.publish(topic=stat_t, payload=json.dumps(chamber_default_logger), qos=0, retain=True)

        '''starter'''
        #WIP

        '''Misc'''
        uid_pref=f"ch{chamber_id}_msc_"
        
        #chamber nickname
        stat_t=f"chambers/{chamber_id}/misc/nickname"
        name, code=MISC_chamber_nick
        self.__send_config("text",self.__generate_text(name=name, stat_t=stat_t, cmd_t=stat_t, json_code=code, unique_id=uid_pref+code, dev=ch_dev, retain=True)) 
        self.client.publish(topic=stat_t, payload=json.dumps(chamber_default_nickname), qos=0, retain=True)

        #connstatus
        stat_t=f"chambers/{chamber_id}/misc/conn_stat"
        name,code=MISC_conn_status
        self.__send_config("sensor",self.__generate_sensor(name=name, stat_t=stat_t, json_code=code, unique_id=uid_pref+code, dev=ch_dev))
        self.client.publish(topic=stat_t, payload=json.dumps(chamber_default_connstat), qos=0, retain=True)

    def register_lab(self):
        dev={"ids": [f"lab"], "name": f"Lab 141"}
        print(f"\nRegistering Lab:")

        '''readings'''
        uid_pref="lab_sen_"
        stat_t="lab/graph"
        for name, json_code, unit in lab_sensor_tmpl["sen"]:
            self.__send_config("sensor",self.__generate_sensor(name=name, stat_t=stat_t, json_code=json_code, unique_id=uid_pref+json_code, dev=dev, unit=unit, state_class="measurement"))

        #init value (retained)
        payload=json.dumps({"TL":None,"HL":None,"TR":None})
        self.client.publish(topic=stat_t, payload=payload, qos=0, retain=True)




if __name__=="__main__":
    #cmd parse
    regist=None
    import argparse

    parser=argparse.ArgumentParser(description="Script used to register HA entities\nWARNING: this script overwrites effected entities")
    subparsers=parser.add_subparsers(dest="mode", required=True, help="Execution mode")

    chamber_parser=subparsers.add_parser("chamber", help="register chamber entities")
    chamber_parser.add_argument("id", type=str, help="chamber number to register (0, 1,... or 0...2 format). Needed only for \"chamber\" execution mode")

    lab_parser=subparsers.add_parser("lab", help="register lab entities")

    args=parser.parse_args()

    try:
        regist=Registerer()

        if args.mode=="chamber":
            if "..." in args.id:
                start, end=map(int, args.id.split("..."))
                end+=1
                for id in range(start, end):
                    regist.register_chamber(id)
    
            else:
                regist.register_chamber(int(args.id))

        elif args.mode=="lab":
            regist.register_lab()
        else:
            print("[Error] Unknown execution mode")
        
    except Exception as e:
        print(f"[Error] Exception: {e}")

    finally:
        if regist is not None:
            regist.close()
    