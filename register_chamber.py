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
    "log": [],
}

numbers_tmpl={
    "reg": [
        ("SP","SP", "%", 0, 100, 0.5),
        ("Hist","HI", "%", 0, 20, 0.1)],
    "log": [
        ("logger_max_records","MR", "", "10", "10000", "500"),
        ("logger_save_interval","SI", "s", "1", "60", "1")]
}

switch_tmpl={
    "reg": [
        ("reg_en","EN")],
    "log": [
        ("logger_state","EN"),
        ("include_regulator","IR")]
}

text_tmpl={
    "log": [
        ("logger_filepostfix", "FP")],
}

MISC_chamber_nick=("chamber_nick", "CN")
MISC_conn_status=("Conn_status", "CS")

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

    def __generate_text(self, name: str, stat_t:str, cmd_t:str, json_code: str, unique_id: str, dev:dict)->dict:
        payload={
            "name": name, 
            "stat_t": stat_t, 
            "cmd_t": cmd_t, 
            "unique_id": unique_id, 
            "dev": dev, 
            "val_tpl": f"{{{{value_json.{json_code}}}}}", 
            "cmd_tpl": f"{{\"{json_code}\": \"{{{{ value }}}}\" }}"}
        return payload

    def register_chamber(self, chamber_id:int):
        ch_dev={"ids": [f"ch{chamber_id}"], "name": f"Chamber {chamber_id}"}
        print(f"\nRegistering Chamber {chamber_id}:")

        '''sensors'''
        stat_t=f"chambers/{chamber_id}/readings"
        uid_pref=f"ch{chamber_id}_sen_"
        for name, code, unit in sensor_tmpl["sen"]:
            self.__send_config("sensor",self.__generate_sensor(name=name, stat_t=stat_t, json_code=code, unique_id=uid_pref+code, dev=ch_dev, unit=unit, state_class="measurement"))
        

        '''regulator'''
        stat_t=f"chambers/{chamber_id}/regulator/get"
        cmd_t=f"chambers/{chamber_id}/regulator/set"
        uid_pref=f"ch{chamber_id}_reg_"
        for name, code, unit, min, max, step in numbers_tmpl["reg"]:
            self.__send_config("number",self.__generate_number(name=name, stat_t=stat_t, cmd_t=cmd_t, json_code=code, unique_id=uid_pref+code, dev=ch_dev, unit=unit,
            min=min, max=max, step=step))

        for name, code in switch_tmpl["reg"]:
            self.__send_config("switch",self.__generate_switch(name=name, stat_t=stat_t, cmd_t=cmd_t, json_code=code, unique_id=uid_pref+code, dev=ch_dev))        

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

        '''starter'''
        #WIP

        '''Misc'''
        uid_pref=f"ch{chamber_id}_msc_"
        
        #chamber nickname
        topic_t=f"chambers/{chamber_id}/misc/nickname"
        name, code=MISC_chamber_nick
        self.__send_config("text",self.__generate_text(name=name, stat_t=topic_t, cmd_t=topic_t, json_code=code, unique_id=uid_pref+code, dev=ch_dev)) 
    
        #connstatus
        topic_t=f"chambers/{chamber_id}/misc/conn_stat"
        name,code=MISC_conn_status
        self.__send_config("sensor",self.__generate_sensor(name=name, stat_t=topic_t, json_code=code, unique_id=uid_pref+code, dev=ch_dev))













    def register_lab(self):
        pass #WIP






if __name__=="__main__":
    #cmd parse
    try:
        if len(sys.argv)!=2:
            print("Usage: python ./register_chamber.py <chamber_id>\n")
            print("\tchamber_id - chamber number to register (0, 1, 2, ..) or a...b to register a range\n")
            print("warning: this script overwrites entities in HA")
            sys.exit(0)
        else:
            regist=Registerer()
            chamber_id_arg=sys.argv[1]


            if "..." in chamber_id_arg:
                start, end=map(int, chamber_id_arg.split("..."))
                end+=1
                for id in range(start, end):
                    regist.register_chamber(id)

            else:
                id=int(chamber_id_arg)
                regist.register_chamber(id)
    except Exception as e:
        print(f"EXCEPTION: {e}")

    finally:
        regist.close()
    