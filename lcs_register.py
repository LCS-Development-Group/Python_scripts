import paho.mqtt.client as mqtt
import json
import sys
import time
from dataclasses import dataclass

BROKER="LCSRP5"
PORT=1883

@dataclass
class sensor_t:
    name: str
    unit: str
    json_code: str

sensor_list=[
    sensor_t("RH_int", "%", "HI"),
    sensor_t("T_int", "°C", "TI"),
    sensor_t("RH_ext", "%", "HE"),
    sensor_t("T_ext", "°C", "TE"),
    sensor_t("Memb_cur", "A", "MC"),
    sensor_t("Memb_vol", "V", "MV"),
    sensor_t("Memb_pow", "W", "MP"),
]

@dataclass
class regulator_t:
    name: str
    unit: str
    json_code: str
    type: str
    min_v: float=0
    max_v: float=100
    step: float=0

regulator_list=[
    regulator_t("SP", "%", "SP", "number", 0, 100, 0.5),
    regulator_t("Hist", "%", "HI", "number", 0.1, 10, 0.1),
    regulator_t("reg_en", "", "EN", "switch"),
]


def sen_cfg(dev_id, sensor):
    return {
        "type": "sensor",
        "payload":
        {
            "name": sensor.name,
            "stat_t": f"chambers/{dev_id}/readings",
            "val_tpl": f"{{{{value_json.{sensor.json_code}}}}}",
            "unit_of_meas": sensor.unit,
            "unique_id": f"ch{dev_id}_sen_{sensor.json_code}",
            "state_class": "measurement",
            "dev": {
                "ids": [f"ch{dev_id}"],
                "name": f"Chamber {dev_id}"
            }
        }
    }

def create_sensor_config(dev_id):
    configs=[]
    for sen in sensor_list:
        configs.append(sen_cfg(dev_id, sen))

    return configs


def reg_cfg(dev_id, regulator):
    config= {
        "type": regulator.type,
        "payload":
        {
            "name": regulator.name,
            "stat_t": f"chambers/{dev_id}/regulator/get",
            "cmd_t": f"chambers/{dev_id}/regulator/set",
            "unique_id": f"ch{dev_id}_reg_{regulator.json_code}",
            "dev": {
                "ids": [f"ch{dev_id}"],
                "name": f"Chamber {dev_id}"
            }
        }
        
    }
    if regulator.type=="number":
        config["payload"].update({
            "val_tpl": f"{{{{value_json.{regulator.json_code}}}}}",
            "cmd_tpl": f'{{"{regulator.json_code}": {{{{ value }}}} }}',
            "unit_of_meas": regulator.unit,
            "min": regulator.min_v,
            "max": regulator.max_v,
            "step": regulator.step,
            "mode": "box"
        })
    elif regulator.type=="switch":
        config["payload"].update({
            "val_tpl": f"{{{{ value_json.{regulator.json_code} }}}}",
            "cmd_tpl": f'{{"{regulator.json_code}": "{{{{ value }}}}" }}',
            "payload_on": "ON",
            "payload_off": "OFF"
        })
    return config

def create_regulator_config(dev_id):
    configs=[]
    for reg in regulator_list:
        configs.append(reg_cfg(dev_id, reg))
    return configs

def create_logger_config(dev_id):
    LOGGER_SET_TOPIC=f"chambers/{dev_id}/logger/set"
    LOGGER_GET_TOPIC=f"chambers/{dev_id}/logger/get"
    LOGGER_UNIQUE_ID_PREFIX=f"ch{dev_id}_log_"
    LOGGER_DEVICE={
        "ids": [f"ch{dev_id}"],
        "name": f"Chamber {dev_id}"
    }

    config=[
        {
            "type": "text",
            "payload":
            {
                "name": "logger filename",
                "stat_t": LOGGER_GET_TOPIC,
                "cmd_t": LOGGER_SET_TOPIC,
                "unique_id": f"{LOGGER_UNIQUE_ID_PREFIX}FN",
                "dev": LOGGER_DEVICE,
                "val_tpl": f"{{{{ value_json.FN }}}}",
                "cmd_tpl": f'{{"FN": "{{{{ value }}}}" }}',
            }
        },
        {
            "type": "number",
            "payload":
            {
                "name": "logger max records",
                "stat_t": LOGGER_GET_TOPIC,
                "cmd_t": LOGGER_SET_TOPIC,
                "unique_id": f"{LOGGER_UNIQUE_ID_PREFIX}MR",
                "dev": LOGGER_DEVICE,
                "val_tpl": f"{{{{ value_json.MR }}}}",
                "cmd_tpl": f'{{"MR": "{{{{ value }}}}" }}',
                "unit_of_meas": "",
                "min": 10,
                #"min": 500,
                "max": 10000,
                "step": 500,
                "mode": "box"
            }
        },
        {
            "type": "number",
            "payload":
            {
                "name": "logger save interval",
                "stat_t": LOGGER_GET_TOPIC,
                "cmd_t": LOGGER_SET_TOPIC,
                "unique_id": f"{LOGGER_UNIQUE_ID_PREFIX}SI",
                "dev": LOGGER_DEVICE,
                "val_tpl": f"{{{{ value_json.SI }}}}",
                "cmd_tpl": f'{{"SI": "{{{{ value }}}}" }}',
                "unit_of_meas": "s",
                "min": 1,
                "max": 60,
                "step": 1,
                "mode": "box"
            }
        },
        {
            "type": "switch",
            "payload":
            {
                "name": "logger state",
                "stat_t": LOGGER_GET_TOPIC,
                "cmd_t": LOGGER_SET_TOPIC,
                "unique_id": f"{LOGGER_UNIQUE_ID_PREFIX}EN",
                "dev": LOGGER_DEVICE,
                "val_tpl": f"{{{{ value_json.EN }}}}",
                "cmd_tpl": f'{{"EN": "{{{{ value }}}}" }}',
                "payload_on": "ON",
                "payload_off": "OFF"
            }
        },
        {
            "type": "switch",
            "payload":
            {
                "name": "logger incl. reg.",
                "stat_t": LOGGER_GET_TOPIC,
                "cmd_t": LOGGER_SET_TOPIC,
                "unique_id": f"{LOGGER_UNIQUE_ID_PREFIX}IR",
                "dev": LOGGER_DEVICE,
                "val_tpl": f"{{{{ value_json.IR }}}}",
                "cmd_tpl": f'{{"IR": "{{{{ value }}}}" }}',
                "payload_on": "ON",
                "payload_off": "OFF"
            }
        }
    ]
    return config


def create_starter_config(dev_id):
    STARTER_GET_TOPIC=f"chambers/{dev_id}/starter/get"
    STARTER_SET_TOPIC=f"chambers/{dev_id}/starter/set"
    STARTER_UNIQUE_ID_PREFIX=f"ch{dev_id}_sta_"
    DEVICE={
        "ids": [f"ch{dev_id}"],
        "name": f"Chamber {dev_id}"
    }

    laser_state_code="LS"
    starter_state_code="SS"
    config=[
        {
            "type": "sensor",
            "payload":
            {
                "name": "starter laser state",
                "stat_t": STARTER_GET_TOPIC,
                "unique_id": f"{STARTER_UNIQUE_ID_PREFIX}{laser_state_code}",
                "dev": DEVICE,
                "val_tpl": f"{{{{ value_json.{laser_state_code} }}}}",
            }
        },
        {
            "type": "switch",
            "payload":
            {
                "name": "starter state",
                "stat_t": STARTER_GET_TOPIC,
                "cmd_t": STARTER_SET_TOPIC,
                "unique_id": f"{STARTER_UNIQUE_ID_PREFIX}{starter_state_code}",
                "dev": DEVICE,
                "val_tpl": f"{{{{ value_json.{starter_state_code} }}}}",
                "cmd_tpl": f'{{"{starter_state_code}": "{{{{ value }}}}" }}',
                "payload_on": "ON",
                "payload_off": "OFF"
            }
        }
    ]
    return config

def register_configs(dev_id, config):
    print("\n[registering entities]")
    configs=create_sensor_config(device)+create_regulator_config(dev_id)+create_logger_config(dev_id)+create_starter_config(dev_id)
    
    for item in configs:
        config=item['payload']

        topic=f"homeassistant/{item['type']}/{config['unique_id']}/config"

        #print(f"{topic}")
        #continue

        json_payload = json.dumps(config)

        result = client.publish(topic, json_payload, retain=True)
        result.wait_for_publish()

        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"Registered {config['unique_id']}")
        else:
            print(f"Failed to send message: {result.rc}")


try:
    device=sys.argv[1]
except IndexError:
    print("Missing arguments: [chamber id]")
    sys.exit(1)

client=mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
print(f"connecting to {BROKER}")
client.connect(BROKER, PORT, 60)

#register_sensors(device, client)
#register_regulator(device, client)
#register_logger(device, client)
register_configs(device, client)

time.sleep(1)
client.disconnect()
print("finished execution")