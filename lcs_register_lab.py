import paho.mqtt.client as mqtt
import json

MQTT_BROKER="LCSRP5"
MQTT_PORT=1883
MQTT_LABSEN_TOPIC="lab/readings"

class Sensor_t:
    def __init__(self, name: str, unit: str, json_code: str, device_id: str, device_name: str):
        self.name=name
        self.unit=unit
        self.json_code=json_code
        self.device_id=device_id
        self.device_name=device_name

sensors=[
    Sensor_t("RP5_T", "°C", "SOCT", "lcs_rp5_board", "Lab142 Raspberry Pi 5"),
]



class Register:
    def __init__(self):
        self.cfgs=[]
        try:
            self.mqtt_client=mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
            self.mqtt_client.connect(host=MQTT_BROKER, port=MQTT_PORT)

        except Exception as e:
            print(f"EXCEPT: {e}")

    def register_sensors(self):
        for sen in sensors:
            self.cfgs.append(self._make_sensor_cfg(sensor=sen))

        for item in self.cfgs:
            try:
                config=item['payload']
                topic=f"homeassistant/{item['type']}/{config['unique_id']}/config"

                json_payload = json.dumps(config)
                
                result=self.mqtt_client.publish(topic, json_payload, retain=True)
                result.wait_for_publish()

                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    print(f"Registered {config['unique_id']}")
                else:
                    print(f"Failed to send message: {result.rc}")

            except Exception as e:
                print(f"EXCEPT: {e}")

    def _make_sensor_cfg(self, sensor:Sensor_t):
        return {
            "type": "sensor",
            "payload":
            {
                "name": sensor.name,
                "stat_t": f"lab/readings",
                "val_tpl": f"{{{{value_json.{sensor.json_code}}}}}",
                "unit_of_meas": sensor.unit,
                "unique_id": f"lab_{sensor.json_code}",
                "state_class": "measurement",
                "dev": {
                    "ids": [sensor.device_id],
                    "name": sensor.device_name
                }
            }
        }

if __name__=="__main__":
    registerer=Register()
    registerer.register_sensors()





