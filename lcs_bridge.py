import serial
import json
import time
import threading
import paho.mqtt.client as mqtt

readings={
    "JT":"sen",
    "HI":0.0,
    "TI":0.0,
    "HE":0.0,
    "TE":0.0,
    "MC":0.0,
    "MV":0.0,
    "MP":0.0,
}
regulator_settings={
    "SP": 25.0,
    "HI": 5.0,
    "EN": "OFF",
    "ME": "OFF"
}
starter_settings={
    "SS": "OFF",
    "LS": "OFF"
}

UART_PORT="COM3"
BAUDRATE=115200
MQTT_BROKER="LCSRP5"
MQTT_PORT=1883

def readings_topic(dev_id):
    return f"chambers/{dev_id}/readings"
def reg_set_topic(dev_id):
    return f"chambers/{dev_id}/regulator/set"
def reg_get_topic(dev_id):
    return f"chambers/{dev_id}/regulator/get"
def sta_set_topic(dev_id):
    return f"chambers/{dev_id}/starter/set"
def sta_get_topic(dev_id):
    return f"chambers/{dev_id}/starter/get"


def mqtt_result_handler(result):
    if result.rc!=mqtt.MQTT_ERR_SUCCESS:
        print(f"Failed to send message: {result.rc}")

class bridge_intance(threading.Thread):
    def __init__(self, task_name):
        super().__init__()
        self.task_name=task_name

        self.mqtt_client=mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self.mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        

        self.daemon=True
        self.keep_alive=False
        
        self.sm=serial.Serial()
        self.sm.dtr=False
        self.sm.rts=False

        self.regulator_return_topic=None
        self.regulator_value_topic=None
        self.starter_return_topic=None
        self.starter_value_topic=None

    def deinit(self):
        self.uart_disconnect()
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()

    def uart_connect(self, port, baudrate):
        self.sm.port=port
        self.sm.baudrate=baudrate
        self.dev_id=None

        self.sm.open()
        self.keep_alive=True
        self.sm.reset_input_buffer()
        self.sm.reset_output_buffer()
        self.start()

    def reconnect_mqtts(self):
        self.mqtt_client.loop_stop()
        self.readings_topic=readings_topic(self.dev_id)

        if self.regulator_value_topic is not None:
            self.mqtt_client.unsubscribe(self.regulator_value_topic)
            self.mqtt_client.message_callback_remove(self.regulator_value_topic)

        self.regulator_return_topic=reg_get_topic(self.dev_id)
        self.regulator_value_topic=reg_set_topic(self.dev_id)
        self.mqtt_client.message_callback_add(self.regulator_value_topic, self.regulator_from_server_cb)
        self.mqtt_client.subscribe(self.regulator_value_topic)

        if self.starter_value_topic is not None:
            self.mqtt_client.unsubscribe(self.starter_value_topic)
            self.mqtt_client.message_callback_remove(self.starter_value_topic)

        self.starter_return_topic=sta_get_topic(self.dev_id)
        self.starter_value_topic=sta_set_topic(self.dev_id)
        self.mqtt_client.message_callback_add(self.starter_value_topic, self.starter_from_server_cb)
        self.mqtt_client.subscribe(self.starter_value_topic)

        self.mqtt_client.loop_start()

    def uart_disconnect(self):
        self.keep_alive=False
        if self.sm and self.sm.is_open:
            self.sm.close()

        self.port=None
        self.baudrate=None
        self.dev_id=None

    def regulator_from_server_cb(self, client, userdata, msg):
        try:
            payload=json.loads(msg.payload.decode())

            msg={
                "JT": "reg",
                "ID": self.dev_id,
            }
            msg.update(payload)

            print(msg)
            json_line=json.dumps(msg)+'\n'
            self.sm.write(json_line.encode("utf-8"))

        except Exception as e:
            print(f"json parsing exception: {e}")

    def starter_from_server_cb(self, client, userdata, msg):
        try:
            payload=json.loads(msg.payload.decode())

            msg={
                "JT": "sta",
                "ID": self.dev_id,
            }
            msg.update(payload)

            print(msg)
            json_line=json.dumps(msg)+'\n'
            self.sm.write(json_line.encode("utf-8"))

        except Exception as e:
            print(f"json parsing exception: {e}")

    def run(self):
        try:
            while self.keep_alive and self.sm and self.sm.is_open:
                line=self.sm.readline() #blocking

                if not line:
                    break

                #process the line
                payload=json.loads(line)

                if "ID" in payload:
                    id=payload.pop("ID")
                    if id!=self.dev_id:
                        self.dev_id=id
                        self.reconnect_mqtts()
                else:
                    print(f"{self.task_name} no dev_id")

                if "JT" in payload:
                    jason_type=payload.pop("JT")
                    match jason_type:
                        case "sen":
                            mqtt_result_handler(self.mqtt_client.publish(self.readings_topic, json.dumps(payload)))
                        case "reg":
                            mqtt_result_handler(self.mqtt_client.publish(self.regulator_return_topic, json.dumps(payload)))
                        case "sta":
                            mqtt_result_handler(self.mqtt_client.publish(self.starter_return_topic, json.dumps(payload)))
                else:
                    print(f"{self.task_name} no jason type")


        except Exception as e:
            print(f"{self.task_name} -run encountered exception: {e}, {payload}")

try:
    bridge0=bridge_intance("bridge0")
    bridge0.uart_connect(UART_PORT, BAUDRATE)

    while True:
        time.sleep(1)

except KeyboardInterrupt:
    print("stopping")
    bridge0.deinit()
