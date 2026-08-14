import paho.mqtt.client as mqtt
import json
import time
import threading
import sys
import signal

MQTT_BROKER="LCSRP5"
MQTT_PORT=1883
MQTT_LABSEN_TOPIC="lab/readings"
RP5_CPU_TEMP_PATH="/sys/devices/virtual/thermal/thermal_zone0/temp"
SEND_PERIOD_S=1


class LabSender:
    def __init__(self):
        self._soc_temperat=0.0
        self._stop_cond=threading.Event()

        signal.signal(signal.SIGINT, self._system_signal_handler)
        signal.signal(signal.SIGTERM, self._system_signal_handler)

        try:
            self.mqtt_client=mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
            self.mqtt_client.connect(host=MQTT_BROKER, port=MQTT_PORT)

        except Exception as e:
            print(f"EXCEPT: {e}")
            self._stop_cond.set()

    def main_loop(self):
        while not self._stop_cond.is_set():
            try:
                self._soc_temperat=self._get_soc_temperat()
                readigns_json=self._assemble_readings_json()

                self.mqtt_client.publish(MQTT_LABSEN_TOPIC, readigns_json)
                self._stop_cond.wait(timeout=SEND_PERIOD_S)

            except Exception as e:
                print(f"EXCEPT: {e}")
                self._stop_cond.set()

        self.cleanup()

    def cleanup(self):
        pass

    def _assemble_readings_json(self):
        data={
            "SOCT":self._soc_temperat,
        }
        return json.dumps(data)

    def _get_soc_temperat(self):
        with open(RP5_CPU_TEMP_PATH, "r") as file:
            return int(file.read().strip())/1000.0

    def _system_signal_handler(self, signum, frame):
        match signum:
            case signal.SIGINT | signal.SIGTERM:
                self._stop_cond.set()

    
            

if __name__=="__main__":
    sender=LabSender()
    sender.main_loop()
    