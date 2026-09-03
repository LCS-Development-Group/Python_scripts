import paho.mqtt.client as mqtt
import json
import threading
import signal
import sys

# MQTT_BROKER="LCSRP5.local"
MQTT_BROKER="127.0.0.1" #same device, mDNS unnecessary
MQTT_PORT=1883
MQTT_LAB_SEN_TOPIC="lab/readings"
MQTT_LAB_SEN_GRAPH_TOPIC="lab/graph"
RP5_CPU_TEMP_PATH="/sys/devices/virtual/thermal/thermal_zone0/temp"
SEND_PERIOD_S=5


class LabSender:
    def __init__(self):
        self._RP5_temperat=0.0
        self._lab_temperat=None
        self._lab_RH=None
        self._stop_cond=threading.Event()

        signal.signal(signal.SIGINT, self._system_signal_handler)
        signal.signal(signal.SIGTERM, self._system_signal_handler)

        try:
            self.mqtt_client=mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
            self.mqtt_client.connect(host=MQTT_BROKER, port=MQTT_PORT)
            self.mqtt_client.loop_start()

        except Exception as e:
            sys.stderr.write(f"[EXCEPT] {e}\n")
            self._stop_cond.set()

    def main_loop(self):
        while not self._stop_cond.is_set():
            try:
                self._RP5_temperat=self._get_RP5_temperat()
                readigns_json=self._assemble_graph_json()
                self.mqtt_client.publish(MQTT_LAB_SEN_GRAPH_TOPIC, readigns_json)
                self._stop_cond.wait(timeout=SEND_PERIOD_S)

            except Exception as e:
                sys.stderr.write(f"[EXCEPT] {e}\n")
                self._stop_cond.set()
        self.cleanup()

    def cleanup(self):
        try:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        except Exception:
            pass

    def _assemble_graph_json(self):
        data={
            "HL":None,
            "TL":None,
            "TR":round(self._RP5_temperat, 2),
        }
        return json.dumps(data)

    def _get_RP5_temperat(self):
        with open(RP5_CPU_TEMP_PATH, "r") as file:
            return int(file.read().strip())/1000.0

    def _system_signal_handler(self, signum, frame):
        match signum:
            case signal.SIGINT | signal.SIGTERM:
                self._stop_cond.set()

    
            

if __name__=="__main__":
    sender=LabSender()
    sender.main_loop()
    