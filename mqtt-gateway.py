#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2017 Freie Universität Berlin
# Copyright © 2017 HAW Hamburg
#
# Distributed under terms of the MIT license.

import paho.mqtt.client as mqtt
import argparse, socket, os, select, tempfile, subprocess, sys, threading, time
import re
import queue

if sys.version_info < (3,):
    from StringIo import StringIO as BytesIO
else:
    from io import BytesIO

ndn = None  # import ndn later when CCNLite path is set

CCNL_INTEREST_TIMEOUT = 40

class MQTTGateway(object):
    class CCNLiteInterestTimer(threading.Thread):
        def __init__(self, face):
            super(MQTTGateway.CCNLiteInterestTimer, self).__init__()
            self.face = face
            self.stop = threading.Event()

        def run(self):
            while not self.stop.isSet():
                time.sleep(CCNL_INTEREST_TIMEOUT)
                with self.face.pit_lock:
                    for name, (_, received) in self.face.pit.items():
                        if time.time() > (received + CCNL_INTEREST_TIMEOUT):
                            self.face.mqtt_gateway.mqtt_client.client.unsubscribe(name.decode())
                            del self.face.pit[name]

    class CCNLiteFace(threading.Thread):
        def __init__(self, mqtt_gateway):
            super(MQTTGateway.CCNLiteFace, self).__init__()
            self.mqtt_gateway = mqtt_gateway
            self.timer = MQTTGateway.CCNLiteInterestTimer(self)

            self.stop = threading.Event()
            self.pit_lock = threading.Lock()
            self.pit = {}

        def run(self):
            self.mqtt_gateway.setup()

            face_fd = self.mqtt_gateway.face_socket.fileno()
            poll = select.poll()
            poll.register(face_fd, select.POLLIN)

            try:
                while not self.stop.isSet():
                    buf, addr = self.mqtt_gateway.face_socket.recvfrom(1024)
                    f = BytesIO(buf)
                    if ndn.isInterest(f):
                        f.seek(0)
                        name = ndn.parseInterest(f)
                        name = b'/'.join(name)
                        with self.pit_lock:
                            self.pit[name] = (addr, time.time())
                            self.mqtt_gateway.mqtt_client.client.subscribe(name.decode())
                    if ndn.isData(f):
                        f.seek(0)
                        name, data = ndn.parseData(f)
                        name = b'/'.join(name)
                        self.mqtt_gateway.content_queue.put((name, data))
            finally:
                poll.unregister(face_fd)
                self.timer.stop.set()
                self.timer.join()
                with self.pit_lock:
                    for name in self.pit:
                        self.face.mqtt_gateway.mqtt_client.client.unsubscribe(name.decode())
                self.pit = {}

    class MQTTClient(threading.Thread):
        def __init__(self, ccnlite_gateway, broker_host, broker_port, retain=False):
            super(MQTTGateway.MQTTClient, self).__init__()
            self.ccnlite_gateway = ccnlite_gateway
            self.stop = threading.Event()
            self.broker_host = broker_host
            self.broker_port = broker_port
            self.retain = retain
            self.client = mqtt.Client(userdata=self.ccnlite_gateway)
            self.client.on_message = MQTTGateway.MQTTClient.on_message
            self.client.connect(broker_host, broker_port)
            self.started = False

        @staticmethod
        def on_message(client, ccnlite_gateway, message):
            face = ccnlite_gateway.ccnlite_face
            addr = None
            name = message.topic.encode()
            with face.pit_lock:
                if name not in face.pit:
                    return
                addr, _ = face.pit[name]
            name = name.split(b'/')
            data = ndn.mkData(name, message.payload)
            ccnlite_gateway.face_socket.sendto(data, addr)
            client.unsubscribe(message.topic)

        def run(self):
            if not self.started:
                self.ccnlite_gateway.setup()
                self.started = True
                self.client.loop_start()

                try:
                    while not self.stop.isSet():
                        name, data = self.ccnlite_gateway.content_queue.get()
                        self.client.publish(name.decode(), data, retain=self.retain)
                finally:
                    self.client.loop_stop()

    def __init__(self, ccn_lite_path, mqtt_broker_host, mqtt_broker_port=1883,
                 retain_pub=False, wpan_iface="lowpan0", face_port=6364, datadir=None,
                 http_status_port=None):
        self.ccn_lite_path = ccn_lite_path
        sys.path.append(os.path.abspath(os.path.join(ccn_lite_path, "src", "py", "ccnlite")))
        global ndn
        import ndn2013 as ndn
        mqtt_broker_host = str(mqtt_broker_host)
        mqtt_broker_port = int(mqtt_broker_port)
        self.wpan_iface = wpan_iface
        self.face_port = face_port
        self.face_socket = None
        if datadir == None:
            datadir = tempfile.mkdtemp(prefix="ccn-lite-datadir-")
        elif not os.path.isdir(datadir):
            os.mkdir(datadir)
        self.datadir = datadir
        self.http_status_port = http_status_port
        self._relay = None
        self._setup_lock = threading.Lock()
        self.content_queue = queue.LifoQueue()
        self.ccnlite_face = MQTTGateway.CCNLiteFace(self)
        self.ccnlite_face.start()
        self.mqtt_client = MQTTGateway.MQTTClient(self, mqtt_broker_host,
                                                  mqtt_broker_port, retain_pub)
        self.mqtt_client.start()

    def __exit__(self):
        self.ccnlite_face.stop.set()
        self.ccnlite_face.join()
        self.mqtt_client.stop.set()
        self.mqtt_client.join()
        if self._relay != None:
            self._relay.terminate()
            self._relay.wait()
        if self.face_socket != None:
            self.face_socket.close()
            os.unlink(self.face_socket_name)

    def setup(self):
        with self._setup_lock:
            if self._relay == None:
                args = [os.path.join(self.ccn_lite_path, "src", "ccn-lite-relay"),
                        '-u', str(self.face_port),
                        '-w', self.wpan_iface,
                        '-v', 'info']
                if self.http_status_port:
                    args.extend(['-t', str(self.http_status_port)])
                self._relay = subprocess.Popen(args, stderr=subprocess.PIPE)
            if self.face_socket == None:
                self.face_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.face_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST,
                                            1)
                self.face_socket.bind(("", 6363))

def ccnb_to_ndn_name(name):
    name = name[(name.rfind("prefixreg")+len("prefixreg")):name.find("%00%00")]
    name = name.replace("%00", "/")
    return name

if __name__ == "__main__":
    argparser = argparse.ArgumentParser(description="CCN-lite to MQTT gateway")

    argparser.add_argument('-m', '--mqtt-broker-host', required=True,
                           help="Host of the MQTT broker")
    argparser.add_argument('-p', '--mqtt-broker-port', type=int, required=False,
                           help="Port of the MQTT broker", default=1883)
    argparser.add_argument('-r', '--retain-pub', required=False,
                           help="Retain published data at MQTT broker",
                           action="store_true")
    argparser.add_argument('-w', '--wpan-iface', required=False,
                           default="wpan0",
                           help="Interface for the face from the CCN lite relay to the WPAN")
    argparser.add_argument('-u', '--face-port', required=False, default=6364, type=int,
                           help="UDP socket port for the face to the CCN lite relay")
    argparser.add_argument('-d', '--datadir', required=False,
                           default=None,
                           help="Directory to the CCN lite database")
    argparser.add_argument('-t', '--http-status-port', required=False,
                           default=None, type=int,
                           help="Directory to the CCN lite database")
    argparser.add_argument("ccn_lite_path", metavar="CCN_LITE_PATH",
                           help="Path to the CCN lite git repository")

    args = argparser.parse_args()
    # add CCN-lite python bindings for packet parsing
    g = MQTTGateway(**vars(args))
    pattern = re.compile(r"incoming data=<(.*)> ndn2013 from=")
    while True:
        line = g._relay.stderr.readline().decode('utf-8',"replace")
        if not line:
            continue
        match = pattern.match(line)
        if match:
            name = match.group(1)
            print("New content at %s... INDUCING CONTENT" % name)
            name = name.strip('/')
            interest = ndn.mkInterest(name.encode().split(b'/'))
            g.face_socket.sendto(interest, ("<broadcast>", g.face_port))
        print(line, end="")
