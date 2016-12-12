import serial
from datetime import datetime, timedelta
from struct import unpack
from binascii import unhexlify

from xbee import ZigBee
from kombu.common import maybe_declare
from kombu.pools import producers
from kombu import BrokerConnection, Exchange

temperature_exchange = Exchange('temperature', type='fanout')
button_exchange = Exchange('button', type='fanout')

def decode_float(s):
  """Other possible implementation. Don't know what's better
  #from ctypes import *
  s = s[6:8] + s[4:6] + s[2:4] + s[0:2] # reverse the byte order
  i = int(s, 16)                   # convert from hex to a Python int
  cp = pointer(c_int(i))           # make this into a c integer
  fp = cast(cp, POINTER(c_float))  # cast the int pointer to a float pointer
  return fp.contents.value         # dereference the pointer, get the float
  """
  return unpack('<f', unhexlify(s))[0]


class XBeeReceiver(object):

    DATA_PROCESSORS = {
        'TMP': decode_float,
        'BTN': int,
    }

    MESSAGE_TYPES = {
        'TMP': 'temperature',
        'BTN': 'button'
    }

    EXCHANGES = {
        'TMP': 'temperature',
        'BTN': 'button'
    }

    def __init__(self, serial_port, username='guest', password='guest', hostname='localhost', port=5672):
        self.xbee = ZigBee(serial_port, escaped=True)
        self.connection = BrokerConnection('amqp://{}:{}@{}:{}//'.format(username, password, hostname, port))

    def on_message(self, message, producer):
        try:
            msg = message['rf_data']
        except KeyError:
            return
        print "Data {}".format(msg)
        try:
            type_code = msg.split(' ')[0]
        except IndexError:
            return
        print "Type Code = {}".format(type_code)
        msg_data = msg.split(' ')[1]

        processor = self.DATA_PROCESSORS.get(type_code, None)
        if processor:
            processed_data = processor(msg_data)
            payload = {"source": "living_room",
                       "data": processed_data,
                       "type": self.MESSAGE_TYPES[type_code],
                       "queue_time": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')}
            producer.publish(payload, exchange=self.EXCHANGES[type_code], serializer='json', routing_key="living_room")

    def run(self):
        self.xbee.at(id=b'\x08', command='\x4E\44')
        with producers[self.connection].acquire(block=True) as producer:
            maybe_declare(temperature_exchange, producer.channel)
            maybe_declare(button_exchange, producer.channel)
            
            while True:
                frame = self.xbee.wait_read_frame()
                self.on_message(frame, producer)

if __name__ == '__main__':
    serial_port = serial.Serial('/dev/ttyAMA0', 9600)
    receiver = XBeeReceiver(serial_port)
    receiver.run()
