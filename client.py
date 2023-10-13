import argparse
import struct
from datetime import datetime

import numpy as np
from common import HANDSHAKE_INIT_MESSAGE, PACKET_FORMAT
from twisted.internet import protocol, reactor


class ClientProtocol(protocol.Protocol):
    def connectionMade(self):
        self.start_time = datetime.now()
        self.num_packets = self.factory.num_packets
        self.patient_id = self.factory.get_next_patient_id()
        self.send_data()

    def send_data(self):

        self.transport.write(HANDSHAKE_INIT_MESSAGE)

        self.transport.write(self.patient_id.to_bytes(4, "little"))
        self.transport.write(b" " * 10)

        packet = np.zeros(12, dtype=np.int16)
        packet[0] = self.patient_id
        packet = np.random.randint(0, 1000, size=12, dtype=np.int16)
        for _ in range(self.num_packets):
            packet[1] += 1
            packet[2:] += np.random.randint(-10, 10, size=10, dtype=np.int16)
            packet_data = struct.pack(PACKET_FORMAT, *packet)
            self.transport.write(packet_data)

        self.transport.loseConnection()

    def connectionLost(self, reason):
        end_time = datetime.now()
        elapsed_time = (end_time - self.start_time).total_seconds()
        packets_per_sec = self.num_packets / elapsed_time
        print(
            f"Patient ID {self.patient_id} time elapsed: {elapsed_time:.4f}, packets/sec: {packets_per_sec:.2f}. Total packets: {self.num_packets}"
        )


class ClientFactory(protocol.ClientFactory):
    def __init__(self, num_clients, num_packets) -> None:
        self.num_clients = num_clients
        self.num_packets = num_packets
        self.closed_protocols = 0
        self.next_patient_id = 0

    def get_next_patient_id(self):
        patient_id = self.next_patient_id
        self.next_patient_id += 1        
        return patient_id

    def buildProtocol(self, addr):
        protocol = ClientProtocol()
        protocol.factory = self
        return protocol

    def clientConnectionLost(self, connector, reason):
        self.closed_protocols += 1
        if self.closed_protocols == self.num_clients:
            reactor.stop()
            print("Total packets sent: ", self.num_packets * self.num_clients)


def start_clients(num_clients, num_packets=1000):
    factory = ClientFactory(num_clients=num_clients, num_packets=num_packets)
    for i in range(num_clients):
        reactor.connectTCP("localhost", 8080, factory)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-clients", type=int, default=1)
    parser.add_argument("--num-packets", type=int, default=1000)
    args = parser.parse_args()

    start_clients(args.num_clients, args.num_packets)
    reactor.run()
