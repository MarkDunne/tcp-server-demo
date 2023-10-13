import struct
from datetime import datetime

import psycopg
from common import HANDSHAKE_INIT_MESSAGE, PACKET_FORMAT, PACKET_SIZE
from twisted.internet import protocol, reactor

DATABASE_URL = "postgres://postgres:neurobellpw@localhost/mydb"


def clear_db():
    with psycopg.connect(DATABASE_URL) as conn, conn.cursor() as curr:
        curr.execute(
            """
            DROP TABLE IF EXISTS readings;
            CREATE TABLE readings (
                device_id INT NOT NULL,
                counter INT NOT NULL,
                channel1 INT NOT NULL,
                channel2 INT NOT NULL,
                channel3 INT NOT NULL,
                channel4 INT NOT NULL,
                channel5 INT NOT NULL,
                channel6 INT NOT NULL,
                channel7 INT NOT NULL,
                channel8 INT NOT NULL,
                channel9 INT NOT NULL,
                channel10 INT NOT NULL
            );

            CREATE INDEX ON readings (device_id);
        """
        )
    print("Database cleared")


class ServerProtocol(protocol.Protocol):
    def connectionMade(self):
        self.start_time = datetime.now()
        self.num_packets_received = 0
        self.data_buffer = b""
        self.handshake_complete = False

    def dataReceived(self, data):
        self.data_buffer += data
        if not self.handshake_complete:
            if len(self.data_buffer) < len(HANDSHAKE_INIT_MESSAGE) + 4:
                # Not enough data to check for handshake
                return
            
            if not self.data_buffer.startswith(HANDSHAKE_INIT_MESSAGE):
                print("Error: Handshake message not received")
                self.transport.loseConnection()

            self.data_buffer = self.data_buffer[len(HANDSHAKE_INIT_MESSAGE) :]
            self.patient_id = int.from_bytes(self.data_buffer[:4], "little")
            self.data_buffer = self.data_buffer[4:]
            print(f"Handshake complete for patient ID: {self.patient_id}")
            self.handshake_complete = True

        num_full_packets = len(self.data_buffer) // PACKET_SIZE
        bytes_to_extract = num_full_packets * PACKET_SIZE
        well_formed_bytes = self.data_buffer[:bytes_to_extract]
        self.data_buffer = self.data_buffer[bytes_to_extract:]

        with self.factory.curr.copy("COPY readings FROM STDIN") as copy:
            for packet in struct.iter_unpack(PACKET_FORMAT, well_formed_bytes):
                self.num_packets_received += 1
                copy.write_row(packet)

    def connectionLost(self, reason):
        self.factory.conn.commit()
        end_time = datetime.now()
        elapsed_time = (end_time - self.start_time).total_seconds()
        packets_per_sec = self.num_packets_received / elapsed_time
        self.factory.connection_closed(
            elapsed_time, packets_per_sec, self.num_packets_received
        )


class ServerFactory(protocol.Factory):
    def __init__(self) -> None:
        self.open_protocols = 0
        self.connection_elapsed_times = []
        self.connection_packets_per_sec = []
        self.total_packets_received = 0

    def startFactory(self):
        self.conn = psycopg.connect(DATABASE_URL, autocommit=True)
        self.curr = self.conn.cursor()

    def stopFactory(self):
        self.curr.close()
        self.conn.close()

    def buildProtocol(self, addr):
        protocol = ServerProtocol()
        protocol.factory = self
        self.open_protocols += 1
        return protocol

    def connection_closed(self, elapsed_time, packets_per_sec, num_packets_received):
        self.open_protocols -= 1
        self.connection_elapsed_times.append(elapsed_time)
        self.connection_packets_per_sec.append(packets_per_sec)
        self.total_packets_received += num_packets_received

        if self.open_protocols == 0:
            print("All connections closed")

            mean_elapsed_time = sum(self.connection_elapsed_times) / len(
                self.connection_elapsed_times
            )
            mean_packets_per_sec = sum(self.connection_packets_per_sec) / len(
                self.connection_packets_per_sec
            )

            print(
                f"Server mean elapsed time: {mean_elapsed_time:.4f}, mean packets/sec: {mean_packets_per_sec:.2f}, total packets received: {self.total_packets_received}"
            )


if __name__ == "__main__":
    clear_db()
    factory = ServerFactory()
    reactor.listenTCP(8080, factory)
    print("Serving on 8080")

    reactor.run()
