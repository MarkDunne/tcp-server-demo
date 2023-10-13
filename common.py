import struct

PACKET_FORMAT = '<12h'
PACKET_SIZE = struct.calcsize(PACKET_FORMAT)
HANDSHAKE_INIT_MESSAGE = b"Board Connect"
