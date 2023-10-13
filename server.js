const { Pool } = require('pg');
const net = require('net');
const { performance } = require('perf_hooks');
// import { from as copyFrom } from 'pg-copy-streams';
var { from } = require('pg-copy-streams')

const DATABASE_URL = "postgres://postgres:neurobellpw@localhost/mydb";
const PACKET_SIZE = 22;

const pool = new Pool({ connectionString: DATABASE_URL });

const handleConnection = (socket) => {
    let start_time = performance.now();
    let num_packets_received = 0;
    let data_buffer = Buffer.alloc(0);

    socket.on('data', (data) => {
        data_buffer = Buffer.concat([data_buffer, data]);
        const num_full_packets = Math.floor(data_buffer.length / PACKET_SIZE);
        const bytes_to_extract = num_full_packets * PACKET_SIZE;
        const well_formed_bytes = data_buffer.subarray(0, bytes_to_extract);
        data_buffer = data_buffer.subarray(bytes_to_extract);

        const rows = [];
        for (let i = 0; i < num_full_packets; i++) {
            const packet = well_formed_bytes.subarray(i * PACKET_SIZE, (i + 1) * PACKET_SIZE);
            rows.push(packet)
            num_packets_received++;
        }

        pool.connect(function (err, client, done) {
            var stream = client.query(from('COPY readings FROM STDIN WITH (FORMAT binary)'))
            stream.on('error', done)
            stream.on('finish', done)
            rows.forEach(row => stream.write(row));
          })
    });

    socket.on('end', () => {
        const end_time = performance.now();
        const elapsed_time = (end_time - start_time) / 1000;
        const packets_per_sec = num_packets_received / elapsed_time;
        console.log(`Connection closed: elapsed time=${elapsed_time}s, packets per second=${packets_per_sec}, num packets received=${num_packets_received}`);
    });
};

const server = net.createServer(handleConnection);

server.listen(8080, () => {
    console.log('Server listening on port 8000');
});

process.on('SIGINT', () => {
    console.log('Shutting down server');
    server.close(() => {
        pool.end(() => {
            console.log('Server and database connection closed');
            process.exit(0);
        });
    });
});