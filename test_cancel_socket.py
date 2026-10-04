"""Real psycopg against a loopback PostgreSQL-protocol server, not a SQL stub."""
import asyncio
import struct
import time
import unittest

import psycopg

import loadgen_lambda as app


class SocketDeadlineTest(unittest.IsolatedAsyncioTestCase):
    async def test_stalled_query_has_bounded_timeout_and_no_cancel_connection(self):
        seen, peers, jobs = [], [], set()

        async def serve(reader, writer):
            jobs.add(asyncio.current_task())
            peers.append(writer)
            try:
                n = struct.unpack('!I', await reader.readexactly(4))[0]
                body = await reader.readexactly(n - 4)
                code = struct.unpack('!I', body[:4])[0]
                seen.append('cancel' if code == 80877102 else 'startup')
                if code != 80877102:
                    def msg(kind, payload):
                        return kind + struct.pack('!I', len(payload) + 4) + payload
                    writer.write(msg(b'R', struct.pack('!I', 0))
                                 + msg(b'S', b'server_version\x0016.0\x00')
                                 + msg(b'S', b'client_encoding\x00UTF8\x00')
                                 + msg(b'K', struct.pack('!II', 1, 2)) + msg(b'Z', b'I'))
                    await writer.drain()
                await reader.read()  # accept queries/cancellation but never answer
            finally:
                writer.close()
                jobs.discard(asyncio.current_task())

        server = await asyncio.start_server(serve, '127.0.0.1', 0)
        port = server.sockets[0].getsockname()[1]
        conn = await psycopg.AsyncConnection.connect(host='127.0.0.1', port=port, user='x', dbname='x',
                                                     sslmode='disable', gssencmode='disable', autocommit=True)
        started = time.monotonic()
        try:
            try:
                await app.execute(conn, 'SELECT 1')
            except (psycopg.OperationalError, TimeoutError):
                pass
            elapsed = time.monotonic() - started
            print(f'FAKE SERVER: timeout={app.DB_TIMEOUT}s elapsed={elapsed:.2f}s connections={seen}', flush=True)
            self.assertLess(elapsed, app.DB_TIMEOUT + 1.0)
            self.assertEqual(seen, ['startup'])
        finally:
            await conn.close()
            server.close()
            await server.wait_closed()
            for writer in peers:
                writer.close()
            if jobs:
                await asyncio.gather(*jobs, return_exceptions=True)


if __name__ == '__main__':
    unittest.main(verbosity=2)
