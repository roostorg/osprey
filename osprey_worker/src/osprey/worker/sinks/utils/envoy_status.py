import socket

from gevent.server import StreamServer


def get_envoy_check_server(port: int) -> StreamServer:
    """
    In order to have the Envoy sidecar not shut down too early, we start up a dummy TCP server whose only purpose is
    to accept connections so that in the pre-stop hook we can loop using `nc -z` as the while loop condition.

    We create the socket with SO_REUSEPORT so that Envoy only has to try opening connections to one port, which will
    succeed until the very last subscriber stops.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    if sock.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT) == 0:
        raise RuntimeError('Failed to set SO_REUSEPORT.')
    sock.bind(('127.0.0.1', port))
    # 50 comes from the default for gevent's tcpserver
    sock.listen(50)
    sock.setblocking(False)
    # We just need to accept and close the socket, so we set a handler that does nothing
    return StreamServer(sock, handle=lambda sock, addr: None)
