import logging


log = logging.getLogger( __name__ )


def zmq_socket_bind_range(sock, listen):
    """
    This accepts port ranges as argument, for example "tcp://*:6100-6200" or
    even concatenated ranges like "tcp://0.0.0.0:6100-6200,7100-7200".

    Returns the port to which the socket was bound or None in case of error.
    """
    proto, addr, ports = listen.split(":")
    port = None
    if ports.isdigit():
        # this is just a port number, do simple bind
        try:
            sock.bind(listen)
            port = int(ports)
        except Exception as e:
            log.error("zmq_socket_bind_range failed: %r" % e)
    elif "," in ports or "-" in ports:
        for prange in ports.split(","):
            if "-" in prange:
                # this should better be a range of ports
                p_min, p_max = prange.split("-")
                p_min = int(p_min)
                p_max = int(p_max)
                try:
                    port = sock.bind_to_random_port(proto + ":" + addr, min_port=p_min, max_port=p_max, max_tries=100)
                except Exception as e:
                    log.error("zmq_socket_bind_range failed: %r" % e)
                else:
                    break
            elif prange.isdigit():
                try:
                    sock.bind(proto + ":" + addr + ":" + prange)
                    port = int(prange)
                except Exception as e:
                    pass
                else:
                    break
    return port
