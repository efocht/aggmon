import logging
import time
import random
try:
    import ujson as json
except:
    import json
import zmq


log = logging.getLogger( __name__ )


def send_agg_command(zmq_context, jagg_port, cmd, **kwds):
    """
    Send a command to a job aggregator over it's listen (PULL) port.
    This works asynchronously, no ACK is received.
    """
    socket = zmq_context.socket(zmq.PUSH)
    log.debug("connecting to %s" % jagg_port)
    socket.connect(jagg_port)
    kwds["cmd"] = cmd
    msg = {"_COMMAND_": kwds}
    json_msg = json.dumps(msg)
    log.debug("sending to %s msg: %r" % (jagg_port, json_msg))
    try:
        socket.send(json_msg, flags=zmq.NOBLOCK)
    except Exception as e:
        log.error("Exception when sending: %r" % e)
    #
    # TODO: keep connection and socket...?
    #
    log.debug("disconnecting from %s" % jagg_port)
    socket.disconnect(jagg_port)
    socket.close()

def send_agg_data(zmq_context, jagg_port, data, **kwds):
    """
    Send a command to a job aggregator over it's listen (PULL) port.
    This works asynchronously, no ACK is received.
    """
    socket = zmq_context.socket(zmq.PUSH)
    log.debug("connecting to %s" % jagg_port)
    socket.connect(jagg_port)
    json_msg = json.dumps(data)
    log.debug("sending to %s msg: %r" % (jagg_port, json_msg))
    try:
        socket.send(json_msg, flags=zmq.NOBLOCK)
    except Exception as e:
        log.error("Exception when sending: %r" % e)
    #
    # TODO: keep connection and socket...?
    #
    log.debug("disconnecting from %s" % jagg_port)
    socket.disconnect(jagg_port)
    socket.close()

if __name__ == "__main__":
    context = zmq.Context()
    epoch = int(time.time()) - 100
    random.seed(epoch)
    for n in xrange(9):
        data = {"N": "likwid.cpu1.dpmflops", "H": "node" + str(n), "V": random.randint(0, 99), "T": epoch + n}
        send_agg_data(context, "tcp://127.0.0.1:5550", data)    
        data = {"N": "cpu.total.user", "H": "node" + str(n), "V": random.randint(0, 99), "T": epoch + n}
        send_agg_data(context, "tcp://127.0.0.1:5550", data)    
        data = {"N": "cpu.cpu1.nice", "H": "node" + str(n), "V": random.randint(0, 99), "T": epoch + n}
        send_agg_data(context, "tcp://127.0.0.1:5550", data)    
        data = {"N": "memory.SwapTotal", "H": "node" + str(n), "V": random.randint(0, 99), "T": epoch + n}
        send_agg_data(context, "tcp://127.0.0.1:5550", data)    

