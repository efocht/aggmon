import logging
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

