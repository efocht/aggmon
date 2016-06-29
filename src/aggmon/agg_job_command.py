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
    epoch = time.time() - 100
    random.seed(epoch)
    epoch = int(epoch * 10) / 10.
    for n in xrange(9):
        epoch += 1
        data = [
        {"N":"servers.%s.likwid.cpu7.rmem_mbpers","T": epoch,"V":0.0},
        {"N":"servers.%s.likwid.cpu7.rmem_r_mbpers","T": epoch,"V":0.0},
        {"N":"servers.%s.likwid.cpu7.rmem_w_mbpers","T": epoch,"V":0.0},
        {"N":"servers.%s.likwid.cpu8.clock","T": epoch,"V":2933.3852888381},
        {"N":"servers.%s.likwid.cpu8.cpi","T": epoch,"V":1.0470516825},
        {"N":"servers.%s.likwid.cpu8.dpmflops","T": epoch,"V":332.4072434038},
        {"N":"servers.%s.likwid.cpu8.spmflops","T": epoch,"V":609.6180576591},
        {"N":"servers.%s.likwid.cpu8.spmuops","T": epoch,"V":188.8682376402},
        {"N":"servers.%s.likwid.cpu8.dpmuops","T": epoch,"V":4.9335986359},
        {"N":"servers.%s.likwid.cpu8.mem_mbpers","T": epoch,"V":0.0},
        {"N":"servers.%s.interrupts.cpu4.PCI-MSI-edge_mlx4-ib-1-13@PCI_Bus_0000:10","T": epoch,"V":0},
        {"N":"servers.%s.interrupts.cpu5.PCI-MSI-edge_mlx4-ib-1-13@PCI_Bus_0000:10","T": epoch,"V":0},
        {"N":"servers.%s.interrupts.cpu6.PCI-MSI-edge_mlx4-ib-1-13@PCI_Bus_0000:10","T": epoch,"V":0},
        {"N":"servers.%s.interrupts.cpu7.PCI-MSI-edge_mlx4-ib-1-13@PCI_Bus_0000:10","T": epoch,"V":0},
        {"N":"servers.%s.interrupts.cpu8.PCI-MSI-edge_mlx4-ib-1-13@PCI_Bus_0000:10","T": epoch,"V":0},
        {"N":"servers.%s.interrupts.cpu9.PCI-MSI-edge_mlx4-ib-1-13@PCI_Bus_0000:10","T": epoch,"V":0},
        {"N":"servers.%s.interrupts.cpu10.PCI-MSI-edge_mlx4-ib-1-13@PCI_Bus_0000:10","T": epoch,"V":0},
        {"N":"servers.%s.interrupts.cpu11.PCI-MSI-edge_mlx4-ib-1-13@PCI_Bus_0000:10","T": epoch,"V":0},
        {"N":"servers.%s.interrupts.cpu12.PCI-MSI-edge_mlx4-ib-1-13@PCI_Bus_0000:10","T": epoch,"V":0},
        {"N":"servers.%s.interrupts.cpu13.PCI-MSI-edge_mlx4-ib-1-13@PCI_Bus_0000:10","T": epoch,"V":0},
        {"N":"servers.%s.cpu.cpu11.system","T": epoch,"V":0.18},
        {"N":"servers.%s.cpu.cpu9.idle","T": epoch,"V":90.3533333333},
        {"N":"servers.%s.cpu.cpu12.system","T": epoch,"V":1.1666666667},
        {"N":"servers.%s.cpu.cpu2.system","T": epoch,"V":0.0866666667},
        {"N":"servers.%s.cpu.cpu5.iowait","T": epoch,"V":0.0},
        {"N":"servers.%s.cpu.cpu15.softirq","T": epoch,"V":0.0},
        {"N":"servers.%s.cpu.cpu12.iowait","T": epoch,"V":0.0266666667},
        {"N":"servers.%s.cpu.cpu11.nice","T": epoch,"V":0.0},
        {"N":"servers.%s.cpu.cpu4.system","T": epoch,"V":0.0666666667},
        {"N":"servers.%s.cpu.cpu10.system","T": epoch,"V":0.09}]
        name = "tb00" + str(n)
        for m in data:
            m["H"] = name
            m["N"] = m["N"] % name
            send_agg_data(context, "tcp://127.0.0.1:5550", m)

