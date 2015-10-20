from agg_rpc import send_rpc
from repeat_timer import RepeatTimer
from repeat_timer import RepeatTimer
import logging


log = logging.getLogger( __name__ )
DEFAULT_PING_INTERVAL = 60



def get_kwds(**kwds):
    return kwds


class ComponentState(object):
    """
    Component side state. A timer is instantiated and sends component state information periodically.
    The state itself is in the dict "state".
    """
    def __init__(self, zmq_context, dispatcher, ping_interval=DEFAULT_PING_INTERVAL, state={}):
        self.zmq_context = zmq_context
        self.dispatcher = dispatcher
        self.ping_interval = ping_interval
        self.state = state
        self.timer = None
        self.reset_timer()

    def reset_timer(self, *__args, **__kwds):
        if self.timer is not None:
            self.timer.stop()
        # send one state ping message
        self.send_state_update()
        # and create new repeat timer
        self.timer = RepeatTimer(self.ping_interval, self.send_state_update)

    def send_state_update(self):
        send_rpc(self.zmq_context, self.dispatcher, "set_component_state", **self.state)
        
    def update(self, state):
        self.state.update(state)
