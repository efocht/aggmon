import json
import logging
import os
import pdb
import subprocess
import time
from agg_rpc import send_rpc, zmq_own_addr_for_tgt
from agg_job_command import send_agg_command
from repeat_timer import RepeatTimer


log = logging.getLogger( __name__ )
DEFAULT_PING_INTERVAL = 60


def get_kwds(**kwds):
    return kwds


# equivalent of the UNIX 'which' command
def which(file):
    for path in os.environ["PATH"].split(os.pathsep):
        if os.path.exists(os.path.join(path, file)):
                return os.path.join(path, file)
    return None


def component_key(keys, kwds):
    key = []
    for k in keys:
        if k in kwds:
            key.append(kwds[k])
        elif len(key) > 0:
            return ":".join(key) + ":"
    return ":".join(key)


def check_output(*popenargs, **kwargs):
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        error = subprocess.CalledProcessError(retcode, cmd)
        error.output = output
        raise error
    return output


# trick for adding check_output on python < 2.7
try:
    subprocess.check_output
except:
    subprocess.check_output = check_output


def group_name(group):
    return group.lstrip("/").replace("/", "_")


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
        assert isinstance(self.state, dict), "ComponentState: state must be a dict!"
        self.state["pid"] = os.getpid()
        self.state["started"] = time.time()
        self.state["host"] = zmq_own_addr_for_tgt('8.8.8.8')
        self.state["ping_interval"] = self.ping_interval
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


class ComponentStatesRepo(object):
    """
    Component states repository: this is the place where component states that are
    received by the controller are stored.
    """
    def __init__(self, config, dispatcher, zmq_context):
        self.zmq_context = zmq_context
        self.dispatcher = dispatcher
        self.repo = {}
        self.config = config
        self.component_start_cb = {}
        self.component_kill_cb = {}


    def start_component(self, service, group_path, __CALLBACK=None, __CALLBACK_ARGS=[], **kwds):
        """
        """
        if group_path not in self.config["groups"]:
            log.error("start_component: group '%s' not found in configuration!" % group_path)
            return False
        if service not in self.config["services"]:
            log.error("start_component: service '%s' not found in configuration!" % service)
            return False
        group = group_name(group_path)
        nodes_key = "%s_nodes" % service
        if nodes_key not in self.config["groups"][group_path]:
            log.error("start_component: '%s' not found in configuration of group '%s'!" % (nodes_key, group_path))
            return False
        nodes = self.config["groups"][group_path][nodes_key]
        assert(isinstance(nodes, list))
        locals().update(kwds)
        if "database" in self.config:
            if isinstance(self.config["database"], dict):
                locals().update(self.config["database"])
        svc_info = self.config["services"][service]
        cwd = svc_info["cwd"]
        cmd = svc_info["cmd"]
        cmd_opts = svc_info["cmd_opts"]
        if "cmdport_range" in svc_info:
            cmdport = "tcp://0.0.0.0:%s" % svc_info["cmdport_range"]
        if "listen_port_range" in svc_info:
            listen = "tcp://0.0.0.0:%s" % svc_info["listen_port_range"]
        if "logfile" in svc_info:
            logfile = svc_info["logfile"] % locals()
        state_file = "/tmp/state_%(service)s_%(group)s" % locals()
        # register callback
        if __CALLBACK is not None:
            key = service + ":" + component_key(self.config["services"][service]["component_key"], kwds)
            self.component_start_cb[key] = {"cb": __CALLBACK, "args": __CALLBACK_ARGS}
        for host in nodes:
            try:
                cmd = which(cmd)
                cmd_opts = cmd_opts % locals()
                cmd = cmd + " " + cmd_opts
                exec_cmd = self.config["global"]["remote_cmd"] % locals()
                log.info("starting subprocess: %s" % exec_cmd)
                out = subprocess.check_output(exec_cmd, stderr=subprocess.STDOUT, shell=True)
                log.info("output: %s" % out)
                break
            except Exception as e:
                log.error("subprocess error '%r'" % e)
                log.error("subprocess error when running '%s' : '%r'" % (exec_cmd, e))
                # trying the next node, if any
    
    
    def kill_component(self, service, group_path, __CALLBACK=None, __CALLBACK_ARGS=[], METHOD="msg", **kwds):
        global pargs

        msg = {"component": service, "group": group_path}
        msg.update(kwds)
        res = False

        group = group_name(group_path)
        state = self.get_state(msg)
        if state is None:
            log.warning("component '%s' state not found. Don't know how to kill it." % service)
            return False
        if METHOD == "msg":
            if "cmd_port" in state:
                # send "quit" cmd over RPC
                reply = send_rpc(self.zmq_context, state["cmd_port"], "quit")
                if reply is not None:
                    res = True
            elif "listen" in state:
                # send "quit" cmd over PULL port
                send_agg_command(self.zmq_context, state["listen"], "quit")
                res = True
        else:
            # kill process using the remembered pid. This could be dangerous as we could kill another process.
            try:
                exec_cmd = self.config["global"]["remote_kill"] % state
                out = subprocess.check_output(exec_cmd, stderr=subprocess.STDOUT, shell=True)
                #res = del_state(msg)
                send_rpc(self.zmq_context, self.dispatcher, "del_component_state", **msg)
            except Exception as e:
                log.error("subprocess error when running '%s' : '%r'" % (exec_cmd, e))
                res = False
        return res


    def kill_components(self, component_types):
        for group_path in self.config["groups"]:
            for comp_type in component_types:
                c = self.get_state({"component": comp_type, "group": group_path})
                if c is not None:
                    if "component" in c:
                        self.kill_component(comp_type, group_path, METHOD="kill")
                    else:
                        log.debug("components: %r" % c)
                        for jobid, jagg in c.items():
                            self.kill_component(comp_type, group_path, jobid=jobid, METHOD="kill")


    def set_state(self, msg):
        """
        """
        log.debug("set_state: msg %r" % msg)
        #pdb.set_trace()
        if "component" not in msg:
            log.error("set_state message has no component!? %r" % msg)
            # TODO raise ComponentMsgError
            return None
        component = msg["component"]
        if component not in self.repo:
            self.repo[component] = {}
        # now make a meaningful minimal unique key
        key = component_key(self.config["services"][component]["component_key"], msg)
        started = None
        if key in self.repo[component]:
            log.debug("set_state: updating state for %s %s" % (component, key))
            started = self.repo[component][key]["started"]
        else:
            log.debug("set_state: setting state for %s %s" % (component, key))
        msg["last_update"] = time.time()
        if started is not None:
            if started != msg["started"]:
                msg["restart!"] = True
        self.repo[component][key] = msg
        cbkey = component + ":" + key
        if cbkey in self.component_start_cb:
            cb = self.component_start_cb[cbkey]["cb"]
            args = self.component_start_cb[cbkey]["args"]
            try:
                cb(*args)
            except Exception as e:
                log.error("start_cb error: %r" % e)
            del self.component_start_cb[cbkey]


    def get_state(self, msg):
        """
        """
        log.debug("get_state: msg %r" % msg)
        if "component" not in msg:
            return self.repo
        component = msg["component"]
        if component not in self.repo:
            return {}
        key = component_key(self.config["services"][component]["component_key"], msg)
        if len(key) > 0:
            if key in self.repo[component]:
                return self.repo[component][key]
            else:
                # try to match for it
                for k in self.repo[component].keys():
                    if k.startswith(key):
                        return self.repo[component][k]
        else:
            # return all components of this type
            return self.repo[component]


    def del_state(self, msg):
        """
        """
        log.info("del_state: msg %r" % msg)
        if "component" not in msg:
            return False
        component = msg["component"]
        if component not in self.repo:
            return False
        key = component_key(self.config["services"][component]["component_key"], msg)
        if len(key) > 0:
            fullkey = None
            if key in self.repo[component]:
                fullkey = key
            else:
                # try to match for it
                for k in self.repo[component].keys():
                    if k.startswith(key):
                        fullkey = k
                        break
            if fullkey is not None:
                log.info("del_state: deleting state for '%s' -> '%s'" % (component, fullkey))
                del self.repo[component][fullkey]
                # callback handling
                cbkey = component + ":" + fullkey
                if cbkey in self.component_kill_cb:
                    cb = self.component_kill_cb[cbkey]["cb"]
                    args = self.component_kill_cb[cbkey]["args"]
                    try:
                        cb(*args)
                    except Exception as e:
                        log.error("kill_cb error: %r" % e)
                    del self.component_kill_cb[cbkey]
    
                if self.repo[component] == {}:
                    del self.repo[component]
                return True
        return False

    def request_resend(self, state):
        res = False
        if "cmd_port" in state:
            # send "quit" cmd over RPC
            reply = send_rpc(self.zmq_context, state["cmd_port"], "resend_state")
            if reply is not None:
                res = True
        elif "listen" in state:
            # send "quit" cmd over PULL port
            send_agg_command(self.zmq_context, state["listen"], "resend_state")
            res = True
        return res

    def load_state(self, state_file, mode="keep"):
        """
        Load component state from disk or database.
        Parameters:
        state_file: the file name where the state is saved
        mode: load mode: can be:
              "keep": keep running components
              "restart": restart running components
              ...?
        """
        log.debug("load_state from %s" % state_file)
        try:
            fp = open(state_file)
            loaded = json.load(fp)
            fp.close()
        except Exception as e:
            log.error("Exception in state load '%s': %r" % (state_file, e))
            return None

        if len(loaded) == 0 or len(loaded[0]) == 0:
            return None
        loaded_state = loaded[0]
        #
        # only mode == "keep" is implemented
        #
        # request resend of state from all components
        for component, compval in loaded_state.items():
            self.repo[component] = compval
            for ckey, cstate in compval.items():
                # set the "outdated!" attribute
                # it will disappear if the component sends a component update message
                # thus it is used for marking non-working components
                self.repo[component][ckey]["outdated!"] = True
                self.request_resend(cstate)
                time.sleep(0.05)
        return True

    def save_state(self, __msg, state_file):
        """
        Save component state to disk (or database).
        Parameters:
        __msg: ignored
        state_file: path to state file
        """
        log.debug("save_state to %s" % state_file)
        try:
            fp = open(state_file, "w")
            json.dump([self.repo], fp)
            fp.close()
        except Exception as e:
            log.error("Exception in state save '%s': %r" % (state_file, e))
            return False
        return True

    def component_wait_timeout(self, component, num, timeout=120):
        """
        Wait until 'num' components of the given type have appeared
        or timeout was reached.
        """
        tstart = time.time()
    
        while component not in self.repo or len(self.repo[component].keys()) < num:
            time.sleep(0.1)
            if time.time() - tstart > timeout:
                return False
        return True




