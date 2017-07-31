import json
import logging
import os
import pdb
import subprocess
import time
import traceback
from etcd_rpc import send_rpc, own_addr_for_tgt, RPCThread, RPCNoReplyError
from agg_job_command import send_agg_command
from etcd_client import *
from repeat_timer import RepeatTimer


log = logging.getLogger( __name__ )
ETCD_COMPONENT_PATH = "/component"
DEFAULT_PING_INTERVAL = 60
KILL_WAIT_TIMEOUT = 10


def get_kwds(**kwds):
    return kwds


# equivalent of the UNIX 'which' command
def which(file):
    for path in os.environ["PATH"].split(os.pathsep):
        if os.path.exists(os.path.join(path, file)):
                return os.path.join(path, file)
    return None


def component_key(keys, kwds):
    keys = keys.split(":")
    key = []
    for k in keys:
        if k in kwds:
            key.append(kwds[k])
        elif len(key) > 0:
            return ":".join(key) + ":"
    return ":".join(key)


def hierarchy_from_url(url):
    """
    Decode hierarchy and hierarchy_key from a hierarchy URL.

    The URL has the format:
    <hierarchy_name>:<hierarchy_path>
    for example:
    monitor:/universe/rack2

    This function returns a tuple made of the hierarchy name and a flattened "hierarchy_key",
    for example: ("universe", "universe_rack2")
    The hierarchy_key can be used as a shard key in the data stores.
    """
    hierarchy = None
    key = None
    path = None
    if url.find(":") > 0:
        hierarchy = url[: url.find(":")]
        path = url[url.find(":") + 1 :]
    key = "_".join(path.split("/")[1:])
    return hierarchy, key, path


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
    Component side state.
    A timer is instantiated and sends component state information periodically.
    The state is in the dict "state" and is periodically serialized into etcd
    eg. /component/group/universe_rack1/state.
    Data that should be saved across restarts of the component is stored upon
    request.
    """
    this = None

    def __init__(self, etcd_client, component_type, hierarchy_url,
                 ping_interval=DEFAULT_PING_INTERVAL, state={}):
        self.etcd_client = etcd_client
        self.component_type = component_type
        self.hierarchy_url = hierarchy_url
        hierarchy, component_id, hierarchy_path = hierarchy_from_url(hierarchy_url)
        self.hierarchy = hierarchy
        self.component_id = component_id
        self.etcd_path = "/".join([ETCD_COMPONENT_PATH, component_type, hierarchy, component_id])
        # create component state directory
        try:
            self.etcd_client.write(self.etcd_path, None, dir=True, prevExist=False)
        except EtcdAlreadyExist:
            log.warning("etcd directory path for %s %s exists. Reusing." % (component_type, component_id))

        self.ping_interval = ping_interval
        self.state = state
        assert isinstance(self.state, dict), "ComponentState: state must be a dict!"
        self.state["component"] = component_type
        self.state["id"] = component_id
        self.state["pid"] = os.getpid()
        self.state["hierarchy_url"] = hierarchy_url
        self.state["started"] = time.time()
        self.state["host"] = own_addr_for_tgt('8.8.8.8')
        self.state["ping_interval"] = self.ping_interval
        self.state["rpc_path"] = self.etcd_path + "/rpc"
        self.timer = None
        ComponentState.this = self
        self.rpc = RPCThread(etcd_client, self.etcd_path + "/rpc")

    def get_data(self, path):
        """
        Retrieve component data.
        """
        if not path.startswith("/"):
            path = "/" + path
        try:
            return self.etcd_client.deserialize(self.etcd_path + "/data" + path)
        except EtcdKeyNotFound:
            return None
        except Exception as e:
            log.warning("Etcd error while retrieving data (%s): %r" % (path, e))

    def iter_components_state(self, component_type=None):
        path = ETCD_COMPONENT_PATH
        if component_type is not None:
            path += "/" + component_type
        for r in self.etcd_client.read(path, recursive=True).children:
            if r.key.endswith("/state"):
                yield self.etcd_client.get(r.key)

    def reset_timer(self, *__args, **__kwds):
        if self.timer is not None:
            self.timer.stop()
        # send one state ping message
        self.send_state_update()
        # and create new repeat timer
        self.timer = RepeatTimer(self.ping_interval, self.send_state_update)

    def send_state_update(self):
        try:
            return self.etcd_client.update(self.etcd_path + "/state", self.state,
                                           ttl=int(self.ping_interval*1.3))
        except Exception as e:
            log.warning("Etcd error at state update: %r" % e)

    def set_data(self, path, data):
        """
        Store component data.
        """
        if not path.startswith("/"):
            path = "/" + path
        try:
            return self.etcd_client.update(self.etcd_path + "/data" + path, data)
        except Exception as e:
            log.warning("Etcd error while saving data (%s): %r" % (path, e))

    def start(self):
        self.rpc.start()
        self.reset_timer()

    def update_state(self, state):
        """
        Update the internal state.
        """
        self.state.update(state)


class ComponentDeadError(Exception):
    pass

#
# The class below is here for reference. In the current etcd based code this should not be needed,
# except for some of the methods which start and kill components.
#
class ComponentStatesRepo(object):
    """
    Component states repository: this is the place where component states are stored in etcd
    """
    def __init__(self, config, etcd_client):
        self.etcd_client = etcd_client
        #self.repo = {}            # not needed any more, data is in etcd
        self.config = config
        self.component_start_cb = {}
        self.component_kill_cb = {}


    def start_component(self, component_type, hierarchy_url,
                        __CALLBACK=None, __CALLBACK_ARGS=[], **kwds):
        """
        Starts a component. Called from control to start collectors, aggregators, data_stores.

        'component_type' is one of collector, data_store, job_agg, group_agg (maybe we
        join the aggregators). It could be renamed to 'service', to be consistent with
        the config, or we could rename service to component_type in the config. (TODO!)

        'hierarchy' is the hierarchy name this component is working on.

        'hierarchy_key' is the shard key for the flattened hierarchy corresponding to the
        place where the comnponent is positioned. For the monitoring hierarchy this is the
        flattened group. For the job hierarchy it is the job ID. Further details on the hierarchy
        are in the /config/hierarchy/<hierarchy_key> value.

        """
        hierarchy, hierarchy_key, hierarchy_path = hierarchy_from_url(hierarchy_url)
        if hierarchy_key not in self.config.get("/hierarchy/%s" % hierarchy):
            log.error("start_component: hierarchy_key '%s' not found in configuration!" % hierarchy_key)
            return False
        if component_type not in self.config.get("/services"):
            log.error("start_component: service '%s' not found in configuration!" % component_type)
            return False
        locals().update(kwds)
        svc_info = self.config.get("/services/%s" % service)
        cwd = svc_info["cwd"]
        cmd = svc_info["cmd"]
        cmd_opts = svc_info["cmd_opts"]
        if "listen_port_range" in svc_info:
            listen = "tcp://0.0.0.0:%s" % svc_info["listen_port_range"]
        if "logfile" in svc_info:
            logfile = svc_info["logfile"] % locals()
        state_file = "/tmp/state_%(service)s_%(hierarchy)s_%(hierarchy_key)s" % locals()
        # register callback
        if __CALLBACK is not None:
            key = service + ":" + hierarchy_url
            self.component_start_cb[key] = {"cb": __CALLBACK, "args": __CALLBACK_ARGS}
        try:
            cmd = which(cmd)
            cmd_opts = cmd_opts % locals()
            cmd = cmd + " " + cmd_opts
            exec_cmd = self.config.get("/global/local_cmd") % locals()
            log.info("starting subprocess: %s" % exec_cmd)
            out = subprocess.check_output(exec_cmd, stderr=subprocess.STDOUT, shell=True)
            log.info("output: %s" % out)
        except Exception as e:
            log.error(traceback.format_exc())
            log.error("subprocess error '%r'" % e)
            log.error("subprocess error when running '%s' : '%r'" % (exec_cmd, e))
            # trying the next node, if any


    def kill_component(self, service, hierarchy_url, __CALLBACK=None, __CALLBACK_ARGS=[], **kwds):
        """
        Kill a component. First attempt is by sending it a "quit" command.
        This sets the "soft-fill" flag in the component state. When this flag is found at
        a subsequent kill attempt, the kill will attempt to kill the process of the
        component (hard kill).
        """
        msg = {"component": service, "hierarchy_url": hierarchy_url}
        msg.update(kwds)
        res = False

        state = self.get_state(msg)
        if state is None:
            log.warning("component '%s' state not found. Don't know how to kill it." % service)
            return False
        # register callback
        if __CALLBACK is not None:
            key = service + ":" + hierarchy_url
            self.component_kill_cb[key] = {"cb": __CALLBACK, "args": __CALLBACK_ARGS}
        if "cmd_port" not in state:
            log.error("cmd_port not found in state: %r" % state)
            return False

        # is the process running? check with "ps"
        status = self.process_status(state)
        if status == "not found":
            res = self.del_state(msg)
            log.debug("kill_component deleting state %r" % res)
            return True
        elif status == "unknown":
            log.error("process status found as '%s'. Aborting kill." % status)
            return False
        
        # send "quit" cmd over RPC
        try:
            reply = send_rpc(self.etcd_client, state["rpc_path"], "quit")
            res = True
        except RPCNoReplyError as e:
            log.warning("Component did not reply, maybe it is dead already? %r" % e)
            reply = None

        # wait ... seconds and check if process has disappeared
        start_wait = time.time()
        while time.time() - start_wait < KILL_WAIT_TIMEOUT:
            status = self.process_status(state)
            #
            # This wait loop is disabled right now (by the -1000).
            # It took too long for the processes to die.
            #
            break
            if status == "running":
                time.sleep(1)
            elif status == "not found":
                log.info("status changed to 'not found'")
                break
            elif status == "unknown":
                return False
        log.info("status for pid=%d is '%s'" % (state["pid"], status))
        if status == "running":
            # kill process using the remembered pid. This could be dangerous as we could kill another process.
            res = True
            try:
                log.info("attempting hard-kill of pid %d" % state["pid"])
                exec_cmd = self.config.get("/global/remote_kill") % state
                out = subprocess.check_output(exec_cmd, stderr=subprocess.STDOUT, shell=True)
                #send_rpc(self.zmq_context, self.dispatcher, "del_component_state", **msg)
                log.debug("kill_component (kill) res=%r" % res)
            except Exception as e:
                log.error("subprocess error when running '%s' : '%r'" % (exec_cmd, e))
        if res:
            res = self.del_state(msg)
            log.debug("kill_component deleting state %r" % res)
        return res


    #def kill_components(self, component_types):
    #    for group_path in self.config.get("/groups").keys():
    #        for comp_type in component_types:
    #            c = self.get_state({"component": comp_type})
    #            if c is not None:
    #                if "component" in c:
    #                    log.info("killing component %r" % c)
    #                    self.kill_component(comp_type, group_path, METHOD="kill")
    #                else:
    #                    log.debug("components: %r" % c)
    #                    for jobid, jagg in c.items():
    #                        log.info("killing job_agg component %s" % jobid)
    #                        self.kill_component(comp_type, group_path, jobid=jobid, METHOD="kill")


    def process_status(self, component_state):
        """
        Find out process status returned by the ps command, return "running", "not found" or "unkown".
        """
        if "pid" not in component_state:
            log.warning("pid not found in component state: %r" % component_state)
            return None
        status = "unknown"
        try:
            pid = component_state["pid"]
            host = component_state["host"]
            service = component_state["component"].replace("_", "")
            exec_cmd = self.config.get("/global/local_status") % locals()
            log.info("starting subprocess: %s" % exec_cmd)
            out = subprocess.check_output(exec_cmd, stderr=subprocess.STDOUT, shell=True)
            log.info("output: '%s'" % out)
            if out.find(service) >=0:
                status = "running"
            else:
                status = "not found"
        except Exception as e:
            log.error(traceback.format_exc())
            log.error("subprocess error '%r'" % e)
            log.error("subprocess error when running '%s' : '%r'" % (exec_cmd, e))
            # trying the next node, if any
        return status


    def get_state(self, msg):
        """
        Get the state of a component or all components of the same type.

        'msg' is a dict that must contain at least a "component" key. If it also contains a
        "hierarchy_url" key, then a particular instance of a component is looked up. Otherwise
        all instances of the given component type are returned.
        """
        log.debug("get_state: msg %r" % msg)
        path = ETCD_COMPONENT_PATH
        if "component" in msg:
            component = msg["component"]
            path += "/" + component
            
            if "hierarchy_url" in msg:
                hierarchy_url = msg["hierarchy_url"]
                hierarchy, hierarchy_key, hierarchy_path = hierarchy_from_url(hierarchy_url)
                path += "/" + hierarchy + "/" + hierarchy_key
                return self.etcd_client.get(path + "/state")
            elif "hierarchy" in msg:
                path += "/" + hierarchy

        result = []
        for c in self.etcd_client.read(path, recursive=True).children:
            if not c.dir and c.key.endswith("/state"):
                result.append(c.value)
        return result


    def del_state(self, msg):
        """
        """
        log.debug("del_state: msg %r" % msg)
        if "component" not in msg:
            log.error("del_state: 'component' not in msg %r" % msg)
            return False
        component = msg["component"]
        if component not in self.repo:
            log.error("del_state: component '%s' not in msg %r" % (component, msg))
            return False
        key = component_key(self.config.get("/services/%s/component_key" % component), msg)
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

    def check_outdated(self):
        outdated = {}
        now = time.time()
        for component in self.repo.keys():
            outdated[component] = []
            for key, state in self.repo[component].items():
                if "outdated!" in state:
                    outdated[component].append(state)
                    continue
                if now - state["last_update"] > 2 * state["ping_interval"]:
                    self.repo[component][key]["outdated!"] = 1
                    outdated[component].append(state)
        return outdated

    def request_resend(self, state):
        res = False
        if "rpc_path" in state:
            # send "quit" cmd over RPC
            log.info("requesting resend_state from %s" % state["rpc_path"])
            reply = send_rpc(self.etcd_client, state["rpc_path"], "resend_state")
            if reply is not None:
                res = True
        return res

    def load_state(self, state_file):
        """
        Load component state from disk or database.
        Parameters:
        state_file: the file name where the state is saved
        """
        if not os.path.exists(state_file):
            return None
        log.info("load_state from %s" % state_file)
        try:
            fp = open(state_file)
            loaded = json.load(fp)
            fp.close()
        except Exception as e:
            log.error("Exception in state load '%s': %r" % (state_file, e))
            return None

        log.debug("loaded: %r" % loaded)
        if len(loaded) == 0 or len(loaded[0]) == 0:
            return None
        loaded_state = loaded[0]
        if len(loaded_state) == 0:
            return None
        #
        # request resend of state from all components
        for component, compval in loaded_state.items():
            self.repo[component] = compval
            for ckey, cstate in compval.items():
                # set the "outdated!" attribute
                # it will disappear if the component sends a component update message
                # thus it is used for marking non-working components
                self.repo[component][ckey]["outdated!"] = True
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




