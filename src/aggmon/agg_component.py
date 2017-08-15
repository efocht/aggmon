import json
import logging
import os
import pdb
import subprocess
import time
import traceback
from agg_rpc import send_rpc, RPCThread, RPCNoReplyError
from agg_job_command import send_agg_command
from agg_helpers import *
from etcd_client import *
from repeat_timer import RepeatTimer
from hierarchy_helpers import hierarchy_from_url
from listener import Listener


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

    def del_data(self, path):
        """
        Delete component data.
        """
        if not path.startswith("/"):
            path = "/" + path
        path = self.etcd_path + "/data" + path
        try:
            data = self.etcd_client.read(path)
            self.etcd_client.delete(path, recursive=True, dir=data.dir)
        except EtcdKeyNotFound:
            return False
        except Exception as e:
            log.warning("Etcd error while retrieving data (%s): %r" % (path, e))
            return False
        return True

    def del_state(self, component_type, hierarchy_url=None):
        """
        Delete the state of a component immediately, don't wait for TTL expiration.
        If hierarchy_url is not passed, delet all component states of a certain
        component type.
        """
        if hierarchy_url is None:
            path = "/".join([ETCD_COMPONENT_PATH, component_type])
        else:
            hierarchy, component_id, hierarchy_path = hierarchy_from_url(hierarchy_url)
            path = "/".join([ETCD_COMPONENT_PATH, component_type, hierarchy, component_id, "state"])
        try:
            self.etcd_client.delete(path, recursive=True, dir=True)
        except EtcdKeyNotFound:
            return False
        except Exception as e:
            log.warning("Etcd error while deleting state (%s): %r" % (path, e))
            return False
        return True

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

    #@staticmethod
    def get_state(self, component_type, hierarchy_url):
        """
        Retrieve component instance state.
        """
        if hierarchy_url is None:
            path = "/".join([ETCD_COMPONENT_PATH, component_type])
        else:
            hierarchy, component_id, hierarchy_path = hierarchy_from_url(hierarchy_url)
            path = "/".join([ETCD_COMPONENT_PATH, component_type, hierarchy, component_id, "state"])
        try:
            return self.etcd_client.deserialize(path)
        except EtcdKeyNotFound:
            return None
        except Exception as e:
            log.warning("Etcd error while retrieving state (%s): %r" % (path, e))

    def iter_components_state(self, component_type=None):
        path = ETCD_COMPONENT_PATH
        if component_type is not None:
            path += "/" + component_type
        for r in self.etcd_client.read(path, recursive=True).children:
            if r.key.endswith("/state"):
                yield self.etcd_client.deserialize(r.key)

    def reset_timer(self):
        if self.timer is not None:
            self.timer.stop()
        # send one state ping message
        self.set_state()
        # and create new repeat timer
        self.timer = RepeatTimer(self.ping_interval, self.set_state)

    def set_state(self):
        try:
            log.debug("set_state: %s, %r" % (self.etcd_path + "/state", self.state))
            return self.etcd_client.update(self.etcd_path + "/state", self.state,
                                           ttl=int(self.ping_interval*1.5))
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

    def update_state_cache(self, state):
        """
        Update the internal state.
        """
        self.state.update(state)


class ComponentDeadError(Exception):
    pass

#
# Component control
# 
#
class ComponentControl(object):
    """
    Component states repository: this is the place where component states are stored in etcd
    """
    def __init__(self, config, etcd_client, comp_state):
        self.etcd_client = etcd_client
        self.config = config
        self.comp_state = comp_state
        self.component_start_cb = {}
        self.component_kill_cb = {}


    def start_component(self, service, hierarchy_url,
                        __CALLBACK=None, __CALLBACK_ARGS=[], **kwds):
        """
        Starts a component. Called from control to start collectors, aggregators, data_stores.

        'component_type' is one of collector, data_store, job_agg, group_agg (maybe we
        join the aggregators). It could be renamed to 'service', to be consistent with
        the config, or we could rename service to component_type in the config. (TODO!)

        'hierarchy_url' specifies location of component in a certain hierarchy.

        """
        hierarchy, hierarchy_key, hierarchy_path = hierarchy_from_url(hierarchy_url)
        if hierarchy_key not in self.config.get("/hierarchy/%s" % hierarchy):
            log.error("start_component: hierarchy_key '%s' not found in configuration!" % hierarchy_key)
            return False
        if service not in self.config.get("/services"):
            log.error("start_component: service '%s' not found in configuration!" % service)
            return False
        locals().update(kwds)
        svc_info = self.config.get("/services/%s" % service)
        cwd = svc_info["cwd"]
        cmd_opts = svc_info["cmd_opts"]
        if "config_add" in svc_info:
            add_config = self.config.get(svc_info["config_add"])
            locals().update(add_config)
        if "listen_port_range" in svc_info:
            listen = "tcp://0.0.0.0:%s" % svc_info["listen_port_range"]
        # register callback
        if __CALLBACK is not None:
            key = service + ":" + hierarchy_url
            self.component_start_cb[key] = {"cb": __CALLBACK, "args": __CALLBACK_ARGS}
        try:
            if "logfile" in svc_info:
                logfile = svc_info["logfile"] % locals()
            cmd = which(svc_info["cmd"])
            if cmd is None:
                log.error("Could not find command '%s' in PATH!" % svc_info["cmd"])
                return
            cmd_opts = cmd_opts % locals()
            cmd = cmd + " " + cmd_opts
            exec_cmd = self.config.get("/global/local_cmd") % locals()
        except KeyError as e:
            log.error("Configuration error: service %s :%r" % (service, e))
            return

        log.info("starting subprocess: %s" % exec_cmd)
        try:
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
        res = False

        state = self.comp_state.get_state(service, hierarchy_url)
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
        status = self._process_status(state)
        if status == "not found":
            res = self.comp_state.del_state(service, hierarchy_url)
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
                exec_cmd = self.config.get("/global/local_kill") % state
                out = subprocess.check_output(exec_cmd, stderr=subprocess.STDOUT, shell=True)
                log.debug("kill_component (kill) res=%r" % res)
            except Exception as e:
                log.error("subprocess error when running '%s' : '%r'" % (exec_cmd, e))
        return res


    def _process_status(self, component_state):
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

    #
    # This method is obsolete if TTL expiration works properly.
    #
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

    #
    # TODO: ?? is this still needed?
    #
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




