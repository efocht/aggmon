# aggmon

## Overview
TODO

## Basic Configuration
Aggmon configuration files are by default stored in a directory named _/etc/aggmon_. All (valid) [YAML](https://en.wikipedia.org/wiki/YAML) files located there are
parsed and build together the aggmon configurataion. Usually there is a _config.yaml_ file which contains the basic
configuration within a block named __config__. Configuration files are read and parsed by _agg_control_ and are passed to
other aggmon components like aggregators.

Example basic aggmon configuration:

```yaml
config:
    groups:
        /universe:
            job_agg_nodes:    [localhost]
            data_store_nodes: [localhost]
            collector_nodes:  [localhost]
    services:
        collector:
            cmdport_range: 5100-5199
            listen_port_range: 5262
            logfile: /tmp/%(service)s_%(group)s.log
        data_store:
            cmd_opts: --cmd-port %(cmdport)s --listen %(listen)s --dbname %(dbname)s --host %(dbhost)s --group %(group_path)s --dispatcher %(dispatcher)s --backend mongodb,influxdb %(msgbus_opts)s
            cmdport_range: 5100-5199                                                                                                                                                            
            listen_port_range: 5200-5299                                                                                                                                                        
            logfile: /tmp/%(service)s_%(group)s.log                                                                                                                                             
        job_agg:                                                                                                                                                                                
            cmdport_range: 5000-5999                                                                                                                                                            
            listen_port_range: 5300-5999                                                                                                                                                        
            logfile: /tmp/%(service)s_%(jobid)s.log                                                                                                                                             
    database:                                                                                                                                                                                   
        dbname: metricdb                                                                                                                                                                        
        dbhost: ""                                                                                                                                                       
        user: ""                                                                                                                                                                                
        password: ""                                                                                                                                                                            
    global:                                                                                                                                                                                     
        local_cmd:   cd %(cwd)s; %(cmd)s >%(logfile)s 2>&1 &                                                                                                                                    
        remote_cmd:  'ssh %(host)s "cd %(cwd)s; %(cmd)s >%(logfile)s 2>&1 &"'                                                                                                                   
        remote_kill: ssh %(host)s kill %(pid)d                                                                                                                                                  
    resource_manager:                                                                                                                                                                           
        type: pbs                                                                                                                                                                               
        pull_state_cmd: '/usr/bin/pbsnodes'                                                                                                      
        master: none
```

Aggmon can use two alternative backends to store metric data: [InfluxDB](https://en.wikipedia.org/wiki/InfluxDB) and [MongoDB](https://en.wikipedia.org/wiki/MongoDB).
By default aggmon uses a MongoDB as backend. Alternatively aggmon can store data in InfluxDB which requires to add
a __cmd_opts__ block under __data_store__ and set the __--backend__ command line option to __influxdb__. It is also
possible to send metrics to both backends at the same time which requires to specify the __--backend__ option as a
komma separated list of all backends like shown in the following example:

```yaml
data_store:
            cmd_opts: --cmd-port %(cmdport)s --listen %(listen)s --dbname %(dbname)s --host %(dbhost)s --group %(group_path)s --dispatcher %(dispatcher)s --backend mongodb,influxdb %(msgbus_opts)s
            ...
```        

Non default ports of the backends can be specified by adding another command line option, e.g. __--port 27017,8085__ to the
__cmd_opts__ block.



## Aggregation
Aggregation is defined as part of the aggmon configuration and is by default located in _/etc/aggmon/aggregate.yaml_ and
_/etc/aggmon/agg-template.yaml_.

Example aggregation definition:

```yaml
agg:
    - template:        default
      push_target:     "@TOP_STORE"
      agg_metric_name: "%(metric)s_%(agg_type)s"
      metrics:         [load_one]
    - template:        [default,long-interval]
      push_target:     "@TOP_STORE"
      agg_metric_name: "%(metric)s_%(agg_type)s"
      metrics:         [cpu_temp]
    - push_target:     "@TOP_STORE"
      agg_class:       job
      interval:        180
      agg_type:        quant10
      ttl:             180
      agg_metric_name: "%(metric)s_%(agg_type)s"
      metrics:         RE:.*likwid.*

agg-templates:
     default:
         agg_class:       job
         interval:        120
         agg_type:        avg
         ttl:             120

     long-interval:
         interval:        300
```

Each element in the list defined in the __agg__ block leads to an aggregation call.
Every call of an aggregation function shares the specified settings like time interval and value type in the
__agg-templates__ block.
Metrics which should be aggregated are defined in the block __metrics__ which could be either a list of exact metric
names (__[metric_name1,metric_name2,...]__) or a regular expression (__RE:.\*part_of_metric_name.\*__). The method or
function which will be used to calculate the aggregated values is specified with the __agg_type__ block. The resulting
metric will be named like set in __agg_metric_name__. Note, that this block can contain place holders __metric__ which
refers to the name and __agg_type__ which represents the string specified in the __agg_type__ block. 
In the example above the first call aggregates a single metric called "load_one" by building the average value in an
intervall of 300 seconds and sends this value with the name of "load_one_avg" to the backend.

Aggregation functions can be easily added to aggmon. At the moment there are some predefined functions like
(__agg_type__) __min__, __max__, __sum__, __avg__, __quant10__.
