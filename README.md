# aggmon

## Overview

Aggmon is a framework aimed for very large scale system monitoring with flexible aggregation. It achieves scalability by using a distributed architecture resembling a natural monitoring hierarchy. Aggmon is receiving monitoring data from the monitored nodes, is processing, tagging, aggregating and storing it. The components are linked with publish-subscribe mechanisms built on top of ZeroMQ.

Data collection is done at node level by components like ganglia gmond or diamond. Nodes are grouped and send their metrics to an aggmon collector instance that is responsible for their group. 

Data store components subscribe to the collectors and store monitoring data in one or multiple databases. Data store components are organized according to a hierarchy and shard the monitoring data in order to achieve scalability.

Aggregators subscribe to the collectors and push the aggregated results to the data stores.

The aggmon controller is keeping track of the state of aggmon components, spawns, kills or re-spawns them in case of failure.

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

An example for aggregation definitions is below:

```yaml
# /etc/aggmon/aggregate.yaml
agg:
    - template:        default
      push_target:     "@TOP_STORE"
      agg_metric_name: "%(metric)s_%(agg_type)s"
      metrics:         [load_one]
    - template:        [default,long-interval]
      push_target:     "@TOP_STORE"
      agg_type:        max
      agg_metric_name: "%(metric)s_%(agg_type)s"
      metrics:         [cpu_temp]
    - push_target:     "@TOP_STORE"
      agg_class:       job
      interval:        180
      agg_type:        quant10
      ttl:             180
      agg_metric_name: "%(metric)s_%(agg_type)s"
      metrics:         RE:.*likwid.*
```
```yaml
# /etc/aggmon/agg-template.yaml
agg-templates:
     default:
         agg_class:       job
         interval:        120
         agg_type:        avg
         ttl:             120

     long-interval:
         interval:        300
```

Each element in the list defined in the __agg__ section leads to an aggregation block for one or multiple metrics. The attributes defining an aggregation block are:
- __template__: python list of aggregation templates to inherit from. Aggregation templates are usually defined in _/etc/aggmon/agg-template.yaml_.
- __agg_class__: the aggregation class. Currently only the "job" aggregation is implemented.
- __agg_type__: specifies the aggregation function used within the current block. Currently implemented are: __min__, __max__, __sum__, __avg__, __quant10__.
- __interval__: time interval of aggregation in seconds.
- __ttl__: time to live for the aggregated metric.
- __metrics__: the metrics that shall be aggregated, either specified as a list of exact metric
names (__[metric_name1,metric_name2,...]__) or a string prefixed by "RE:" and the remainder representing a regular expression (__RE:.\*part_of_metric_name.\*__).
- __agg_metric_name__: a python template for generating the aggregated metric's name. Besides the attribute names one can use the variable __metric__ in the template for including the original metric name.
- __push_target__: specifies where to push the aggregated metric to. This can be the ZeroMQ URI of the listen port of one of the components or a symbolic name starting with "@". Currently only the symbolic push target __@TOP_STORE__ is implemented. It refers to the top group's data store.

The aggregation class "job" is aggregating values only from the member nodes of a job. This wokrs only when jobs are using dedicated nodes and don't share nodes. The job aggregators subscribe to all collectors but receive only metrics tagged with the job ID they care for. For each job an own instance of a job aggregator will be spawned, it caches the latest metrics from the nodes of the job.

In the example above the first block aggregates a single metric called "load_one" by building the average value every 
300 seconds and sends the resulting metric with the name of "load_one_avg" to the top level data store. It uses the _default_ template.

The second aggregation block computes the maximum cpu temperature (__cpu_temp__) for the nodes of a job. It inherits attributes from two templates and overwrites the __agg_type__.

The third example is aggregating all metrics that contain the string "likwid" in their name to 10 percent percentiles (__quant10__ aggregation type). The result is a vector of 11 values plus the average of all values. This aggregation block is using no templates.
