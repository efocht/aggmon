"""
Helper functions to send data to InfluxDB using HTTP requests.

Created by Thomas Roehl (Thomas.Roehl@fau.de) for the FEPA project.
"""
import os
import sys
import urllib2
import base64
import re
import logging

__all__ = ["write_influx", "createdb_influx"]

log = logging.getLogger( __name__ )

# Regex to anaylze URLError exceptions
urlerr_regex = re.compile("\[Errno\s*(\d+)\]\s*(.+)$")

def GET(url, headers={}, timeout=10):
    out = ""
    try:
        req = urllib2.Request(url, headers=headers)
        resp = urllib2.urlopen(req, timeout=timeout)
        code = resp.getcode()
        if code >= 200 and code < 300:
            out = resp.read()
            if len(out) == 0:
                return resp.getcode(), "Request to URL %s returns empty data" % (resp.geturl(),), out
        else:
            return resp.getcode(), "Request to URL %s returns error code %d" % (resp.geturl(), resp.getcode(),), out
    except urllib2.HTTPError as e:
        return e.code, "HTTPError for url %s : [Errno %d] %s" % (url,e.code, e.reason,), out
    except urllib2.URLError as e:
        # URLError exception has no code and reason field, one reason.
        # We extract the error code with regex
        m = urlerr_regex.match(str(e.reason))
        code = 404
        reason = "Unknown error"
        if m and len(m.groups()) == 2:
            code, reason = m.groups()
            code = int(code)
        return code, "URLError for url %s : [Errno %d] %s" % (url, code, reason,), out
    except Exception as e:
        # Most global exception
        return 400, "Exception for url %s : %s" % (url,e,), out
    return 200, "OK", out

def POST(url, data={}, headers={}, timeout=10):
    out = ""
    if not isinstance(data, str):
        data = str(data)
    try:
        req = urllib2.Request(url, data, headers=headers)
        resp = urllib2.urlopen(req, timeout=timeout)
        # Commonly write requests return 204 for 'No Content'
        if resp.getcode() == 204:
            out = ""
        elif resp.getcode() >= 200 and resp.getcode() < 300:
            try:
                f = resp.read()
                if len(f) > 0:
                    out = f
                else:
                    return resp.getcode(), "Empty response from %s" % (url,), out
            except ValueError, e:
                return resp.getcode(), "Response from %s is no JSON document" % (url,), out
        else:
            return resp.getcode(), "Request to URL %s returns error code %d" % (resp.geturl(), resp.getcode(),), out
    except urllib2.HTTPError as e:
        return e.code, "HTTPError for url %s : [Errno %d] %s" % (url,e.code, e.reason,), out
    except urllib2.URLError as e:
        # URLError exception has no code and reason field, one reason.
        # We extract the error code with regex
        m = urlerr_regex.match(str(e.reason))
        code = 404
        reason = "Unknown error"
        if m and len(m.groups()) == 2:
            code, reason = m.groups()
            code = int(code)
        return code, "URLError for url %s : [Errno %d] %s" % (url, code, reason,), out
    except Exception as e:
        # Most global exception
        return 400, "Exception for url %s : %s" % (url,e,), out
    return 200, "OK", out

def write_influx(hostname, port, db, measurements, username=None, password=None, apitoken=None):
    """
    Helper function to send measurements to an InfluxDB instance
    """
    if not measurements or not hostname or not db or not isinstance(port, int) or port < 1 or port > 65535:
        return "", "Parameter missing/mismatch."
    if isinstance(measurements, str):
        measurements = [measurements]
    headers = {"Content-Type" : "text/plain", "Accept": "*/*"}
    if username and password:
        base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
        headers.update({"Authorization" : "Basic %s" % base64string})
    elif apitoken:
        headers.update({"Authorization" : "Bearer %s" % (apitoken,)})
    url = "http://%s:%d/write?db=%s" % (hostname, port, db,)
    data = "\n".join(measurements)
    err, estr, data = POST(url, data=data, headers=headers)
    if err >= 200 and err < 300:
        log.info("send %d metrics to influxdb: %d" % (len(measurements), err))
        return "", ""
    log.error("failed to sending %d metrics to influxdb: %d, %s" % (len(measurements), err, estr))
    return "", estr

def createdb_influx(hostname, port, db, username=None, password=None, apitoken=None):
    """
    Helper function to create an InfluxDB database
    """
    if not hostname or not db or not isinstance(port, int) or port < 1 or port > 65535:
        return "", "Parameter missing/mismatch."
    headers = {"Accept": "*/*"}
    if username and password:
        base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
        headers.update({"Authorization" : "Basic %s" % base64string})
    elif apitoken:
        headers.update({"Authorization" : "Bearer %s" % (apitoken,)})
    # GET /query?q=CREATE%20DATABASE%20metric_universe HTTP/1.1
    url = "http://%s:%d/query?q=CREATE%%20DATABASE%%20%s" % (hostname, port, db)
    err, estr, data = GET(url, headers=headers)
    if err >= 200 and err < 300:
        log.info("created database %s: %d" % (db, err))
        return "", ""
    log.error("failed to create database %s: %d, %s" % (db, err, estr))
    return "", estr

if __name__ == "__main__":
    data = ["Local_timer_interrupts,host=tb003,collector=interrupts,cpu=2 value=43709748.0 1473834212000000000",
            "Local_timer_interrupts,host=tb003,collector=interrupts,cpu=12 value=1357139.0 1473834212000000000"]
    hostname = "localhost"
    port = 8086
    db = "test"
    username = "testuser"
    password = "testpass"
    o, e = createdb_influx(hostname, port, db, username=username, password=password)
    if e:
        print("create db failed: %s" % e)
    else:
        print("create db ok")
    o, e = write_influx(hostname, port, db, data, username=username, password=password)
    if e:
        print("send failed: %s" % e)
    else:
        print("send ok")

