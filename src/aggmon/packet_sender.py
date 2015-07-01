import base64
import socket
import json
import sys
import time


UDP_IP = "127.0.0.1"
UDP_PORT = 10000

def usage():
    print "./packet_sender.py <packet_file> <num_sends>"

if len(sys.argv) < 3:
    usage()
    sys.exit(1)

fname = sys.argv[1]
nsend = int(sys.argv[2])

recvd = []
try:
    f = open(fname)
    sendb = json.load(f)
    f.close()
except Exception as e:
    print e
    sys.exit(1)

for i in xrange(len(sendb)):
    sendb[i] = base64.b64decode(sendb[i])

print "UDP target IP:", UDP_IP
print "UDP target port:", UDP_PORT

sock = socket.socket(socket.AF_INET, # Internet
                     socket.SOCK_DGRAM) # UDP

tstart = time.time()
count = 0
while count < nsend:
    for i in xrange(len(sendb)):
        sock.sendto(sendb[i], (UDP_IP, UDP_PORT))
        count += 1
        if count >= nsend:
            break
        #time.sleep(0.000029)
        #time.sleep(0.000001)
tend = time.time()

print "sent %d messages in %f seconds: %f msg/s" % (count, tend - tstart, float(count)/(tend - tstart))

