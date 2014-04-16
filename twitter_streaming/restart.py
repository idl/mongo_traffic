import sys
import os
import time
import subprocess
import psutil



def psutil_check(script_name):
    state_flag = 0
    for proc in psutil.process_iter():
        #print proc.cmdline
        if len(proc.cmdline) > 1 and proc.cmdline[1].find(script_name) > -1:
            print proc.cmdline, proc.pid, proc.ppid
            state_flag = 1
            break

    if state_flag == 0:
        print "Connection not currently running"
        restart_state = start_gnip()
        if restart_state == 'success':
            state_flag = 1
            print "Connection restarted \n"
        elif restart_state == 'failed':
            print "Connection could not be restarted \n"
        
    elif state_flag == 1:
        print "Connection is currently up\n"


def start_gnip():
    try:
        print "Tring to restart Twitter connection\n"
        cmd = "/home/administrator/.virtualenvs/mongo/bin/python2.7 %s" % (file_path)
        print cmd
        restart_state = os.system(cmd)
        return "success"
    except Exception, e:
        print >>sys.stderr, "Execution failed:", e
        return "failed"



file_path = "/home/administrator/tw_pub_consumer/twitter_streaming.py &"

script_name = "twitter_streaming.py"

psutil_check(script_name)
