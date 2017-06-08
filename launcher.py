#! /usr/local/bin/python

import argparse
import os, sys, time

sys.path.append('./libs/');
from remote_exec import SvrRemoteControl, SvrRemoteThread;

# Defaults can be overridden via the command line.
CFG_DEF_TARGET_USER   = "root";
CFG_DEF_TARGET_PWD    = "vmware";

def AddArgs(parser_obj):
   parser_obj.add_argument('-u', '--user',   dest='CfgUserName', action='store', required=False, default=CFG_DEF_TARGET_USER, help='ESXi host (SSH) user name (root).');
   parser_obj.add_argument('-p', '--pwd',    dest='CfgUserPwd',  action='store', required=False, default=CFG_DEF_TARGET_PWD,  help='ESXi (SSH) user password (root).');

def GetArgs():
   # create the top-level parser
   parser = argparse.ArgumentParser(description='Remote fio launcher - input arguments.');
   AddArgs(parser);

   # parse the args and call whatever function was selected
   args = parser.parse_args();
   return args;

args = GetArgs();

def ExecFioThread(rc, params):
   if (rc is None):
      print "ERR: invalid context in ExecFioThread(rc)...";
      return [ 1, "invalid context" ];
   if (params is None):
      print "ERR: no parameter list specified in ExecFioThread(rc, params)...";
      return [ 1, "unspecified parameters" ];
   ret_code, out_str = rc.rexec_v(params['exec_str']);
   return [ ret_code, out_str ];

job_list = [ { "host" : "10.28.240.164", "exec_str" : "cd /home/vmware/Desktop/ ; fio wc-fill-640qd.fio" },
             { "host" : "10.28.240.195", "exec_str" : "cd /home/vmware/Desktop/ ; sleep 60  ; fio wc-fill-640qd.fio" },
             { "host" : "10.28.240.204", "exec_str" : "cd /home/vmware/Desktop/ ; sleep 120 ; fio wc-fill-640qd.fio" },
             { "host" : "10.28.240.157", "exec_str" : "cd /home/vmware/Desktop/ ; sleep 180 ; fio wc-fill-640qd.fio" },
           ];

#dbg_list = [ { "host" : "10.28.240.164", "exec_str" : "cd /home/vmware/Desktop/ ; ls -al" },
#             { "host" : "10.28.240.195", "exec_str" : "cd /home/vmware/Desktop/ ; sleep 10 ; ls -al" },
#             { "host" : "10.28.240.204", "exec_str" : "cd /home/vmware/Desktop/ ; sleep 20 ; ls -al" },
#             { "host" : "10.28.240.157", "exec_str" : "cd /home/vmware/Desktop/ ; sleep 30 ; ls -al" },
#           ];

for ii in range(1, 6, 1):
   print "Repeat Job Loop #%d" % (ii);
   print "#####################################################################\n"
   for job in job_list:
      rc = SvrRemoteThread(job['host'], args.CfgUserName, args.CfgUserPwd, thread_fn=ExecFioThread);
      e_code = 0;
      if (rc is None):
         e_code = 1;
         print "ERR: failed to instantiate SvrRemoteThread class on target %s" % (job['host']);

      if (e_code == 0):
         print "---------- launching scripts on target %s------------" % (job['host']);
         # Save of objects for use during thread "run" invokation
         job['rc'] = rc;
         rc.setParams(job);
         rc.start();

   print "Waiting 60s before monitoring completion of remote threads...";
   time.sleep(60);
   for job in job_list:
      print "Waiting for target %s to complete..." % (job['host']);
      job['rc'].join();
      print "  job completed.";
      print "----------------------------------------------------------\n\n";
   print "#####################################################################\n"

