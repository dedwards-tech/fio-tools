#! /usr/bin/python

# I incorporated moussa's threading script into remote_exec.py, and created this "similar" script
# multihost.py.  It requires PyYaml to be installed on the proxy VM though!
#  ~ sudo yum install PyYaml
#
# The command line looks like the following:
#   ./multihost.py -y <yaml_input_file> -o ./out -u root -p 'pass!Q@W#E'
#
# Here iss an example YAML config file for the script for using Alex tools.  Kind of a pain not
# knowing the output file name but it works.  It will create a remote folder /scratch/dave,
# Run alex tools with output to /scratch/dave, then tar up specific file output to a known
# file name, then delete the /scratch/dave folder.
#
# NOTE: this will leave the .tgz file on the host, and hopefully replace it each time it is run.
#
# hosts:
#     dedwood-03.micron.com:
#        command: "mkdir /scratch/dave ; /scratch/blktbl-capture-vmware/dumpreg /scratch/dave ; tar -zcf regdump.tgz /scratch/dave/*_REG.txt ; rm -rf /scratch/dave"
#        file: "regdump.tgz"
#     dedwood-04.micron.com:
#        command: "mkdir /scratch/dave ; /scratch/blktbl-capture-vmware/dumpreg /scratch/dave ; tar -zcf regdump.tgz /scratch/dave/*_REG.txt ; rm -rf /scratch/dave"
#        file: "regdump.tgz"
#
# You can also override the command line -u and -p for user name or password, or specify a
# unique one per host in the yaml as below.
#
# hosts:
#     l-cheddar1:
#        username: jenkins
#        password: "123456"
#        command: "sudo fio fio/sysbench_4k_w.fio > 4k_w.out"
#        file: "4k_w.out"
#     l-cheddar2:
#        username: jenkins
#        password: "123456"
#        command: "sudo fio fio/sysbench_4k_r.fio > 4k_r.out"
#        file: "4k_r.out"
#     l-cheddar3:
#        username: jenkins
#        password: "123456"
#        command: "sudo fio fio/sysbench_4k_w.fio > 4k_w.out"
#        file: "4k_w.out"
#     l-cheddar4:
#        username: jenkins
#        password: "123456"
#        file: "4k_r.out"
#        command: "sudo fio fio/sysbench_4k_r.fio > 4k_r.out"
#

"""
Utility for execution commands on mutliple hosts simultaneously via SSH and YAML input.
"""

import sys
sys.path.append('../libs/');
from remote_exec import SvrRemoteControl, SvrRemoteThread

import argparse
import yaml
import string
import os

# Defaults can be overridden via the command line.
CFG_DEF_TARGET_USER   = "root";
CFG_DEF_TARGET_PWD    = "pass!Q@W#E";

def AddArgs(parser_obj):
   parser_obj.add_argument('-y', '--yaml', dest='CfgYamlIn',   action='store', required=True, type=argparse.FileType('r'),  help='Input file containing YAML host + command config.');
   parser_obj.add_argument('-o', '--out',  dest='CfgOut',      action='store', required=True,                               help='Folder to place output files.');
   parser_obj.add_argument('-u', '--user', dest='CfgUserName', action='store', required=False, default=CFG_DEF_TARGET_USER, help='ESXi host (SSH) user name (root).');
   parser_obj.add_argument('-p', '--pwd',  dest='CfgUserPwd',  action='store', required=False, default=CFG_DEF_TARGET_PWD,  help='ESXi (SSH) user password (root).');

def GetArgs():
   """;
   Supports the command-line arguments listed below.;
   """;

   # create the top-level parser
   parser = argparse.ArgumentParser(description='Remote execution library - input arguments.');
   AddArgs(parser);

   # parse the args and call whatever function was selected
   args = parser.parse_args();
   return args;

#############################################

# SvrMultiHost - works of a yaml spec object with input format below.  This
#     class provides a mechanism to launch SSH commands on multiple hosts simultaneously.
#     Main input is command, and response is expected in the form of a file to copy back
#     to the current host.
#
#     user_name and user_pass fields of the yaml input is optional, default will be to
#     use specified user_name and user_pass given during __init__().
#
# hosts:
#   host_name1:
#      username: "jenkins" (optional - default = 'root')
#      password: "123456" (optional - default = 'pass!Q@W#E')
#      command: "sudo fio fio/sysbench_4k_w.fio > fio_w_4k.out"
#      file: "fio_w_4k.out"
#   host_name2:
#      command: "sudo fio fio/sysbench_4k_w.fio > fio_w_4k.out"
#      file: "fio_w_4k.out"
#
# NOTE: upon thread completion, the Wait method will copy the expected output file
#       from the remote target to the local host.  Upon copy back to the local host
#       the file name will be prepended with the host name.
#
class SvrMultiHost:
   def __init__(self, yaml_obj, user_name='root', user_pass='pass!Q@W#E'):
      self.ThreadList  = list();
      self.YamlObj     = yaml_obj;
      self.DefaultUser = user_name;
      self.DefaultPwd  = user_pass;

   def __threxec_cb(self, rc):
      host_name = rc.HostName;
      cmd_str   = self.YamlObj['hosts'][host_name]['command']
      print "Executing '%s' on host %s" % (cmd_str, host_name);
      e_code, out_str = rc.rexec(cmd_str);
      return [ e_code, out_str ];

   def Start(self):
      print "Starting multi-host threads...";
      for host in self.YamlObj['hosts']:
         user_name  = self.YamlObj['hosts'][host].get('username', self.DefaultUser);
         user_pass  = self.YamlObj['hosts'][host].get('password', self.DefaultPwd);
         th = SvrRemoteThread(host, user_name, user_pass, self.__threxec_cb)
         if (th.RC.is_connected()):
            self.ThreadList.append(th);
            th.start();

   def Wait(self):
      print "\nWaiting for threads to exit...";
      for th in self.ThreadList:
         th.join();
      print "Thread execution complete.";

   # Copy files from the specified remote out_folder, and copy to the "cwd",
   # once copy is complete, disconnect the remote host; command execution is complete.
   def GetFiles(self, out_folder='.'):
      print "Retrieving files from remote hosts...";
      for th in self.ThreadList:
         host_name = th.RC.HostName;
         rem_file = self.YamlObj['hosts'][host_name]['file'];
         loc_file = "%s/%s_%s" % (out_folder, host_name, rem_file);
         print "  Retrieving %s from %s" % (rem_file, host_name)
         th.RC.get_file(rem_file, loc_file)
         # close the remote connection, no more commands allowed
         th.disconnect();
      print "File retrieval complete.";

   def Go(self, out_folder='.', in_folder='.'):
      self.Start();
      self.Wait();
      self.GetFiles(out_folder);

# SvrMultiHostCustom - Base clase for providing callback based factory to extend for your
#                    own purposes.
#
class SvrMultiHostCustom:
   def __init__(self):
      self.ThreadList  = list();

   def AddThread(self, host_thread):
      self.ThreadList.append(host_thread);

   def Start(self):
      print "Starting multi-host threads...";
      for th in self.ThreadList:
         th.start();

   def Wait(self):
      for th in self.ThreadList:
         th.join();
      print "Threads exited, disconnecting..."
      for th in self.ThreadList:
         th.disconnect();
      print "Thread execution complete.";

   def Go(self):
      self.Start();
      self.Wait();

#############################################

# Determine how we were instantiated (command line, or included)
CFG_FROM_CMD_LINE = False;
if (sys.argv[0] == __file__):
   CFG_FROM_CMD_LINE = True;

if (CFG_FROM_CMD_LINE):
   # We were launched from the command line so execute a test workload, only on the first
   # host in the list; this could easily be adapted to work on each host in the list but is
   # not necessary for the "unit test" purpose of this basic functionality.
   args = GetArgs();

   yaml_obj = None;
   try:
      yaml_obj = yaml.load(args.CfgYamlIn);
   except yaml.YAMLError, exc:
      print "Error in configuration file: %s" % (exc);

   if (yaml_obj is None):
      print "ERR: failed to load YAML config file from %s" % (args.CfgYamlIn.name);
      raise SystemExit(1);

   rem_exec = SvrMultiHost(yaml_obj, user_name=args.CfgUserName, user_pass=args.CfgUserPwd);
   rem_exec.Go(args.CfgOut);
   raise SystemExit(0);
