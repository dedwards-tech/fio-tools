#! /usr/bin/python

# This is a script to simplify the creation of and use of an SSH connection between
# a proxyvm (Linux) and an ESXi host (SSH shell).  It allows for command line execution
# and parsing of output without depending on the use of SSH key exchange.
#
# The parmiko library provides the helpers to process SSH connections and requests, and
# this script provides a wrapper for command line usage as well as library usage.
#
# There are also ESXi shell command helpers for doing common things like getting and setting
# kernel or advanced host settings.  It is expected that this list grows over time to make
# it easier to share common ESXi shell command protocols.
#

"""
Utilities for makeing ssh connections to an ESXi host.
"""

import argparse
import paramiko
import yaml
import string
import os, sys
from time import sleep
from time import time
from scp import SCPClient
import threading;

# Defaults can be overridden via the command line.
CFG_DEF_TARGET_USER = "root";
CFG_DEF_TARGET_PWD = "pass!Q@W#E";


def AddArgs(parser_obj):
    parser_obj.add_argument('-s', '--server', dest='CfgHostAddr', action='store', nargs='*', required=True,
                            help='Remote host name or IP address; or list of hosts separated by spaces.');
    parser_obj.add_argument('-u', '--user', dest='CfgUserName', action='store', required=False,
                            default=CFG_DEF_TARGET_USER, help='ESXi host (SSH) user name (root).');
    parser_obj.add_argument('-p', '--pwd', dest='CfgUserPwd', action='store', required=False,
                            default=CFG_DEF_TARGET_PWD, help='ESXi (SSH) user password (root).');


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

class SvrRemoteControl:
    def __init__(self, host_name, user_name, password, option=None, auto_connect=True, exit_on_error=True):
        self.HostName = host_name;
        self.UserName = user_name;
        self.Password = password;
        # temp dae: I don't see a local use for this, why is this here?
        self.Option = None;
        # end dae
        self.ConnectOnline = False;

        # Create SSH connection to target_ip...
        self.Client = paramiko.SSHClient();
        self.ScpConn = None;
        self.Client.set_missing_host_key_policy(paramiko.AutoAddPolicy());
        self.Client.load_system_host_keys();

        if (auto_connect):
            self.connect(exit_on_error=exit_on_error);

    def __ssh_exec_cmd__(self, cmd_str):
        if (self.ConnectOnline):
            try:
                stdin, stdout, stderr = self.Client.exec_command(cmd_str);
                stdin.close();

                chk_err = string.join(stderr.readlines(), "");
                if (len(chk_err) > 0):
                    return [chk_err, 1]
            except:
                # Failed to execute request, return error
                str_out = "ERR: failed to execute SSH request:\n%s" % (cmd_str);
                return [str_out, 2];
        else:
            print("ERR: no valid connection to host %s" % (self.HostName));

        # Success
        str_out = string.join(stdout.readlines(), "");
        return [str_out, 0];

    # connect - initial connection request to a specified host.
    #
    #   Inputs:
    #   - exit_on_error  will raise an exception if set to true, return
    #              1 error code by default.
    #
    def connect(self, exit_on_error=False):
        self.ConnectOnline = False;
        try:
            self.Client.connect(self.HostName, username=self.UserName, password=self.Password);
            self.ScpConn = SCPClient(self.Client.get_transport())
            self.ConnectOnline = True;
            return 0;  # success
        except:
            if (exit_on_error):
                print("ERR: cannot connect to %s, exiting!" % (self.HostName));
                raise SystemExit(1);
        return 1;  # failure

    # connect_retry - connect with retry if a timeout occurs.
    #   Inputs:
    #     - retry_count (disabled default) - number of times to retry connection
    #                                        before failing.
    #     - retry_delay (30s default)      - delay in seconds between retries.
    #     - signal "exit" with code 1 (enabled by default).
    #
    def connect_retry(self, retry_count=0, retry_delay=30, exit_on_error=False):
        num_retries = 0;
        for ii in range(0, retry_count + 1):
            if (self.connect(exit_on_error) == 0):
                return 0;  # success
            else:
                num_retries += 1;
            if (ii <= retry_count):
                # sleep between retries.
                sleep(retry_delay);
        self.ConnectOnline = False;
        if (exit_on_error):
            print("ERR: Timeout, max retries %s, attempting to reconnect to host %s" % (num_retries, self.HostName));
            raise SystemExit(1);  # failure, raise exit exception
        return 1;  # timeout waiting

    # rexec - remotely execute the command.
    #
    def rexec(self, cmd_str):
        if (True):
            out_str, err_code = self.__ssh_exec_cmd__(cmd_str);
        else:
            print("Connection to host %s not established." % (self.HostName));
            err_code = 0;
            out_str = "";
        return [err_code, out_str];

    # rexec_v - (verbose) remotely execute the command by displaying response text.
    #
    def rexec_v(self, cmd_str):
        if (True):
            out_str, err_code = self.__ssh_exec_cmd__(cmd_str);
        else:
            err_code = 0;
            out_str = "";
        if (out_str != ""):
            print(cmd_str + "\nReturned:\n" + out_str);
        else:
            print(cmd_str + "\n");
        return [err_code, out_str];

    def put_file(self, local_file, remote_file):
        e_code = 1;
        print("Copy %s to remote host at %s" % (local_file, remote_file));
        if (self.ScpConn is not None):
            # scp the requested 'local_file' to the remote host file...
            self.ScpConn.put(local_file, remote_file);
            sys.stdout.flush();
            e_code = 0;
        else:
            print("ERR: uninitialized SCP connection with host %s" % (self.HostName));
        return e_code;

    def get_file(self, remote_file, local_file):
        print("Download %s from remote host at %s" % (remote_file, local_file));
        if (self.ScpConn is not None):
            # scp the requested 'local_file' to the remote host file...
            self.ScpConn.get(remote_file, local_file);
            sys.stdout.flush();

    # waitfor_shutdown - does NOT send shutdown command, rather does some magic to wait
    #                    for a host to stop responding to an execution request on a valid
    #                    and active connection.
    #
    def waitfor_shutdown(self, quiet=False):
        now = start = time();
        while True:
            e_code, out_str = self.rexec("ping -c 1 localhost");
            if (e_code != 0):
                # exit while loop
                break;
            # Delay for 10 seconds, then try again.
            sleep(10);
            now = time() - start;
            if (not quiet):
                print("\r *waiting for shutdown, %d(s)..." % (now));
                sys.stdout.flush();
        now = time() - start;
        print("\r host shutdown took %d seconds." % (now));
        sys.stdout.flush();
        return 0;

    # waitfor_bootup - assumes the system is actually booting, i.e. not supporting
    #                  connection attempts.  will wait for a host connection to
    #                  be established to assume it has actually completed boot.
    #
    def waitfor_bootup(self, quiet=False):
        print("Waiting for host bootup...");
        start = time();
        # There doesn't appear to be a timeout for waiting for a connection...
        e_code = self.connect_retry(retry_count=10);
        now = time() - start;
        if ((e_code == 0) and (not quiet)):
            print("\r host boot took %d minutes." % (now / 60));
            sys.stdout.flush();
        else:
            print("\r ERR: connection to host during boot failed, timeout perhaps?");
        return e_code;

    # reboot - for a connected host, issue reboot command, wait for the host to
    #          stop responding to new commands (so we know reboot has actually
    #          initiated), then wait for the host to boot back up.
    #
    def reboot(self):
        print("Sending request for reboot...");
        e_code, out_str = self.rexec("reboot");
        sys.stdout.flush();
        if (e_code == 0):
            self.waitfor_shutdown();
            # Give the system a little longer to ensure the shell doesn't re-connect
            # too quickly following the actual shutdown.
            print(" delaying 30s to ensure shutdown is well underway...");
            sys.stdout.flush();
            sleep(30);
            self.waitfor_bootup();
        else:
            print("ERR: could not reboot host, something is wrong!");
        sys.stdout.flush();
        return e_code;

    # reboot_async - for a connected host, issue reboot command, wait for the host to
    #                stop responding to new commands (so we know reboot has actually
    #                initiated), then exit.
    #
    def reboot_async(self):
        print("Sending request for reboot (async)...");
        e_code, out_str = self.rexec("reboot");
        sys.stdout.flush();
        if (e_code == 0):
            self.waitfor_shutdown();
            # Give the system a little longer to ensure the shell doesn't re-connect
            # too quickly following the actual shutdown.
            print(" delaying 30s to ensure shutdown is well underway...");
            sys.stdout.flush();
        else:
            print("ERR: could not reboot host, something is wrong!");
        return e_code;

    def is_connected(self):
        return (self.ConnectOnline == True);

    def close(self):
        if (self.ConnectOnline):
            self.Client.close();
        self.ScpConn = None;
        self.ConnectOnline = False;


#############################################

# SvrRemoteThread - allows executing of a remote command in a thread.  This class will
#                   always create a new connection per thread!
#
class SvrRemoteThread(threading.Thread):
    def __init__(self, host_name, user_name, password, thread_fn):
        threading.Thread.__init__(self);
        self.RetCode = -1;
        self.OutStr = '<not started>';
        if (thread_fn is None):
            print("ERR: no thread function specified in SvrRemoteThread init.");
            return;
        self.ThreadFn = thread_fn;
        self.Parameters = {};
        self.RC = SvrRemoteControl(host_name, user_name, password, auto_connect=True, exit_on_error=False);
        if (not self.RC.is_connected()):
            print("ERR: could not establish a connection with host %s" % (host_name));

    def setParams(self, params_list):
        self.Parameters = params_list;

    def run(self):
        if ((self.ThreadFn is None) or (not self.RC.is_connected())):
            return;
        self.RetCode = 0;
        self.OutStr = '<started>';
        # Execute thread functionality
        self.RetCode, self.OutStr = self.ThreadFn(self.RC, self.Parameters);

    def disconnect(self):
        # close the connection and exit.
        self.RC.close();


# SvrRemoteThreadBase - setup the context for passing to a thread when it executes.
#
class SvrRemoteThreadBase(threading.Thread):
    def __init__(self, host_name, user_name, user_pwd):
        threading.Thread.__init__(self);
        self.RetCode = -1;
        self.OutStr = '<not started>';
        self.RC = SvrRemoteControl(host_name, user_name, user_pwd, auto_connect=True, exit_on_error=False);
        if (not self.RC.is_connected()):
            print("ERR: could not establish a connection with host %s" % (host_name));
        # create the context to pass to the thread_fn upon start.
        self.Context = {};
        self.Context['rc'] = self.RC;
        self.Context['host'] = host_name;
        self.Context['user_name'] = user_name;
        self.Context['user_pwd'] = user_pwd;
        self.Context['params'] = {};

    @classmethod
    def ThreadFn(self, context):
        return [0, "(not implemented)"];

    def setParams(params_list):
        self.Context['params'] = params_list;

    def run(self):
        if ((self.ThreadFn is None) or (not self.RC.is_connected())):
            return;
        self.RetCode = 0;
        self.OutStr = '<started>';
        # Execute thread functionality
        self.RetCode, self.OutStr = self.ThreadFn(self.Context);

    def disconnect(self):
        # close the connection and exit.
        self.RC.close();


#############################################

class ExecAdvancedSetting:
    def __init__(self, option_path):
        self.OptionPath = option_path;

    def SetInt(self, option_value):
        return "esxcli system settings advanced set -o %s --int-value %s" % (self.OptionPath, option_value);

    def SetStr(self, option_value):
        return "esxcli system settings advanced set -o %s --string-value %s" % (self.OptionPath, option_value);

    def Get(self):
        return "esxcli system settings advanced list -o %s" % (self.OptionPath);


class ExecKernelSetting:
    def __init__(self, option_path):
        self.OptionPath = option_path;

    def SetValue(self, option_value):
        return "esxcli system settings kernel set -o %s -v %s" % (self.OptionPath, option_value);

    def Get(self):
        return "esxcli system settings kernel list -o %s" % (self.OptionPath);


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

    # Test non-threaded class
    rc = SvrRemoteControl(args.CfgHostAddr[0], args.CfgUserName, args.CfgUserPwd);
    if (rc is not None):
        e_code, out_str = rc.rexec("ping -c 1 localhost");
        if (e_code == 0):
            print("SvrRemoteControl: unit test successful");
        else:
            print("ERR: failed to execute simple remote command in SvrRemoteControl unit test.");
            print(out_str);
    else:
        e_code = 1;
        print("ERR: failed to instantiage SvrRemoteControl class on server %s" % (args.CfgHostAddr[0]));

    if (e_code == 0):
        cmd_str = ExecKernelSetting("enablePCIEHotplug").Get();
        e_code, out_str = rc.rexec(cmd_str);
        print(" ~ # %s\n%s" % (cmd_str, out_str));

        cmd_str = ExecAdvancedSetting("/Disk/QFullSampleSize").Get();
        e_code, out_str = rc.rexec(cmd_str);
        print(" ~ # %s\n%s" % (cmd_str, out_str));

        cmd_str = ExecAdvancedSetting("/Disk/QFullThreshold").Get();
        e_code, out_str = rc.rexec(cmd_str);
        print(" ~ # %s\n%s" % (cmd_str, out_str));

        cmd_str = ExecAdvancedSetting("/Disk/SchedNumReqOutstanding").Get();
        e_code, out_str = rc.rexec(cmd_str);
        print(" ~ # %s\n%s" % (cmd_str, out_str));

        # Test file put operations
        #   - first test changing name of local file to a folder + file name.
        #   - second test copying a local file to a folder.
        rc.put_file(__file__, '/scratch/delme.txt');
        rc.rexec_v("cat /scratch/delme.txt");

        rc.put_file(__file__, '/scratch/');
        rc.rexec_v("cat /scratch/%s" % (__file__));

        # Test file get operation
        rc.get_file("/scratch/delme.txt", "/tmp/");

        # remove misc target files for next run...
        rc.rexec("rm /scratch/delme.txt");
        rc.rexec("rm /scratch/%s" % (__file__));

        # System is connected and powered on, test reboot.
        # rc.reboot();
    rc.close();
    raise SystemExit(e_code);
