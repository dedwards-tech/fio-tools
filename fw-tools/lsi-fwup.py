#! /usr/local/bin/python

import argparse
import os, sys

sys.path.append('../libs/');
from remote_exec import SvrRemoteControl

# Defaults can be overridden via the command line.
CFG_DEF_TARGET_USER   = "root";
CFG_DEF_TARGET_PWD    = "ca$hc0w";
CFG_DEF_HOST_LIST     = [ "w2-stsds-240", "w2-stsds-241", "w2-stsds-242", "w2-stsds-243",
                          "w2-stsds-244", "w2-stsds-245", "w2-stsds-246", "w2-stsds-247",
                          "w2-stsds-248", "w2-stsds-249", "w2-stsds-250", "w2-stsds-251",
                          "w2-stsds-252", "w2-stsds-253", "w2-stsds-254" ];

def AddArgs(parser_obj):
   parser_obj.add_argument('-s', '--server', dest='CfgHostAddr',   action='store', nargs='*', required=False,  default=CFG_DEF_HOST_LIST, help='Remote host name or IP address; or list of hosts separated by spaces.');
   parser_obj.add_argument('-u', '--user',   dest='CfgUserName',   action='store', required=False, default=CFG_DEF_TARGET_USER, help='ESXi host (SSH) user name (root).');
   parser_obj.add_argument('-p', '--pwd',    dest='CfgUserPwd',    action='store', required=False, default=CFG_DEF_TARGET_PWD,  help='ESXi (SSH) user password (root).');
   parser_obj.add_argument('-R',             dest='CfgRebootHost', action='store_true', required=False, default=False,  help='Reboot host after FW update(s).');
   parser_obj.add_argument('-C',             dest='CfgInstallCli',   action='store_true', required=False, default=False,  help='Install LSI CLI tools.');
   parser_obj.add_argument('-F',             dest='CfgInstallFW',    action='store_true', required=False, default=False,  help='Install LSI firmware.');
   parser_obj.add_argument('-DD',            dest='CfgInstallDell',  action='store_true', required=False, default=False,  help='Install Dell driver.');
   parser_obj.add_argument('-DI',            dest='CfgInstallInbox', action='store_true', required=False, default=False,  help='Install Inbox driver (mutex with Dell driver).');

   parser_obj.add_argument('-L',             dest='CfgGetHwLogs',     action='store_true',  required=False, default=False,   help='Get the hardware logs from the lsi controller.');
   parser_obj.add_argument('-V',             dest='CfgGetModVersion', action='store_true',  required=False, default=False,  help='Get the loaded driver module version for lsi-mr3 driver.');
   parser_obj.add_argument('--test',         dest='CfgTestRun',       action='store_true',  required=False, default=False,  help='Perform test run, do not execute remotely.');

def GetArgs():
   # create the top-level parser
   parser = argparse.ArgumentParser(description='Remote LSI component update automation tool - input arguments.');
   AddArgs(parser);

   # parse the args and call whatever function was selected
   args = parser.parse_args();
   return args;

#######################################

# ringbuffer debug FW
# fw_image_list = [ "./H730/p9215.rom", "./H730/app_pl_dbg_fault_5862_ring_buf.rom" ];

# SGL Check Inquiry debug FW
# fw_image_list = [ "./H730/p9215.rom", "./H730/app_mr_pl_sgl_check_inquiry.rom" ];
# fw_image_list = [ "./H730/p9215.rom", "./H730/app_pl_mr_sgl_check_inquiry_only_Aug2.rom" ];
# fw_image_list = [ "./H730/p9215.rom", "./H730/app_pl_mr_sgl_check_inquiry_SES_only.rom" ];
# fw_image_list = [ "./H730/p9215.rom", "./H730/app_sgl_inquiry_check_mr_iob_aug3.rom" ];
# fw_image_list = [ "./H730/p9215.rom", "./H730/app_mr_dbg_ieee_sgl_check_aug4.rom" ];
fw_image_list = [ "./H730/p9215.rom", "./H730/app_mr_inquiry_ses_ieee_sgl_changes.rom" ];

cli_tool_list = [ "./H730/vmware-esx-perccli-1.17.10.vib" ];

dell_driver   = [ "./H730/VMW-ESX-6.0.0-lsi_mr3-6.903.85.00_MR-offline_bundle-3818071.zip" ];

inbox_driver  = [ "./inbox/lsi-mr3-6.610.18.05-1OEM.600.0.0.2159203.x86_64.vib" ];

def RemoveRemote(file_list, host, _cmdline_lambda, test_run=False):
   for local_file in file_list:
      print "Removing component %s from host %s..." % (local_file, host);
      err_code, out_str = rc.rexec_v(_cmdline_lambda(remote_file));
      if (err_code != 0):
         print "ERR: component install %s failed on host %s" % (local_file, host);
         print out_str;

def CopyAndExecRemote(file_list, host, _cmdline_lambda, test_run=False):
   for local_file in file_list:
      print "Installing component %s to host %s..." % (local_file, host);
      base_file   = os.path.split(local_file)[1];
      remote_file = "/tmp/%s" % (base_file);
      if (not test_run):
         rc.put_file(local_file, remote_file);
         err_code, out_str = rc.rexec_v(_cmdline_lambda(remote_file));
         if (err_code != 0):
            print "ERR: component install %s failed on host %s" % (local_file, host);
            print out_str;
      else:
         print "  (%s) **>%s" % (host, _cmdline_lambda(remote_file));

def ExecuteAndSaveRemote(host, file_prefix, _cmdline, test_run=False):
   err_code, out_str = rc.rexec_v(_cmdline);
   if (err_code != 0):
      print "ERR: could not requested data from host %s" % (host);
   # open file for writing
   file_name = "%s_%s.txt" % (file_prefix, host);
   hw_log    = open(file_name, 'w');
   # write output from command (even on error)
   hw_log.write(out_str);
   hw_log.close();


args = GetArgs();

for host in args.CfgHostAddr:
   rc = SvrRemoteControl(host, args.CfgUserName, args.CfgUserPwd);
   if (rc is not None):
      e_code, out_str = rc.rexec("ping -c 1 localhost");
      if (e_code != 0):
         print "ERR: failed to connect to host %s" % (host);
         print out_str;
   else:
      e_code = 1;
      print "ERR: failed to instantiate SvrRemoteControl class on host %s" % (host);

   if (e_code == 0):
      print "---------- Performing operations on host %s ------------" % (host);
      if (args.CfgGetModVersion):
         ExecuteAndSaveRemote(host, "module-ver", "esxcli system module get --module=lsi_mr3 | grep Version", args.CfgTestRun);

      # copy driver zip depot to host /tmp and execute driver installation
      if (args.CfgInstallDell):
         CopyAndExecRemote(dell_driver, host, lambda in_str : "esxcli software vib install -d %s" % (in_str), args.CfgTestRun)

      # copy driver .vib (for inbox driver) to hosts /tmp and install unsigned .vib
      if (args.CfgInstallInbox):
         CopyAndExecRemote(inbox_driver, host, lambda in_str : "esxcli software vib install -v %s -f ; esxcli system module set --enabled=true --module=lsi_mr3" % (in_str), args.CfgTestRun)

      # copy CLI vib to host /tmp and execute installation
      if (args.CfgInstallCli):
         CopyAndExecRemote(cli_tool_list, host, lambda in_str : "esxcli software vib install -v %s -f" % (in_str), args.CfgTestRun)

      # copy firmware image to host /tmp and execute FW update
      if (args.CfgInstallFW):
         CopyAndExecRemote(fw_image_list, host, lambda in_str : "cd /opt/lsi/perccli ; ./perccli /c0 download file=%s nosigchk noverchk" % (in_str), args.CfgTestRun)

      if (args.CfgGetHwLogs):
         ExecuteAndSaveRemote(host, "hwlog", "cd /opt/lsi/perccli ; ./perccli /c0 show termlog", args.CfgTestRun);

      # System is connected and powered on, reboot the host and close the connection.
      if (args.CfgRebootHost):
         if (not args.CfgTestRun):
            rc.reboot_async();
         else:
            print "  (%s) **>: REBOOT" % (host);
      rc.close();

#####
print "done."
raise SystemExit(e_code);
