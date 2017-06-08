#! /usr/local/bin/python

import argparse
import yaml
import subprocess
import os, sys
import math
import json
import pprint
sys.path.append('../libs/');
from multihost import SvrMultiHostCustom

sys.path.append('../provision/');
from remote_exec import SvrRemoteThreadBase


CFG_DEFAULT_WORKLOAD_YAML = """fio-gen:
   sequence:
      fill:         "precondition"
      max_baseline: "precondition,max-rrd-iops,max-rrd-bw,max-rwr-iops,max-rwr-bw"
      block_sweep:  "precondition,blkswp-rrd,blkswp-rwr"
      queue_sweep:  "precondition,qdswp-rrd,qdswp-rwr"
      all_rd:       "max-rrd-iops,max-rrd-bw,blkswp-rrd,qdswp-rrd"
      all_wr:       "max-rwr-iops,max-rwr-bw,blkswp-rwr,qdswp-rwr"
      all:          "precondition,max-rrd-iops,max-rrd-bw,blkswp-rrd,qdswp-rrd,max-rwr-iops,max-rwr-bw,blkswp-rwr,qdswp-rwr"
   target_groups:
      d4_b2e:   "/dev/sdb,/dev/sdc,/dev/sdd,/dev/sde"
      d4_f2i:   "/dev/sdf,/dev/sdg,/dev/sdh,/dev/sdi"
      d8_b2i:   "/dev/sdb,/dev/sdc,/dev/sdd,/dev/sde,/dev/sdf,/dev/sdg,/dev/sdh,/dev/sdi"
   global:
      group_reporting: "true"
      reduce_tod:      "false"
   single:
      precondition:
         block_size: "256k"
         io_depth: "1"
         io_type: "Sequential"
         num_jobs: "1"
         read_pct: "0"
         run_time: "-1"
      max-rrd-iops:
         block_size: "4k"
         io_depth:   "32"
         num_jobs:   "8"
         read_pct:   "100"
         io_type:    "Random"
         run_time: "60"
      max-rwr-iops:
         block_size: "4k"
         io_depth:   "32"
         num_jobs:   "8"
         read_pct:   "0"
         io_type:    "Random"
         run_time: "60"
      max-rrd-bw:
         block_size: "256k"
         io_depth:   "32"
         num_jobs:   "4"
         read_pct:   "100"
         io_type:    "Random"
         run_time: "60"
      max-rwr-bw:
         block_size: "256k"
         io_depth:   "32"
         num_jobs:   "4"
         read_pct:   "0"
         io_type:    "Random"
         run_time: "60"
   block_sweep:
      blkswp-rrd:
         block_size: "512,1k,2k,4k,8k,16k,32k,64k,128k,256k,512k"
         io_depth: "32"
         num_jobs: "1,8"
         io_type:  "Random"
         read_pct: "100"
         run_time: "60"
      blkswp-rwr:
         block_size: "512,1k,2k,4k,8k,16k,32k,64k,128k,256k,512k"
         io_depth: "32"
         num_jobs: "1,8"
         io_type:  "Random"
         read_pct: "0"
         run_time: "60"
   qd_sweep:
      qdswp-rrd:
         block_size: "4k,32k,64k"
         io_depth:   "1,2,4,8,16,32"
         num_jobs: "1,8"
         io_type:  "Random"
         read_pct: "100"
         run_time: "60"
      qdswp-rwr:
         block_size: "4k,32k,64k"
         io_depth:   "1,2,4,8,16,32"
         num_jobs: "1,8"
         io_type:  "Random"
         read_pct: "0"
         run_time: "60"
""";

class FioWorkloadSpec:
    'Container for storing and generating fio-r.py workload and job definition.'
    def __init__(self, en_short, name, blk_sz, run_time, read_pct, io_type, io_depth, num_jobs, target_list=[]):
        self.Name = name;
        # Populate default values.
        self.BlockSize   = blk_sz;
        self.ReadPct     = int(read_pct);
        self.IoDepth     = int(io_depth);
        self.NumJobs     = int(num_jobs);
        self.TargetList  = target_list;
        self.NumTargets  = len(target_list);
        self.EnLatency   = False;
        self.EnIops      = False;
        self.EnBandwidth = False;
        self.GroupReport = True;
        self.ReduceTOD   = False;
        self.set_short_run(en_short);
        self.set_io_type(io_type);
        self.set_run_time(int(run_time));

    def set_globals(group_reporting=True, reduce_tod=False):
        self.GroupReport = group_reporting;
        self.ReduceTOD   = reduce_tod;

    def set_modifiers(self, en_lat = False, en_iops = False, en_bw = False):
        self.EnLatency   = en_lat;
        self.EnIops      = en_iops;
        self.EnBandwidth = en_bw;
        self.ReduceTOD   = False;

    def set_short_run(self, en_short):
        if (en_short):
            print "WARNING: enabling SHORT run on workload %s" % (self.Name);
            self.RunTime = 2;
        self.EnShort = en_short;

    def set_io_type(self, io_type):
        io_type = io_type.lower();
        if ((io_type == "random") or (io_type == "rand")):
            io_type = "randrw";
        elif ((io_type == "sequential") or (io_type == "seq")):
            io_type = "readwrite";
        else:
            io_type = "*unknown*";
        self.IoType = io_type;

    def set_run_time(self, run_time):
        # determine if we are to fill the drive, or not.
        if (run_time == -1):
            self.SizeBased  = True;
        else:
            self.SizeBased  = False;
        # override runtime if this is a short run.
        if (self.EnShort):
            self.RunTime = 2;
        else:
            self.RunTime = int(run_time);

    def get_name(self):
        return self.Name;

    def to_fio(self):
        script_txt  = ("[global]\n"
                       "thread\n"
                       "direct=1\n"
                       "norandommap=1\n"
                       "refill_buffers\n"
                       "ioengine=libaio\n");
        if (not self.SizeBased):
            script_txt += "time_based\n";

        if (self.GroupReport):
            script_txt  += "group_reporting\n"

        if (self.ReduceTOD):
            script_txt += "gtod_reduce=1\n";

        if (self.EnLatency or self.EnIops or self.EnBandwidth):
            if (self.EnLatency):
                script_txt += "write_lat_log=%s\n" % (self.Name);
            if (self.EnIops):
                script_txt += "write_iops_log=%s\n" % (self.Name);
            if (self.EnBandwidth):
                script_txt += "write_bw_log=%s\n" % (self.Name);

        script_txt += "runtime=%s\nbs=%s\nnumjobs=%s\niodepth=%s\n" % (self.RunTime, self.BlockSize, self.NumJobs, self.IoDepth);
        script_txt += "rw=%s\nrwmixread=%s\n\n" % (self.IoType, self.ReadPct);

        for target in self.TargetList:
            script_txt += "[%s]\nfilename=%s\n\n" % (target.replace('/', '_'), target);
        return script_txt;

    def __str__(self):
        script_txt   = "# AUTOGENERATED BY %s\n" % (__file__);
        script_txt   = "# WORKLOAD NAME: %s\n" % (self.Name);
        script_txt  += "BLOCK_SIZE: %s\n" % (self.BlockSize);
        script_txt  += "RUN_TIME: %d\n" % (self.RunTime);
        script_txt  += "READ_PCT: %d\n" % (self.ReadPct);
        script_txt  += "IO_TYPE: %s\n" % (self.IoType);
        script_txt  += "IO_DEPTH: %d\n" % (self.IoDepth);
        script_txt  += "NUM_JOBS: %d\n" % (self.NumJobs);
        script_txt  += "NUM_TARGETS: %d\n" % (self.NumTargets);
        if (self.TargetList is not None):
            if (len(self.TargetList) >= self.NumTargets):
                script_txt += "TARGET_LIST: ";
                last_index  = self.NumTargets - 1;
                curr_index  = 0;
                for target in self.TargetList:
                    script_txt += "%s" % (target);
                    if (curr_index != last_index):
                        script_txt += ",";
                    else:
                        # There may be fewer targets in the list
                        # than are requested for testing!
                        break;
                    curr_index += 1;
                script_txt += "\n";
            else:
                print ("ERR: number of targets in target list exceeds targets specified.");
                raise SystemExit(1);
        flag_str     = "False";
        if (self.SizeBased):
            flag_str = "True";
        script_txt  += "SIZE_BASED: %s\n" % (flag_str);
        return script_txt;

#############################################

scr_heading = """#! /usr/bin/python
import subprocess
import json
import argparse
### autogenerated script using fio-gen.py
out_folder = "json";
# Input arguments
def AddArgs(parser_obj):
    parser_obj.add_argument('-s', dest='CfgSequence', action='store', required=False, default="max_baseline", help='Specify sequence to execute; max_baseline by default.');
def GetArgs():
    # create the top-level parser
    parser = argparse.ArgumentParser(description='fio complex workload executor input arguments.');
    AddArgs(parser);
    # parse the args and call whatever function was selected
    args = parser.parse_args();
    return args;
# List of scripts to execute, in order
""";

scr_footing = """
# Execute each script with output matching the input name with .json
subprocess.call("rm -rf %s ; sync ; mkdir %s" % (out_folder, out_folder), shell=True);
# During execution we will parse each json output file for iops, bw and latency averages
csv_str  = "Workload,Read_BW,Read_IOPS,Write_BW,Write_IOPS\\n";
test_exec = name_list.sort();
for fio_item in test_exec:
   json_file = "%s/%s.json" % (out_folder, fio_item);
   subprocess.call("fio %s.fio --output-format=json --output %s ; sync" % (fio_item, json_file), shell=True);
   in_file   = open(json_file, 'r');
   j_data    = json.load(in_file);
   in_file.close();

   # Grab data and save it to output file
   rd_bw    = j_data["jobs"][0]["read"]["bw"];
   rd_iops  = j_data["jobs"][0]["read"]["iops"];
   wr_bw    = j_data["jobs"][0]["write"]["bw"];
   wr_iops  = j_data["jobs"][0]["write"]["iops"];
   csv_str += "%s,%s,%s,%s,%s\\n" % (fio_item, rd_bw, rd_iops, wr_bw, wr_iops);

# Write JSON data to CSV file
out_file_name = "%s/summary.csv" % (out_folder);
print "Saving output to CSV: %s" % (out_file_name);
print csv_str;
csv_file = open(out_file_name, "w");
csv_file.write(csv_str);
csv_file.close();
print "IO characterization run complete.";
""";

def GenerateFioScripts(args, workload_list):
   exec_script = scr_heading;
   name_list   = "\nname_list = { ";
   print "Generating fio scripts...";
   for workload in workload_list:
      # Create workload input file
      file_name = "%s/%s.fio" % (args.CfgOutFolder, workload.Name);
      name_list += "'%s',\n" % (workload.Name);
      out_file = open(file_name, 'w');
      if (out_file is None):
          print "ERR: failure opening workload file %s for writing." % (file_name);
      else:
         out_file.write("%s\n" % (workload.to_fio()));
         out_file.flush();
      print "  Workload written to %s" % (file_name);
      out_file.close();
   # complete the python fio-exec.py script and save to output folder.
   name_list   += " };";
   exec_script += "\n %s \n%s" % (name_list, scr_footing);
   scr_file = open("%s/fio-exec.py" % (args.CfgOutFolder), 'w');
   scr_file.write(exec_script);
   scr_file.flush();
   scr_file.close();
   raise SystemExit(0);

#############################################

def AddArgs(parser_obj):
    parser_obj.add_argument('-o', dest='CfgOutFolder',  action='store', required=False, default="./out", help='Specify a folder to place output files into.');
    parser_obj.add_argument('-w', dest='CfgWorkloads',  action='store', required=False, type=argparse.FileType('r'), default=None, help='Override the default workload YAML based config file.');
    parser_obj.add_argument('-t', dest='CfgTargetSeq',  action='store', required=True,  help='Specify a workload target spec to generate the fio scripts with; we can only process one target spec at a time.');

def GetArgs():
    """;
    Supports the command-line arguments listed below.;
    """;

    # create the top-level parser
    parser = argparse.ArgumentParser(description='fio multi-host multi-workload launcher input arguments.');
    AddArgs(parser);

    # parse the args and call whatever function was selected
    args = parser.parse_args();
    return args;

#############################################

def GetMultipleItems(item_string):
   if (item_string is not None):
      items = item_string.split(',');
      if (len(items) == 0):
         items = [ item_string.strip(' \n\r') ];
   else:
      items = None;
   return items;

def GetGlobals(global_obj, args):
   args.Global = { 'group_reporting': True, 'reduce_tod': False };
   if (global_obj is not None):
      for key, value in global_obj.iter_items():
         if (value.lower() == "false"):
            args.Global[key] = False;

def GetSequenceList(seq_obj, args):
   args.SequenceList = {};
   if (seq_obj is not None):
      for key, value in seq_obj.iter_items():
         if (value is not None):
            if (value != ""):
               seq_items = value.strip(" \n\r").split(',');
               args.SequenceList[key] = seq_items;

def ValidateSequenceList():
   left off here - create validators to check for spelling errors, etc.
   Next step: test parsing of new yaml file.

def GetTargetGroups(target_obj, args):
   args.TargetGroups = {};
   for key, value in target_obj:
      item_list = value.split(',');
      if (len(item_list) > 0):
         args.TargetGroups[key] = item_list;

def GetSweepParameters(sweep_obj, args):
   bs_list  = GetMultipleItems(sweep_obj['block_size']);
   qd_list  = GetMultipleItems(sweep_obj['io_depth']);
   job_list = GetMultipleItems(sweep_obj['num_jobs']);
   io_type  = sweep_obj['io_type'];
   read_pct = sweep_obj['read_pct'];
   if (sweep_obj.has_key('run_time')):
      run_time = sweep_obj['run_time'];
   else:
      run_time = "30";
   return [ bs_list, qd_list, job_list, io_type, read_pct, run_time ];

def GetSingleParameters(single_obj, args):
   block_size  = single_obj['block_size'];
   io_type     = single_obj['io_type'];
   io_depth    = single_obj['io_depth'];
   num_jobs    = single_obj['num_jobs'];
   read_pct    = single_obj['read_pct'];
   if (single_obj['run_time'] is not None):
      run_time = single_obj['run_time'];
   else:
      run_time = "30";
   return [ block_size, io_depth, num_jobs, io_type, read_pct, run_time ]

def ProcessBlockSweepObj(bsweep_obj, item_name, workload_list, args):
   [ bs_list, qd_list, job_list, io_type, read_pct, run_time, target_list ] = GetSweepParameters(bsweep_obj, args);
   # Create workload objects from information
   sequence_id = 0;
   for num_jobs in job_list:
      for io_depth in qd_list:
         for block_size in bs_list:
            wkload_name = "%s-%02d_%sj-%sqd-%s" % (item_name, sequence_id, num_jobs, io_depth, block_size);
            workload = FioWorkloadSpec(False, wkload_name, block_size, run_time, read_pct, io_type, io_depth, num_jobs, target_list);
            workload_list.append(workload);
            sequence_id += 1;

def ProcessQdSweepObj(qdsweep_obj, item_name, workload_list, args):
   [ bs_list, qd_list, job_list, io_type, read_pct, run_time, target_list ] = GetSweepParameters(qdsweep_obj, args);
   # Create workload objects from information
   sequence_id = 0;
   for num_jobs in job_list:
      for block_size in bs_list:
         for io_depth in qd_list:
            wkload_name = "%s-%02d_%sj-%sqd-%s" % (item_name, sequence_id, num_jobs, io_depth, block_size);
            workload = FioWorkloadSpec(False, wkload_name, block_size, run_time, read_pct, io_type, io_depth, num_jobs, target_list);
            workload_list.append(workload);
            sequence_id += 1;

# Determine how we were instantiated (command line, or included)
CFG_FROM_CMD_LINE = False;
if (sys.argv[0] == __file__):
   CFG_FROM_CMD_LINE = True;

if (CFG_FROM_CMD_LINE):
   # We were launched from the command line so execute a test workload, only on the first
   # host in the list; this could easily be adapted to work on each host in the list but is
   # not necessary for the "unit test" purpose of this basic functionality.
   args = GetArgs();
   if (not os.path.exists(args.CfgOutFolder)):
       print "Creating output folder: %s" % (args.CfgOutFolder);
       os.mkdir(args.CfgOutFolder);
       if (not os.path.exists(args.CfgOutFolder)):
           print "ERR: cannot create output folder %s" % (args.CfgOutFolder);
           raise SystemExit;

   # Determine what our target spec is so we can create the proper workload spec.
   # TODO: Get target groups
   # TODO: Turn target specification (from command line) to target groups

   # TODO: need to separate scripts by output folder; simple to simple, sweeps to sweeps
   # TODO: create input config file for fio-exec.py and make fio-exec.py an actual editable script!

   # create list of workload objects, from workload YAML config file.
   workload_list = list();
   try:
      # Load workload definition from file or local (default) string
      if (args.CfgWorkloads is not None):
         yaml_obj = yaml.load(args.CfgWorkloads);
      else:
         yaml_obj = yaml.load(CFG_DEFAULT_WORKLOAD_YAML);

      # Process Single (non-sweeping) Items
      for single_item in yaml_obj['fio-gen']['single']:
         single_obj  = yaml_obj['fio-gen']['single'][single_item];
         [ block_size, io_depth, num_jobs, io_type, read_pct, run_time ] = GetSingleParameters(single_obj, args);
         # Create workload objects from information
         workload = FioWorkloadSpec(False, single_item, block_size, run_time, read_pct, io_type, io_depth, num_jobs, target_list);
         workload_list.append(workload);

      # Process Block Sweep Items
      for bsweep_item in yaml_obj['fio-gen']['block_sweep']:
         bsweep_obj  = yaml_obj['fio-gen']['block_sweep'][bsweep_item];
         ProcessBlockSweepObj(bsweep_obj, bsweep_item, workload_list, args);

      # Process Queue Depth Sweep Items
      for qdweep_item in yaml_obj['fio-gen']['qd_sweep']:
         qdweep_obj  = yaml_obj['fio-gen']['qd_sweep'][qdweep_item];
         ProcessQdSweepObj(qdweep_obj, qdweep_item, workload_list, args);

   except yaml.YAMLError, exc:
      print "Error in workload definition file: %s" % (exc);

   if (len(workload_list) == 0):
      print "ERR: invalid workload config file format in %s" % (args.CfgWorkloads.name);
      raise SystemExit(1);

   # Validate sequences against workload definitions; make sure they all exist.
   # Warn if they don't exist but are NOT selected as target sequence.
   # Fail if they don't exist AND ARE selected as target sequence.
   # TODO: implement

   # Generate script output
   print "Clearing previous scripts from output folder: %s" % (args.CfgOutFolder);
   subprocess.call(["rm -rf %s/*.fio" % (args.CfgOutFolder)], shell=True);
   GenerateFioScripts(args, workload_list);

   raise SystemExit(0);
