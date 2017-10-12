#! /usr/local/bin/python

import argparse
import yaml
import subprocess
import os, sys
import math
import json
import pprint

sys.path.append('./libs/');
from multihost import SvrMultiHostCustom
from remote_exec import SvrRemoteThreadBase

pp = pprint.PrettyPrinter(width=120, compact=True)
CFG_DEFAULT_SHORT_RUN_TIME = "2"   # seconds
CFG_DEFAULT_SCRIPT_VERSION = "0.2.0"

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
   workloads:
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


class FioWorkloadSection:
    def __init__(self, name, section_dict, short_run=False):
        self.Name       = name;
        self.ShortRun   = short_run;
        self.SectionKvp = section_dict;
        self.EnableLogs = False;
        self.XlateKeyList = [ 'run_time', 'norandommap', 'io_type', 'read_pct', 'en_lat_log', 'en_iops_log', 'en_bw_log', 'group_reporting' ];

    def __str__(self):
        return "[%s]" % (self.Name);

    def xlate_fio_kvp(self, key, value):
        kvp_txt = "";
        key     = key.strip(" \n\r");
        value   = value.strip(" \n\r");
        if key == 'run_time':
            # when set to -1 then the section is NOT time based but device "fill" based
            if self.ShortRun:
                # override runtime if this is a short run.
                value = CFG_DEFAULT_SHORT_RUN_TIME;
                print("WARNING: *** SHORT RUN ENABLED, RUNTIME OVERRIDE = %s seconds ***" % (value));
            if value != "-1":
                kvp_txt += "time_based\n";
                kvp_txt += "runtime = %s" % (value);
        elif key == 'norandommap':
            if value.lower() == "true":
                kvp_txt += "norandommap\n";
        elif key == 'group_reporting':
            if value.lower() == "true":
                kvp_txt += "group_reporting\n";
        elif key == 'io_type':
            if value.lower() in ['random', 'rand']:
                kvp_txt += "rw = randrw\n";
            elif value.lower() in ['sequential', 'seq']:
                kvp_txt += "rw = readwrite\n";
        elif key == 'read_pct':
            kvp_txt += "rwmixread = %s\n" % (value);
        elif key == 'en_lat_log':
            if value.lower() == "true":
                kvp_txt += "write_lat_log = %s.log\n" % (self.Name);
                self.EnableLogs = True;
        elif key == 'en_iops_log':
            if value.lower() == "true":
                kvp_txt += "write_iops_log = %s.log\n" % (self.Name);
                self.EnableLogs = True;
        elif key == 'en_bw_log':
            if value.lower() == "true":
                kvp_txt += "write_bw_log = %s.log\n" % (self.Name);
                self.EnableLogs = True;
        else:
            kvp_txt += "%s = %s\n" % (key, value);
        return kvp_txt;

    # to_fio() will turn the key / value pairs into fio config script but will leave
    #          off the [section] header, and will NOT output any target specs (i.e. filename)
    def to_fio(self):
        section_txt = "";
        kvp_items   = {};
        try:
            # python 3
            kvp_items = self.SectionKvp.items();
        except:
            # python 2
            kvp_items = self.SectionKvp.iter_items();

        # walk through the key / value pairs and interpret them into proper fio script text!
        for k,v in kvp_items:
            section_txt += self.xlate_fio_kvp(k, v);
        return section_txt;


# FioSpecAllTargetsSameWorkload - takes a section specification (key/value pair - from json or yaml)
# as input and creates a [global] heading for all targets to execute the same workload.  Each
# section contains only a [<unique_name> + <target_name>] section title and filename=<target_name>
#
class FioSpecAllTargetsSameWorkload:
    def __init__(self, section_key, section_values, target_list, default_spec={}, short_run=False):
        # Merge defaults and section spec, and allow section_spec to "override" defaults
        self.context = default_spec.copy();
        self.context.update(section_values);
        self.SectionSpec = FioWorkloadSection(section_key, self.context, short_run);
        self.Name        = section_key;

        # save off target list and name for generating script output (later).
        self.TargetList  = target_list;

    def to_fio(self):
        script_txt  = "# *** AUTOGEN FIO SCRIPT by fio-gen.py version %s ***\n" % (CFG_DEFAULT_SCRIPT_VERSION);
        script_txt += "\n"
        script_txt += "[global]\n";
        script_txt += self.SectionSpec.to_fio() + "\n";
        for target in self.TargetList:
            script_txt += "\n[%s%s]\nfilename=%s\n" % (self.Name, target.replace('/', '_'), target);
        return script_txt;

    def __str__(self):
        dbg_str  = "Context (dict):\n%s\n\n" % (pp.pformat(self.context));
        dbg_str += "Targets (str): %s\n" % (pp.pformat(self.TargetList));


def GenerateFioScripts(workload_list, out_folder):
    print("Generating fio scripts...");
    for workload in workload_list:
        # Create workload input file
        file_name = "%s/%s.fio" % (out_folder, workload.Name);
        print(" * writing workload file: %s" % (file_name));
        out_file = open(file_name, 'w');
        if (out_file is None):
            print("ERR: failure opening workload file %s for writing." % (file_name));
        else:
            out_file.write("%s\n" % (workload.to_fio()));
            out_file.flush();
        out_file.close();


#############################################

scr_heading = """#! /usr/bin/python
import subprocess
import json
import argparse

### autogenerated script using fio-gen.py
out_folder = "./json";

# Input arguments
def AddArgs(parser_obj):
    parser_obj.add_argument('-s', dest='CfgSequence', action='store', required=True, help='You must specify the test sequence to execute.');

def GetArgs():
    # create the top-level parser
    parser = argparse.ArgumentParser(description='fio complex workload executor input arguments.');
    AddArgs(parser);
    # parse the args and call whatever function was selected
    args = parser.parse_args();
    return args;
    
args = GetArgs();
# List of scripts to execute, in order
""";

scr_footing = """
# Execute each script with output matching the input name with .json
subprocess.call("rm -rf %s ; sync ; mkdir %s" % (out_folder, out_folder), shell=True);
# During execution we will parse each json output file for iops, bw and latency averages
csv_str  = "Workload,Read_BW,Read_IOPS,Write_BW,Write_IOPS\\n";

if not (args.CfgSequence in sequence_dict.keys()):
    print("ERR: invalid sequence '%s' specified on command line." % (args.CfgSequence));
    raise SystemExit(1);
test_exec = sequence_dict[args.CfgSequence];
for fio_item in test_exec:
    json_file = "%s/%s.json" % (out_folder, fio_item);
    subprocess.call("fio %s --output-format=json --output %s ; sync" % (fio_item, json_file), shell=True);
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
print("Saving output to CSV: %s" % (out_file_name));
print(csv_str);
csv_file = open(out_file_name, "w");
csv_file.write(csv_str);
csv_file.close();
print("Workload execution complete.");
""";

def GenerateFioExecScript(sequence_dict, out_folder):
    print("Generating fio-exec.py script...");
    # Generate sequence "dictionary" for fio-exec.py to match with command line "sequence"
    # to execute.  The "sequence" is just a list of .fio scripts to execute, in order.
    var_declaration = "sequence_dict = {\n";
    for seq_key, value_list in GetIterItemsFromKVP(sequence_dict):
        this_seq = "    '%s': [ " % (seq_key);
        for value in value_list:
            this_seq += "'%s', " % (value);
        this_seq += "]";
        var_declaration += "%s,\n" % (this_seq);
    var_declaration += "    };";
    exec_script = "%s\n%s\n%s\n" % (scr_heading, var_declaration, scr_footing);
    scr_file = open("%s/fio-exec.py" % (out_folder), 'w');
    scr_file.write(exec_script);
    scr_file.flush();
    scr_file.close();


#############################################

def AddArgs(parser_obj):
    parser_obj.add_argument('-o', dest='CfgOutFolder', action='store', required=False, default="./out",
                            help='Specify a folder to place output files into.');
    parser_obj.add_argument('-w', dest='CfgWorkloads', action='store', required=False, type=argparse.FileType('r'),
                            default=None, help='Override the default workload YAML based config file.');


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

def GetIterItemsFromKVP(dict_obj, default={}):
    kvp_list = default;
    try:
        # python 3
        kvp_list = dict_obj.items();
    except:
        # python 2
        kvp_list = dict_obj.iter_items();
    return kvp_list;


def GetListFromCommaSepString(item_string):
    if (item_string is not None):
        items = item_string.split(',');
        if (len(items) == 0):
            items = [item_string.strip(' \n\r')];
        for item in items:
            if (item == None) or (item == ''):
                print("ERR: comma separated string missing value.");
                raise SystemExit(1);
    else:
        items = None;
    return items;


# Get Dict object from key / comma separated string value, with CSS as list (array)
def GetDictFromK_Vcss(kvp):
    new_dict = {}
    for k, v in GetIterItemsFromKVP(kvp):
       new_dict.update( { k: GetListFromCommaSepString(v) } );
    return new_dict;


def GetValueFromKey_safe(key, kvp, default):
    if key in kvp:
        if kvp[key] is not None:
            return kvp[key];
    return default;


def GetSweepParameters(sweep_obj):
    bs_list  = GetListFromCommaSepString(sweep_obj['block_size']);
    qd_list  = GetListFromCommaSepString(sweep_obj['io_depth']);
    job_list = GetListFromCommaSepString(sweep_obj['num_jobs']);
    io_type  = sweep_obj['io_type'];
    read_pct = sweep_obj['read_pct'];
    if ('run_time' in sweep_obj.keys()):
        run_time = sweep_obj['run_time'];
    else:
        run_time = "30";
    return [bs_list, qd_list, job_list, io_type, read_pct, run_time];


def ProcessBlockSweepObj(bsweep_obj, item_name, workload_list, target_list):
    [bs_list, qd_list, job_list, io_type, read_pct, run_time] = GetSweepParameters(bsweep_obj);
    # Create workload objects from information
    sequence_id = 0;
    for num_jobs in job_list:
        for io_depth in qd_list:
            for block_size in bs_list:
                wkload_name = "%s-%02d_%sj-%sqd-%s" % (item_name, sequence_id, num_jobs, io_depth, block_size);
                bs_obj = FioBlockSizeSpec('block_size', block_size);
                workload = FioWorkloadSpec(False, wkload_name, bs_obj, run_time, read_pct, io_type, io_depth, num_jobs, target_list);
                workload_list.append(workload);
                sequence_id += 1;


def ProcessQdSweepObj(qdsweep_obj, item_name, workload_list, target_list):
    [bs_list, qd_list, job_list, io_type, read_pct, run_time] = GetSweepParameters(qdsweep_obj);
    # Create workload objects from information
    sequence_id = 0;
    for num_jobs in job_list:
        for block_size in bs_list:
            for io_depth in qd_list:
                wkload_name = "%s-%02d_%sj-%sqd-%s" % (item_name, sequence_id, num_jobs, io_depth, block_size);
                bs_obj = FioBlockSizeSpec('block_size', block_size);
                workload = FioWorkloadSpec(False, wkload_name, bs_obj, run_time, read_pct, io_type, io_depth, num_jobs, target_list);
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
        print("Creating output folder: %s" % (args.CfgOutFolder));
        os.mkdir(args.CfgOutFolder);
        if (not os.path.exists(args.CfgOutFolder)):
            print("ERR: cannot create output folder %s" % (args.CfgOutFolder));
            raise SystemExit(1);

    # Determine what our target spec is so we can create the proper workload spec.
    # TODO: Get target groups
    # TODO: Turn target specification (from command line) to target groups

    # TODO: need to separate scripts by output folder; simple to simple, sweeps to sweeps
    # TODO: create input config file for fio-exec.py and make fio-exec.py an actual editable script!

    # create list of workload objects, from workload YAML config file.
    sequence_dict  = {};
    targets_dict   = {};
    global_default = {};
    workload_list  = list();
    try:
        # Load workload definition from file or local (default) string
        if (args.CfgWorkloads is not None):
            yaml_obj = yaml.load(args.CfgWorkloads);
        else:
            yaml_obj = yaml.load(CFG_DEFAULT_WORKLOAD_YAML);

        ################################################
        # Pre-process the input file into various lists to act on later.
        ################################################

        print("Pre-processing the workload specifications...");
        workload_spec_list = list();
        for section, values in GetIterItemsFromKVP(yaml_obj['fio-gen']):
            print(" * processing section: %s" % (section));
            if section.startswith('workloads'):
                workload_spec_list.append(values);
            elif section == 'sequence':
                # Get sequence declarations
                sequence_dict = GetDictFromK_Vcss(values);
            elif section == 'target_groups':
                # Get target (device) declarations
                target_dict = GetDictFromK_Vcss(values);
            elif section == 'global_default':
                # Obtain global defaults for all workloads* section declarations
                global_default = values;
            elif section == 'block_sweep':
                # Process Block Sweep Items
                #for bsweep_item in yaml_obj['fio-gen']['block_sweep']:
                #    bsweep_obj = yaml_obj['fio-gen']['block_sweep'][bsweep_item];
                #    ProcessBlockSweepObj(bsweep_obj, bsweep_item, workload_list, target_list);
                print("WARNING: \"block_sweep\" parsing NOT IMPLEMENTED!");
            elif section == 'qd_sweep':
                # Process Queue Depth Sweep Items
                #for qdweep_item in yaml_obj['fio-gen']['qd_sweep']:
                #    qdweep_obj = yaml_obj['fio-gen']['qd_sweep'][qdweep_item];
                #    ProcessQdSweepObj(qdweep_obj, qdweep_item, workload_list, target_list);
                print("WARNING: \"qd_sweep\" parsing NOT IMPLEMENTED!");
            else:
                print("WARNING: unrecognized section %s found in workload spec, ignoring!" % (section));

        # Display some stats on what was found in pre-processing...
        num_sequences      = len(sequence_dict.keys());
        num_targets        = len(target_dict.keys());
        num_workload_specs = len(workload_spec_list);
        print("There are %s \"sequence\" declarations to process." % (num_sequences))
        print("There are %s \"target_group\" declarations to process." % (num_targets))
        print("There are %d \"workloads\" sections to process." % (num_workload_specs));

        # Validate inputs, and fail out if warranted...
        if num_targets == 0:
            print("ERR: missing \"target_groups\" declaration in workload specification!")
            raise SystemExit(1);

        for workload_spec in workload_spec_list:
            print("Generating workload configurations...")
            # merge global defaults and section specific defaults.
            section_default = global_default.copy();
            section_default.update(workload_spec['section_default']);

            section_targets = GetValueFromKey_safe('section_targets', workload_spec, None);
            target_list     = GetValueFromKey_safe(section_targets, target_dict, None);
            if target_list is None:
                print("ERR: unknown target group (-t %s) option specified" % (section_targets));
                raise SystemExit(1);

            # process workload spec; but skip the 'section_default' declaration.
            for section_key, section_values in GetIterItemsFromKVP(workload_spec):
                if (section_key != 'section_default') and (section_key != 'section_targets'):
                    # we had to pre-process the defaults because we can't rely on ordering of iteration.
                    print(" * generating workload spec for: %s" % (section_key))
                    workload = FioSpecAllTargetsSameWorkload(section_key, section_values, target_list, section_default)
                    workload_list.append(workload);

    except yaml.YAMLError as exc:
        print("Error in workload definition file: %s" % (exc));

    if (len(workload_list) == 0):
        print("ERR: invalid workload config file format in %s" % (args.CfgWorkloads.name));
        raise SystemExit(1);

    # Validate sequences against workload definitions; make sure they all exist.
    # Warn if they don't exist but are NOT selected as target sequence.
    # Fail if they don't exist AND ARE selected as target sequence.
    # TODO: implement

    # Generate script output
    print("Clearing previous scripts from: %s" % (args.CfgOutFolder));
    subprocess.call(["rm -rf %s/*.fio" % (args.CfgOutFolder)], shell=True);
    GenerateFioScripts(workload_list, args.CfgOutFolder);

    # Generate fio-exec.py script
    print("Clearing previous fio-exec.py from: %s" % (args.CfgOutFolder));
    subprocess.call(["rm -rf %s/fio-exec.py" % (args.CfgOutFolder)], shell=True);
    GenerateFioExecScript(sequence_dict, args.CfgOutFolder);

    raise SystemExit(0);
