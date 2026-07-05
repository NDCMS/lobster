# Lobster Conda Environment Setup
**Lobster now uses conda, never do cmsenv**

First login to glados, and then run the following commands. 
The general directory structure is: 
```
lobster-python3-run3
    lobster
``` 
If you'd like to use a different setup, just adjust the paths accordingly.
```
unset PYTHONPATH

mkdir lobster-python3
cd lobster-python3

git clone https://github.com/anpicci/lobster.git
cd lobster
git checkout lobster-python3-run3

conda env create -f lobster_env.yaml -n lobster
conda activate lobster

cd ..  # back to lobster-python3-run3/

git clone git@github.com:dmwm/WMCore.git --branch 2.3.5
cd WMCore
sed -i -E '/^(gfal2|htcondor|kkmysqlclient|rucio-clients|Sphinx|coverage|memory-profiler|mox3|nose|nose2|pycodestyle|pylint|pymongo)/d' requirements.txt
pip install -r requirements.txt .

cd ../lobster   # to lobster-python3-run3/lobster
```

**Note:** The yaml file and the above command create an environment named `lobster`. If you'd like to change the name of the environment, change the value after the `-n` flag, e.g., `conda env create -f lobster_env.yaml -n lobster-python3-run3`


Then, still in the cloned lobster directory, run the following command to install lobster as an editable package: 
```
pip install -e .
```

Now that the `lobster` env is set up, in the future all you need to do is run the following:
```
unset PYTHONPATH
unset PERL5LIB
conda activate lobster
export PATH=/afs/crc.nd.edu/group/ccl/software/x86_64/RedHat9/cctools/7.11.1/bin:$PATH ##needed to use parrot_run when sandboxing CMSSW
```

# Running a Simple Config
In the lobster repository, there is a Python script called "simple.py". This has been updated to work with `lobster-python3` and can be run in the following way:

1. Set up the necessary CMSSW release in the same directory as where you're running the config file (see directions below).
2. unset the pythonpath and start the `lobster` environment 
3. In the lobster/examples directory, do:  `lobster process simple.py`
4. In the same directory, start a work_queue_factory with the following command: `work_queue_factory -T condor -M "lobster_${USER}.*" -dall -o /tmp/wq-factory-${USER}/debug.log -C factory.json --runos cc7-wq-7.11.1 --scratch-dir /tmp/wq-factory-${USER} -B"+IsNotreDameCMSJob=True\n+AccountingGroup=\"cms_local.${USER}\"" > /tmp/wq-factory-${USER}/factory.log` 

(If it is your first time running this work_queue_factory command, do this first: `mkdir /tmp/wq-factory-${USER}`)

You can monitor the work_queue_factory by doing `work_queue_status` while in your conda environment.
You can monitor the lobster process status by doing `lobster status [lobster working dir path]`. 

This simple test script should spawn 3 data jobs and 1 merge job.  If you see repeated failed jobs in the logs then you should kill the process  
and look for additional error reports in all the log files to assist in debugging.

After the jobs are completed, check the output. In general, lobster output is stored in `cephfs`. For this simple config, there should be an `output*.root` file stored in `/cms/cephfs/data/store/user/USERNAME/lobster_test_*/ttH` that is roughly 8 MB.

# Setting up a CMSSW environment for the simple example
For the simple.py script, we're using CMSSW_10_6_26. There are two options: 
1. Install CMSSW_10_6_26 inside the same directory where simple.py is located
    - `lobster/examples/`
    - inside the examples directory, run `unset PERL5LIB` and `cmsrel CMSSW_10_6_26` (NOTE: this has to be done outside of lobster conda environment)
    - reminder: DO NOT do cmsenv
2. install CMSSW_10_6_26 somewhere else, and edit the path in simple.py on line 45: `release='<your-path-to-CMSSW_10_6_26>'`

# Possible Errors
After submitting a lobster process and starting a work_queue_factory, if there are no errors in `process.err` or `process_debug.log` but workers are never assigned to the job, try the following:
- Kill the current work_queue_factory. 
- Try running a worker directly with the following command:
- `apptainer exec --bind /cvmfs:/cvmfs --bind $CONDA_PREFIX:/conda_env /afs/crc.nd.edu/group/ccl/software/runos/images/cc7-wq-7.11.1.img /conda_env/bin/work_queue_worker -M "lobster_${USER}.*" -dall --cores 1 --disk 10000 -t 150`
- If the manual worker succeeds and not the work_queue_factory, then it may indicate a bug in lobster or your environment.  Save your logs and contact the development team for help.

# Other Troubleshooting
- Did you remember to renew your proxy?
- Unset PYTHONPATH and PERL5LIB?
- In the right conda env?  Run `work_queue_factory --version` and check the output for 7.11.1.
