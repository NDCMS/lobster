# Lobster Conda Environment Setup
**Lobster now uses conda, never do cmsenv**

First login to earth, and then run the following commands. 
The general directory structure is: 
```
lobster-with-conda
    DBS
    lobster
``` 
If you'd like to use a different setup, just adjust the paths accordingly.
```
unset PYTHONPATH

mkdir lobster-with-conda
cd lobster-with-conda

git clone https://github.com/dmwm/DBS.git
git clone git@github.com:NDCMS/lobster.git
cd lobster
git checkout lobster-with-conda

conda env create -f lobster_env.yaml
conda activate lobster-with-conda
```

**Note:** this yaml creates an environment named `lobster-with-conda`, if you'd like to change the name of the environment, edit the first line of lobster_env.yaml before installing

Next, navigate back to the DBS directory and we will install a couple more packages). Run the following commands: 
```
python setup.py install_system -s dbs-client
python setup.py install_system -s pycurl-client
conda install -c conda-forge python-daemon
```

Then, navigate back to the cloned lobster directory (probably `cd ../lobster/`) and run the following command: 
```
pip install -e .
```

Now that the lobster-with-conda env is setup, in the future all you need to do is run the following: 
```
unset PYTHONPATH
conda activate lobster-with-conda
```

# Running a Simple Config
In the lobster repository, there is a python script called "simple.py". This has been updated to work with `lobster-with-conda` and can be run in the following way: 

1. Set up the necessary CMSSW release in the same directory as where you're running the config file (see directions below).
2. unset the pythonpath and start the lobster-with-conda environment 
3. in the lobster/examples directory, do:  `lobster process simple.py`
4. in the same directory, start a work_queue_factory with the following command: `nohup work_queue_factory -T condor -M "lobster_$USER.*" -dall -o /tmp/${USER}_factory.debug -C factory.json > /tmp/${USER}_factory.log &`

You can monitor the work_queue_factory by doing `work_queue_status` while in your conda environment.
You can monitor the lobster process status by doing `lobster status [lobster working dir path]`. 

After the jobs are completed, check the output. In general, lobster output is stored in `hadoop`. For this simple config, there should be an `output*.root` file stored in `/hadoop/store/user/USERNAME/lobster_test_*/ttH` that is roughly 8 MB. 

# Setting up a CMSSW environment for the simple example
For the simple.py script, we're using CMSSW_10_6_26. There are two options: 
1. install CMSSW_10_6_26 inside the same directory where simple.py is located 
    - `lobster/examples/`
    - inside the examples directory run `cmsrel CMSSW_10_6_26` (NOTE: this has to be done outside of lobster conda environment)
    - reminder: DO NO do cmsenv
2. install CMSSW_10_6_26 somewhere else, and edit the path in simple.py on line 45: `release='<your-path-to-CMSSW_10_6_26>'`

# Possible Errors
After submitting a lobster process and starting a work_queue_factory, if there are no errors in `process.err` or `process_debug.log` but workers are never assigned to the job do the follwing: 
- Kill the current work_queue_factory. 
- Restart the work_queue_factory using the absolute path of work_queue: `nohup /afs/crc.nd.edu/group/ccl/software/x86_64/redhat7/cctools/stable/bin/work_queue_factory -T condor -M "lobster_$USER.*" -dall -o /tmp/${USER}_factory.debug -C factory.json > /tmp/${USER}_factory.log &`

If you submit a lobster process and get an error related to `parrot_run` in the `process.err` file, do the following while in the conda environment to add to your path: 
```
export PATH="$PATH:/afs/crc.nd.edu/group/ccl/software/x86_64/redhat7/cctools/lobster-171-cd5e3e2c-cvmfs-70dfa0d6/bin"
```
