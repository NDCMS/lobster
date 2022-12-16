# Lobster Conda Environment Setup
**Lobster now uses conda, never do cmsenv**

First login to earth, and then run the following commands. 
The general directory structure is: 
lobster-with-conda
        DBS
        lobster
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
**Note:** at the end of the packages being installed, you will get a message saying `ERROR: No matching distribution found for dbs-client==3.4.4`. This is fine, we will install the DBS packages next.

**Note:** this yaml creates an environment named `lobster-with-conda`, if you'd like to change the name of the environment, edit the first line of lobster_env.yaml before installing

Next, navigate back to the DBS directory and we will install the missing packaged (probably `cd ../DBS/`). Run the following commands: 
```
python setup.py install_system -s dbs-client
python setup.py install_system -s pycurl-client
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
In the lobster repository, there is a python script called "simple_config.py". This has been updated to work with `lobster-with-conda` and can be run in the following way: 

1. Set up the necessary CMSSW release in the same directory as where you're running the config file (see directions below).
2. unset the pythonpath and start the lobster-with-conda environment 
3. in the lobster/examples directory, do:  `lobster process simple_config.py`
4. in the same directory, start a work_queue_factory with the following command: `nohup work_queue_factory -T condor -M "lobster_$USER.*" -dall -o /tmp/${USER}_factory.debug -C factory.json > /tmp/${USER}_factory.log &`

# Setting up a CMSSW environment for the simple example
For the simple.py script, we're using CMSSW_10_6_26. There are two options: 
1. install CMSSW_10_6_26 inside the same directory where simple.py is located 
    - `lobster/examples/`
    - inside the examples directory run `cmsrel CMSSW_10_6_26` (NOTE: this has to be done outside of lobster conda environment)
    - reminder: DO NO do cmsenv
2. install CMSSW_10_6_26 somewhere else, and edit the path in simple.py on line 45: `release='<your-path-to-CMSSW_10_6_26>'`
