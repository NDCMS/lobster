import os
import shutil
import lobster
import re

def edit_IO_files(cmssw_config_filename, modified_config_filename, dataset_file):
    shutil.copy(cmssw_config_filename, modified_config_filename)

    with open(modified_config_filename, 'a') as modified_config_file:
        with open('%s/cmssw_config_fragment.txt' % lobster.__name__) as fragment:
            modified_config_file.write(fragment.read().format(input_files=file))


