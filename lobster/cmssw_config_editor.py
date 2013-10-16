import os
import shutil
import lobster
import re

def edit_IO_files(dataset, dataset_label, dataset_files, cmssw_config_filename):
    directory = '%s/%s' % (os.getcwd(), dataset_label)
    if not os.path.exists(directory): #to do: add safeguard against clobbering
        os.makedirs(directory)

    for index, file in enumerate(dataset_files):
        cmssw_config_file_base = re.sub('.py', '', cmssw_config_filename)
        modified_config_filename = '%s/%s_%d.py' % (directory, cmssw_config_file_base, index)
        shutil.copy(cmssw_config_filename, modified_config_filename)

        with open(modified_config_filename, 'a') as modified_config_file:
            with open('%s/cmssw_config_fragment.txt' % lobster.__name__) as fragment:
                modified_config_file.write(fragment.read().format(input_files=file))

