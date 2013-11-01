import os
from FWCore.PythonUtilities.LumiList import LumiList
import json

def split_by_lumi(config, dataset_info, task_directory):
    if config.has_key('lumi mask'):
        lumi_mask = LumiList(filename=config['lumi mask'])
        dataset_info.total_lumis = 0
        for file in dataset_info.files:
            dataset_info.lumis[file] = lumi_mask.filterLumis(dataset_info.lumis[file])
            dataset_info.total_lumis += len(dataset_info.lumis[file])

    lumis_per_task = config['lumis per task']
    lumis_processed = 0
    task_id = 0
    tasks = []
    files = iter(dataset_info.files)
    file = files.next()
    input_files_this_task = [file]
    task_lumis_remaining = dataset_info.lumis[file]
    while lumis_processed < dataset_info.total_lumis:
        for file in input_files_this_task:
            common_lumis = set(dataset_info.lumis[file]).intersection(set(task_lumis_remaining))
            if len(common_lumis) == 0 or len(dataset_info.lumis[file]) == 0:
                input_files_this_task.remove(file)
        while lumis_per_task <= len(task_lumis_remaining):
            task_lumis = LumiList(lumis=task_lumis_remaining[:lumis_per_task])
            task_lumis_remaining = task_lumis_remaining[lumis_per_task:]
            tasks.append((input_files_this_task, task_lumis.getVLuminosityBlockRange()))
            task_id += 1
            lumis_processed += lumis_per_task
        try:
            file = files.next()
            input_files_this_task.append(file)
            task_lumis_remaining.extend(dataset_info.lumis[file])
        except:
            lumis_per_task = len(task_lumis_remaining)

    with open(os.path.join(task_directory, 'task_list.json'), 'w') as json_file:
        json.dump(tasks, json_file)

    return len(tasks)
