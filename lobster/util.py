import os.path

def findpath(dirs, path):
    if len(dirs) == 0:
        return path

    for directory in dirs:
        joined = os.path.join(directory, path)
        if os.path.exists(joined):
            return joined
    raise KeyError, "Can't find '{0}' in {1}".format(path, dirs)
