from se import FileSystem, Local, Hadoop, SRM

# Order matters: defines resolution attempts.
h = Hadoop()
s = SRM()
l = Local()

import sys
sys.modules[__name__] = FileSystem()
