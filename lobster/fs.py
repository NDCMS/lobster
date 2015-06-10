from se import FileSystem, Local, Hadoop, SRM

l = Local()
h = Hadoop()
s = SRM()

import sys
sys.modules[__name__] = FileSystem()
