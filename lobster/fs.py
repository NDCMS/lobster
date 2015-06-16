import se

# Order matters: defines resolution attempts.
try:
    h = se.Hadoop()
except:
    pass
s = se.SRM()
l = se.Local()

import sys
sys.modules[__name__] = se.StorageElement()
