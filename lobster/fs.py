# This is not really a module, but some sort of perverse hack.  When
# imported, i.e., with `import fs`, `fs` will not point to this module, but
# to one instance of `se.StorageElement`.  See the documentation there...
#
# NB, creating a diferent instance of `StorageElement` is OK, since all
# access methods are part of class variables, not instance variables.
import se
import sys
sys.modules[__name__] = se.StorageElement()
