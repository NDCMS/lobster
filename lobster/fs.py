# This is not really a module, but some sort of perverse hack.  When
# imported, i.e., with `import fs`, `fs` will not point to this module, but
# to one instance of `se.FileSystem`.  This is done so that all python
# files including `fs` within the same python process share one virtual
# filesystem configuration.
import se
import sys
sys.modules[__name__] = se.FileSystem()
