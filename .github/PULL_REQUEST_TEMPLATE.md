Please make sure that the following items are taken care of if needed:

- [ ] Has the database format changed? (renamed or new columns, tables)
  Or did any of the project layout change? (files required to run the
  workflows)
  - Please increase the version number `VERSION` after the imports in
    `lobster.util`.  This will ensure that Lobster does not try to load old
    projects with an incompatible version.
- [ ] Did the required _Work Queue_ version change?
  - Please update the *two* version numbers in `doc/install.rst`.  If a new
    `cctools` tarball is available, please update that, too.
- [ ] Are all additional dependencies in `setup.py`?
- [ ] [Mark any issues as closed either in commits or in this pull
  request.](https://help.github.com/articles/closing-issues-via-commit-messages/)
