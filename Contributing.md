# Concept

Lobster executes workflows, specified in a configuration file, containing
several jobs.  These jobs are broken down into tasks by a `JobProvider` (to
be renamed), and executed on workers via Work Queue.

Optional, CMS specific component that should be pluggable include:
dashboard monitoring, `cmsRun` support, sandboxing.

# Git Development

Lobster development should follow a style similar to the [github
workflow](http://scottchacon.com/2011/08/31/github-flow.html).  That is:

* the master branch is always stable
* development occurs in branches
* branches are merged via PRs
* PRs should be used for review and discussion
* minor fixes can be direct commits to the master

Git commit messages should be at least one comprehensive, short summary
line, optionally followed by one or more explanitory paragraphs, separated
from the summary by an empty line:

    Implement telepathic stageout and dashboard reporting.

    Done via [brain](http://en.wikipedia.org/wiki/Human_brain).  Required
    re-write of foo.py and bar.py.  Fixes #-1.

Non-descriptive summaries such as `Changed spam.py` should be avoided.

# Testing

Lobster uses `nose` to run unit tests, which can be found in the `test`
directory.  It should be automatically installed as a python dependency.
Some behavior can be tested with

    nosetests
    nosetests test/test_se.py:TestHadoopPermissions

where the latter executes one specific test case.
