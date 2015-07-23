# Error Codes

Lobster uses the following error codes:

| Code | Reason
| :--- | :-----
|  4   | The task ran but its stdout has been truncated
|  8   | The task was terminated with a signal
|  16  | The task used more resources than requested
|  32  | The task ran after specified end time
|  169 | Unable to run parrot
|  170 | Sandbox unpacking failure
|  171 | Failed to determine base release
|  172 | Failed to find old releasetop
|  173 | Failed to create new release area
|  174 | `cmsenv` failure
|  179 | Stagein failure
|  180 | Prologue failure
|  190 | Failed to parse report.xml
|  191 | Failed to parse wrapper timing information
|  192 | Failed to parse CMSSW timing information
|  193 | Failed to write report information
|  194 | Failed to compress output files
|  199 | Epilogue failure
|  200 | Generic parrot failure
|  210 | Stageout failure during transfer
|  211 | Stageout failure cross-checking transfer
|  500 | Publish failure

Error codes lower than 170 may indicate a `cmsRun` problem, codes
O(1k) may hint at a CMS configuration or runtime problem.
