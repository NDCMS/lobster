# Error Codes

TLobster uses the following error codes:

| Code   | Reason
| :---   | :-----
| 169    | Unable to run parrot
| 170    | Sandbox unpacking failure
| 171    | Failed to determine base release
| 172    | Failed to find old releasetop
| 173    | Failed to create new release area
| 174    | `cmsenv` failure
| 179    | Stagein failure
| 180    | Prologue failure
| 190    | Failed to parse report.xml
| 191    | Failed to parse wrapper timing information
| 192    | Failed to parse CMSSW timing information
| 193    | Failed to write report information
| 194    | Failed to compress output files
| 199    | Epilogue failure
| 200    | Generic parrot failure
| 210    | Stageout failure during transfer
| 211    | Stageout failure cross-checking transfer
| 500    | Publish failure
| 100001 | Missing input for task
| 100002 | Missing output for task
| 100004 | The task ran but its stdout has been truncated
| 100008 | The task was terminated with a signal
| 100016 | The task used more resources than requested
| 100032 | The task ran after specified end time
| 100064 | Unclassified error
| 100128 | Unrelated error
| 100256 | Exceeded number of retries (default: 10)
| 100512 | Exceeded runtime

Error codes lower than 170 may indicate a `cmsRun` problem, codes
O(1k) may hint at a CMS configuration or runtime problem.
Codes O(100k) are internal Work Queue error codes and may be bitmasked
together, i.e., 100514 is a combination of errors 100512 and 100002.
