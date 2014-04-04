# Error Codes

Lobster uses the following error codes:

| Code | Reason
| :--- | :-----
|  170 | Sandbox unpacking failure
|  171 | Failed to determine base release
|  172 | Failed to find old releasetop
|  173 | Failed to create new release area
|  174 | `cmsenv` failure
|  190 | Failed to parse report.xml
|  191 | Failed to parse wrapper timing information
|  192 | Failed to parse CMSSW timing information
|  193 | Failed to write timing information
|  194 | Failed to compress output files
|  200 | Generic parrot failure
|  500 | Publish failure

Error codes lower than 170 may indicate a `cmsRun` problem.
