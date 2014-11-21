import gzip
import sys

from IMProv.IMProvDoc import IMProvDoc
from ProdCommon.FwkJobRep.ReportParser import readJobReport
from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport

if len(sys.argv) < 3:
    print "usage: {0} output inputs...".format(sys.argv[0])
    sys.exit(1)

merged = FwkJobReport()
for r in sys.argv[2:]:
    print "> merging {0}".format(r)
    f = gzip.open(r)
    for report in readJobReport(f):
        merged.inputFiles += report.inputFiles
        if len(merged.files) == 0:
            merged.files = report.files
        else:
            for run, lumis in report.files[0]['Runs'].items():
                try:
                    merged.files[0]['Runs'][run] += lumis
                except KeyError:
                    merged.files[0]['Runs'][run] = lumis
            events = int(merged.files[0]['TotalEvents']) + int(report.files[0]['TotalEvents'])
            merged.files[0]['TotalEvents'] = str(events)
    f.close()

output = IMProvDoc("JobReports")
output.addNode(merged.save())

outname = sys.argv[1]
if not outname.endswith('.gz'):
    outname += '.gz'

outfile = gzip.open(outname, 'wb')
outfile.write(output.makeDOMDocument().toprettyxml())
outfile.close()

sys.exit(0)
