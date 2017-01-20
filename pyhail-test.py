#!/usr/bin/env python

from __future__ import print_function
import os

import pyhail

input_vcf = os.path.join(
    '/gscmnt/gc2802/halllab/idas',
    'software/downloads/github/hail',
    'src/test/resources/sample.vcf'
)

output_vds = os.path.join(
    '/gscmnt/gc2802/halllab/idas',
    'laboratory/hail-play',
    'idas-test-3/sample.vds'
)

hail_logfile = os.path.join(
    '/gscmnt/gc2802/halllab/idas',
    'laboratory/hail-play',
    'idas-test-3/test-hail.log'
)

tmp = os.path.join(
    '/gscmnt/gc2802/halllab/idas',
    'laboratory/hail-play',
    'idas-test-3/tmp'
)

hc = pyhail.HailContext(log=hail_logfile, tmp_dir=tmp)
hc.import_vcf(input_vcf).write(output_vds)

vds = hc.read(output_vds)
stats = vds.count(genotypes=True)
print("****** {} ******".format(stats))

print("ALL DONE")
