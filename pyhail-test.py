#!/usr/bin/env python

from __future__ import print_function
import os

import pyhail

input_vcf = os.path.join(
    '/gscmnt/gc2802/halllab/idas',
    'software/downloads/github/hail',
    'src/test/resources/sample.vcf'
)

output_vcf = os.path.join(
    '/gscmnt/gc2802/halllab/idas',
    'laboratory/hail-play',
    'idas-test-2/sample.vds'
)

hc = pyhail.HailContext()
hc.import_vcf(input_vcf).write(output_vcf)

print("ALL DONE")
