# -*- coding: utf-8 -*-

import os
import sys
import logging

from scrapy.cmdline import execute

if len(sys.argv) < 2:
    logging.warning("No spider module name found, check the argv!")
    sys.exit(0)

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
if len(sys.argv) == 2:
    execute(['scrapy', 'crawl', sys.argv[1]])
elif len(sys.argv) == 3:
    execute(['scrapy', 'crawl', sys.argv[1], '-a', 'jobId=' + str(sys.argv[2])])