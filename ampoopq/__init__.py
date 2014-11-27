#!/usr/bin/env python
# -*- coding: utf-8 -*-

# "THE WISKEY-WARE LICENSE":
# <jbc.develop@gmail.com> wrote this file. As long as you retain this notice
# you can do whatever you want with this stuff. If we meet some day, and you
# think this stuff is worth it, you can buy me a WISKEY in return Juan BC


#==============================================================================
# DOCS
#==============================================================================

u"""Pure Python implementation of Map-Reduce over AMPQ (rabitMQ)

"""


#==============================================================================
# CONSTANTS
#==============================================================================

# : This is the project name
PRJ = "AMPoopQ"

# : The project version as tuple of strings
VERSION = ("0", "2")

# : The project version as string
STR_VERSION = ".".join(VERSION)
__version__ = STR_VERSION

DOC = __doc__

# : The short description for pypi
SHORT_DESCRIPTION = []
for line in DOC.splitlines():
    if not line.strip():
        break
    SHORT_DESCRIPTION.append(line)
SHORT_DESCRIPTION = u" ".join(SHORT_DESCRIPTION)
del line


# : Clasifiers for optimize search in pypi
CLASSIFIERS = (
    "Topic :: Utilities",
    "License :: OSI Approved :: BSD License",
    "Programming Language :: Python :: 2",
)

URL = ""

DOC_URL = ""

AUTHOR = "JuanBC"

# : Email ot the autor
EMAIL = "jbc.develop@gmail.com"

# : The license name
LICENSE = "WISKEY-WARE"

FULL_LICENSE = u""""THE WISKEY-WARE LICENSE":
<jbc.develop@gmail.com> wrote this file. As long as you retain this notice
you can do whatever you want with this stuff. If we meet some day, and you
think this stuff is worth it, you can buy me a WISKEY in return Juan BC

"""

# : Keywords for search of pypi
KEYWORDS = """ampq rabitmq map reduce"""

# : If the program is en debug mode
DEBUG = __debug__


#==============================================================================
# MAIN
#==============================================================================

if __name__ == "__main__":
    print(__doc__)
