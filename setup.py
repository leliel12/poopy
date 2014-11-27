#!/usr/bin/env python
# -*- coding: utf-8 -*-

# "THE WISKEY-WARE LICENSE":
# <jbc.develop@gmail.com> wrote this file. As long as you retain this notice
# you can do whatever you want with this stuff. If we meet some day, and you
# think this stuff is worth it, you can buy me a WISKEY in return Juan BC


#==============================================================================
# DOCS
#==============================================================================

"""This file is for distribute AMPoopQ with distutils

"""

#==============================================================================
# CONSTANTS
#==============================================================================

REQUIREMENTS = ["pika>=0.9.14"]


#==============================================================================
# FUNCTIONS
#==============================================================================

if __name__ == "__main__":
    import os
    import sys

    from ez_setup import use_setuptools
    use_setuptools()

    from setuptools import setup, find_packages

    import ampoopq

    setup(
        name=ampoopq.PRJ.lower(),
        version=ampoopq.STR_VERSION,
        description=ampoopq.SHORT_DESCRIPTION,
        author=ampoopq.AUTHOR,
        author_email=ampoopq.EMAIL,
        url=ampoopq.URL,
        license=ampoopq.LICENSE,
        keywords=ampoopq.KEYWORDS,
        classifiers=ampoopq.CLASSIFIERS,
        packages=[pkg for pkg in find_packages() if pkg.startswith("ampoopq")],
        include_package_data=True,
        py_modules=["ez_setup"],
        entry_points={'console_scripts': ['ampoopq = ampoopq.cli:main']},
        install_requires=REQUIREMENTS,
    )
