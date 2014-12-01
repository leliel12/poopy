#!/usr/bin/env python
# -*- coding: utf-8 -*-

# "THE WISKEY-WARE LICENSE":
# <jbc.develop@gmail.com> wrote this file. As long as you retain this notice
# you can do whatever you want with this stuff. If we meet some day, and you
# think this stuff is worth it, you can buy me a WISKEY in return Juan BC


#==============================================================================
# DOCS
#==============================================================================

"""This file is for distribute Poopy with distutils

"""

#==============================================================================
# CONSTANTS
#==============================================================================

REQUIREMENTS = [
    "pika>=0.9.14",
    "numpy>=1.9.1",
    "scipy>=0.14.0",
    "scikit-learn>=0.15.2",
    "unicodecsv>=0.9.4"
]


#==============================================================================
# FUNCTIONS
#==============================================================================

if __name__ == "__main__":
    import os
    import sys

    from ez_setup import use_setuptools
    use_setuptools()

    from setuptools import setup, find_packages

    import poopy

    setup(
        name=poopy.PRJ.lower(),
        version=poopy.STR_VERSION,
        description=poopy.SHORT_DESCRIPTION,
        author=poopy.AUTHOR,
        author_email=poopy.EMAIL,
        url=poopy.URL,
        license=poopy.LICENSE,
        keywords=poopy.KEYWORDS,
        classifiers=poopy.CLASSIFIERS,
        packages=[pkg for pkg in find_packages() if pkg.startswith("poopy")],
        include_package_data=True,
        py_modules=["ez_setup"],
        entry_points={'console_scripts': ['poopy = poopy.cli:main']},
        install_requires=REQUIREMENTS,
    )
