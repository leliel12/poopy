#!/usr/bin/env python
# -*- coding: utf-8 -*-

# "THE WISKEY-WARE LICENSE":
# <jbc.develop@gmail.com> wrote this file. As long as you retain this notice
# you can do whatever you want with this stuff. If we meet some day, and you
# think this stuff is worth it, you can buy me a WISKEY in return Juan BC


#==============================================================================
# DOCS
#==============================================================================

"""This module manipulate pupyeFS paths

"""


#==============================================================================
# IMPORTS
#==============================================================================


import os


#==============================================================================
# CONSTANTS
#==============================================================================

PREFIX_POOPY_FS = "poopyFS://"


#==============================================================================
# CLASS
#==============================================================================

def clean_poopyfs_filename(fpath):
    if not fpath.startswith(PREFIX_POOPY_FS):
            raise ValueError(
                "poopyFS path must start with '{}'".format(PREFIX_POOPY_FS)
            )
    return fpath.replace(PREFIX_POOPY_FS, "", 1)


def resolve_poopyfslocal(fpath, lconf, clean=True):
    if clean:
        fpath = clean_poopyfs_filename(fpath)
    return os.path.join(lconf.POOPY_FS, fpath)

