#!/usr/bin/env python
# -*- coding: utf-8 -*-

# "THE WISKEY-WARE LICENSE":
# <jbc.develop@gmail.com> wrote this file. As long as you retain this notice
# you can do whatever you want with this stuff. If we meet some day, and you
# think this stuff is worth it, you can buy me a WISKEY in return Juan BC


#==============================================================================
# DOCS
#==============================================================================

"""AMPoopQ command line interface

"""


#==============================================================================
# IMPORTS
#==============================================================================

import itertools
import os
import runpy
import argparse
import sys
import uuid
import codecs
import traceback
import time
import random
import pickle
import multiprocessing
import contextlib

from . import PRJ, STR_VERSION
from . import conf, connection, pong_node, poopfs_node, script_node, script


#==============================================================================
# CONSTANTS
#==============================================================================

logger = conf.getLogger()


#==============================================================================
# FUNCTIONS
#==============================================================================

@contextlib.contextmanager
def proccontext():
    """This context manager store the process to be killed after end
    of execution of the end of the block

    """
    procs = set()
    try:
        yield procs
    finally:
        logger.info("Killing {} processes..".format(len(procs)))
        for proc in procs:
            try:
                proc.terminate()
            except Exception as err:
                logger.warning(err)


def main():
    lconf = conf.conf_from_file()

    parser = argparse.ArgumentParser(prog=PRJ, version=STR_VERSION)


    subparsers = parser.add_subparsers(help="Commands help")

    #==========================================================================
    # Create Empty Script
    #==========================================================================

    def manage_createscript(args):
        if os.path.exists(args.filepath):
            raise IOError("File '{}' already exists".format(args.filepath))
        with open(args.filepath, "w") as fp:
            fp.write(script.SCRIPT_TEMPLATE)
        logger.info("Script created at '{}'".format(args.filepath))

    createscript_cmd = subparsers.add_parser(
        'createscript', help='Create a new AMPoopQ script'
    )
    createscript_cmd.add_argument('filepath', help='file to create ')
    createscript_cmd.set_defaults(func=manage_createscript)

    #==========================================================================
    # UPLOAD SUBPARSE
    #==========================================================================

    def manage_upload(args):
        with proccontext() as ctx:
            conn = args.connection
            logger.info("Start discover nodes...")
            pong_sub = pong_node.PongSubscriber(conn, lconf)
            ctx.add(pong_sub)
            pong_sub.start()


            logger.info("Start uploading file...")
            poopfs_pub = poopfs_node.PoopFSPublisher(
                conn, lconf, args.filepath, args.poopFSpath
            )
            ctx.add(poopfs_pub)
            poopfs_pub.start()
            poopfs_pub.join()


    upload_cmd = subparsers.add_parser('upload', help='upload file to poopFS')
    upload_cmd.add_argument('connection', help="AMPQ URL")
    upload_cmd.add_argument('filepath', help='file to upload')
    upload_cmd.add_argument('poopFSpath', help='file path to upload')
    upload_cmd.set_defaults(func=manage_upload)

    #==========================================================================
    # DEPLOY SUBPARSE
    #==========================================================================

    def manage_deploy(args):
        with proccontext() as ctx:
            conn = args.connection

            msg = "Starting poopFS on '{}'..."
            logger.info(msg.format(lconf.POOP_FS))
            poopfs_sub = poopfs_node.PoopFSSuscriber(conn, lconf)
            ctx.add(poopfs_sub)
            poopfs_sub.start()

            msg = "Starting scripts deployment storage on '{}'..."
            logger.info(msg.format(lconf.SCRIPTS))
            script_sub = script_node.ScriptSuscriber(conn, lconf)
            ctx.add(script_sub)
            script_sub.start()

            msg = "Start announce my existence..."
            logger.info(msg)
            pong_pub = pong_node.PongPublisher(conn, lconf)
            ctx.add(pong_pub)
            pong_pub.start()
            pong_pub.join()

    deploy_cmd = subparsers.add_parser('deploy', help='Deploy AMPoopQ node')
    deploy_cmd.add_argument('connection', help="AMPQ URL")
    deploy_cmd.set_defaults(func=manage_deploy)

    #==========================================================================
    # RUN SUBPARSE
    #==========================================================================

    def manage_run(args):
        with proccontext() as ctx:
            conn = args.connection
            logger.info("Start discover nodes...")
            pong_sub = pong_node.PongSubscriber(conn, lconf)
            ctx.add(pong_sub)
            pong_sub.start()

            logger.info("Deploy script...")
            iname = "i{}_{}".format(
                uuid.uuid4().hex, os.path.basename(args.script)
            )
            script_pub = script_node.ScriptPublisher(
                conn, lconf, args.script, iname
            )
            ctx.add(script_pub)
            script_pub.start()
            script_pub.join()

            logger.info(
                "Running will start in {} seconds...".format(lconf.SLEEP)
            )
            time.sleep(lconf.SLEEP)

    run_cmd = subparsers.add_parser('run', help='run script on AMPoopQ')
    run_cmd.add_argument('connection', help="AMPQ URL")
    run_cmd.add_argument('script', help='script to run')
    run_cmd.add_argument('out', help='output directory')
    run_cmd.set_defaults(func=manage_run)

    args = parser.parse_args(sys.argv[1:])
    args.func(args)


if __name__ == "__main__":
    main()
