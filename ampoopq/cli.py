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

from . import PRJ, STR_VERSION
from . import conf, connection, pong_node, poopfs_node, script_node


#==============================================================================
# CONSTANTS
#==============================================================================

logger = conf.getLogger()


#==============================================================================
# FUNCTIONS
#==============================================================================

def main():
    lconf = conf.conf_from_file()

    parser = argparse.ArgumentParser(prog=PRJ, version=STR_VERSION)

    # Global Options
    parser.add_argument('connection', help="AMPQ URL")

    subparsers = parser.add_subparsers(help="Commands help")

    #==========================================================================
    # UPLOAD SUBPARSE
    #==========================================================================

    def manage_upload(args):
        pong_sub, popfs_pub = None, None
        try:
            conn = args.connection
            logger.info("Start discover nodes...")
            pong_sub = pong_node.PongSubscriber(conn, lconf)
            pong_sub.start()

            logger.info("Start uploading file...")
            poopfs_pub = poopfs_node.PoopFSPublisher(
                conn, lconf, args.filepath, args.poopFSpath
            )
            poopfs_pub.start()
            poopfs_pub.join()
        finally:
            logger.info("Killing processes..")
            if pong_sub:
                pong_sub.terminate()
            if popfs_pub:
                poopfs_pub.terminate()


    upload_cmd = subparsers.add_parser('upload', help='upload file to poopFS')
    upload_cmd.add_argument('filepath', help='file to upload')
    upload_cmd.add_argument('poopFSpath', help='file path to upload')
    upload_cmd.set_defaults(func=manage_upload)

    #==========================================================================
    # DEPLOY SUBPARSE
    #==========================================================================

    def manage_deploy(args):
        conn = args.connection

        msg = "Start announce my existence..."
        logger.info(msg)
        pong_pub = pong_node.PongPublisher(conn, lconf)
        pong_pub.start()

        msg = "Starting poopFS on '{}'..."
        logger.info(msg.format(lconf.POOP_FS))
        poopfs_sub = poopfs_node.PoopFSSuscriber(conn, lconf)
        poopfs_sub.start()

        msg = "Starting scripts deployment storage on '{}'..."
        logger.info(msg.format(lconf.SCRIPTS))
        script_sub = script_node.ScriptSuscriber(conn, lconf)
        script_sub.start()

    deploy_cmd = subparsers.add_parser('deploy', help='Deploy AMPoopQ node')
    deploy_cmd.set_defaults(func=manage_deploy)

    #==========================================================================
    # RUN SUBPARSE
    #==========================================================================

    def manage_run(args):
        conn = args.connection
        logger.info("Start discover nodes...")
        pong_sub = pong_node.PongSubscriber(conn, lconf)
        pong_sub.start()

        logger.info("Deploy script...")
        iname = "{}.py".format(uuid.uuid4().hex)
        script_pub = script_node.ScriptPublisher(
            conn, lconf, args.script, iname
        )
        script_pub.start()
        script_pub.join()
        script_pub.terminate()


    run_cmd = subparsers.add_parser('run', help='run script on AMPoopQ')
    run_cmd.add_argument('script', help='script to run')
    run_cmd.add_argument('out', help='output directory')
    run_cmd.add_argument(
        '--files', nargs='+', help='files of poopFS to process', default=()
    )
    run_cmd.set_defaults(func=manage_run)

    args = parser.parse_args(sys.argv[1:])
    args.func(args)


if __name__ == "__main__":
    main()
