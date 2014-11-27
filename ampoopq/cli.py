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
import logging
import multiprocessing

from . import PRJ, STR_VERSION
from . import conf, connection, pong_node, execution_node


#==============================================================================
# CONSTANTS
#==============================================================================


logger = logging.getLogger(PRJ)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '[%(levelname)s|%(asctime)s] %(name)s > %(message)s'
)
ch.setFormatter(formatter)
logger.addHandler(ch)


#==============================================================================
# FUNCTIONS
#==============================================================================

def main():
    lconf = conf.conf_from_file()

    parser = argparse.ArgumentParser(prog=PRJ, version=STR_VERSION)

    # Global Options
    parser.add_argument(
        'connection', help="AMPQ URL", type=connection.AMPoopQConnection
    )

    subparsers = parser.add_subparsers(help="Commands help")

    # Upload subparse
    #~ def manage_upload(args):
        #~ poop.upload(args.file_path, args.poopFS_path)
#~
    #~ upload_cmd = subparsers.add_parser('upload', help='upload file to poopFS')
    #~ upload_cmd.add_argument('file_path', help='file to upload')
    #~ upload_cmd.add_argument('poopFS_path', help='file path to upload')
    #~ upload_cmd.set_defaults(func=manage_upload)

    # Deploy Subparse
    def manage_deploy(args):
        logger.info("Starting Pong on {}".format(args.connection.conn_str))
        pong_pub = pong_node.PongPublisher(args.connection, lconf)
        pong_pub.start()

    deploy_cmd = subparsers.add_parser('deploy', help='Deploy AMPoopQ node')
    deploy_cmd.set_defaults(func=manage_deploy)

    # Run subparse
    def manage_run(args):
        args.connection, lconf
        pong_sub = pong_node.PongSubscriber(args.connection, lconf)
        pong_sub.start()

        import ipdb; ipdb.set_trace()


    run_cmd = subparsers.add_parser('run', help='run script on AMPoopQ')
    #~ run_cmd.add_argument('script', help='script to run')
    #~ run_cmd.add_argument('out', help='output directory')
    #~ run_cmd.add_argument(
        #~ '--files', nargs='+', help='files of poopFS to process', default=()
    #~ )
    run_cmd.set_defaults(func=manage_run)

    args = parser.parse_args(sys.argv[1:])
    args.func(args)


if __name__ == "__main__":
    main()
