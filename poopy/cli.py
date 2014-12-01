#!/usr/bin/env python
# -*- coding: utf-8 -*-

# "THE WISKEY-WARE LICENSE":
# <jbc.develop@gmail.com> wrote this file. As long as you retain this notice
# you can do whatever you want with this stuff. If we meet some day, and you
# think this stuff is worth it, you can buy me a WISKEY in return Juan BC


#==============================================================================
# DOCS
#==============================================================================

"""Poopy command line interface

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

from . import PRJ, STR_VERSION, DOC, WARRANTY
from . import (
    conf, connection, script, pong_node, poopyfs_node, script_node, map_node
)


#==============================================================================
# CONSTANTS
#==============================================================================

logger = conf.getLogger()


#==============================================================================
# ERROR
#==============================================================================

class PoopyError(Exception): pass


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

    parser = argparse.ArgumentParser(
        PRJ, version=STR_VERSION, description=DOC,
        epilog=WARRANTY
    )


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
        'createscript', help='Create a new Poopy script'
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
            poopyfs_pub = poopyfs_node.PoopyFSPublisher(
                conn, lconf, args.filepath, args.poopyFSpath
            )
            ctx.add(poopyfs_pub)
            poopyfs_pub.start()
            poopyfs_pub.join()


    upload_cmd = subparsers.add_parser('upload', help='upload file to poopyFS')
    upload_cmd.add_argument('connection', help="AMPQ URL")
    upload_cmd.add_argument('filepath', help='file to upload')
    upload_cmd.add_argument('poopyFSpath', help='file path to upload')
    upload_cmd.set_defaults(func=manage_upload)

    #==========================================================================
    # DEPLOY SUBPARSE
    #==========================================================================

    def manage_deploy(args):
        with proccontext() as ctx:
            conn = args.connection

            msg = "Starting poopyFS on '{}'..."
            logger.info(msg.format(lconf.POOPY_FS))
            poopyfs_sub = poopyfs_node.PoopyFSSuscriber(conn, lconf)
            ctx.add(poopyfs_sub)
            poopyfs_sub.start()

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

    deploy_cmd = subparsers.add_parser('deploy', help='Deploy Poopy node')
    deploy_cmd.add_argument('connection', help="AMPQ URL")
    deploy_cmd.set_defaults(func=manage_deploy)

    #==========================================================================
    # RUN SUBPARSE
    #==========================================================================

    def manage_run(args):
        with proccontext() as ctx:
            conn = args.connection
            clsname = args.clsname

            logger.info("Start discover nodes...")
            pong_sub = pong_node.PongSubscriber(conn, lconf)
            ctx.add(pong_sub)
            pong_sub.start()

            iname = "i{}_{}".format(
                uuid.uuid4().hex, os.path.basename(args.script)
            )
            logger.info("Iname generated: {}".format(iname))
            logger.info("Reading script {}...".format(args.script))
            Cls = script.cls_from_path(args.script, clsname)
            instance = Cls()

            logger.info("Reading {} configuration...".format(args.script))
            job = script.Job(instance, iname)

            logger.info("Deploying script...")
            script_pub = script_node.ScriptPublisher(
                conn, lconf, args.script, job.iname
            )
            ctx.add(script_pub)
            script_pub.start()
            script_pub.join()

            logger.info("{} will start in {} seconds...".format(
                job.name, lconf.SLEEP
            ))
            time.sleep(lconf.SLEEP)
            uuids = pong_sub.uuids()
            if not uuids:
                msg = "No nodes found. Aborting..."
                raise PoopyError(msg)

            logger.info("Found {} nodes".format(len(uuids)))
            logger.info("Staring Maps...")
            map_pub = map_node.MapPublisher(conn, conf, job, uuids)
            ctx.add(map_pub)
            map_pub.start()
            map_pub.join()


    run_cmd = subparsers.add_parser('run', help='run script on Poopy cluster')
    run_cmd.add_argument('connection', help="AMPQ URL")
    run_cmd.add_argument('script', help='script to run')
    run_cmd.add_argument('clsname', help='class name inside the script')
    run_cmd.add_argument('out', help='output directory')
    run_cmd.set_defaults(func=manage_run)

    args = parser.parse_args(sys.argv[1:])
    args.func(args)


if __name__ == "__main__":
    main()
