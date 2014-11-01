#!/usr/bin/env python
# -*- coding: utf-8 -*-

import itertools
import os
import json
import argparse
import sys
import uuid
import codecs
import traceback

import pika


#==============================================================================
# CONSTANTS
#==============================================================================

ENV_AMPOOPQ_URL = "AMPOOPQ_URL"

AMPOOPQ_URL = os.getenv(ENV_AMPOOPQ_URL)

PREFIX_POOPFS = "poopFS://"

POOPFS_E = "poopFS_exchange"
CODE_E = "code_exchange"


#==============================================================================
#
#==============================================================================

class Context(object):

    def __init__(self):
        self.buff = []
        self.seek = 0

    def write(self, obj):
        self.buff.append(obj)

    def read(self, n=1):
        reads = 0
        while n < reads:
            yield self.buff[self.seek]
            n += 1
            self.seek += 1


#==============================================================================
#
#==============================================================================

class AMPoopQ(object):

    def __init__(self, conn_str):
        self.conn_str = conn_str
        self.params = pika.URLParameters(conn_str)
        self.connection = pika.BlockingConnection(self.params)
        self.map_context = Context()
        self.reduce_context = Context()
        self.mappers = []
        self.reducers = []

    def connect_nodes(self):
        pass

    def upload(self, file_path, poopFS_path):
        if not poopFS_path.startswith(PREFIX_POOPFS):
            raise ValueError(
                "poopFS path must start with '{}'".format(PREFIX_POOPFS)
            )
        to_path = poopFS_path.replace(PREFIX_POOPFS, "", 1)
        channel = self.connection.channel()
        channel.exchange_declare(exchange=POOPFS_E, type='fanout')
        with open(file_path, "rb") as fp:
            src = fp.read().encode("base64")
        body = json.dumps({"poopFS_path": to_path, "src": src})

        print "Uploading '{}' to '{}'...".format(file_path, poopFS_path)
        channel.basic_publish(exchange=POOPFS_E, routing_key='', body=body)

    #==========================================================================
    # RUN SCRIPTS ON AMPoopQ
    #==========================================================================

    def run(self, script_path, out_path):
        out_file = None
        try:
            if not os.path.isdir(out_path):
                os.makedirs(out_path)
            out_file_path = os.path.join(
                out_path, "{}.txt".format(uuid.uuid4())
            )
            out_file = codecs.open(out_file_path, "w", encoding="utf8")

            channel = self.connection.channel()

            #==================================================================
            # Distribute Code
            #==================================================================

            # create a new name for the file
            file_iname = '{}.py'.format(uuid.uuid4().hex)

            channel.exchange_declare(exchange=CODE_E, type='fanout')

            with open(script_path, "rb") as fp:
                src = fp.read().encode("base64")
            body = json.dumps({"file_iname": file_iname, "src": src})

            print "Distributing Code '{}'...".format(script_path)
            channel.basic_publish(exchange=CODE_E, routing_key='', body=body)

        except:
            print "ERROR <----------"
            traceback.print_exc(file=out_file)
        finally:
            if out_file:
                out_file.close()
            print "Your output: '{}'".format(out_file_path)



    #==========================================================================
    # DEPLOY AMPoopQ
    #==========================================================================

    def deploy(self, path):

        #======================================================================
        # PATH
        #======================================================================

        poopfs_path = os.path.join(path, "_poopfs")
        if not os.path.isdir(poopfs_path):
            os.makedirs(poopfs_path)
        code_path = os.path.join(path, "_code")
        if not os.path.isdir(code_path):
            os.makedirs(code_path)


        #======================================================================
        # DOWNLOAD FILES
        #======================================================================

        def download_callback(ch, method, properties, body):
            data = json.loads(body)
            fname = data["poopFS_path"]
            src = data["src"].decode("base64")
            fpath = os.path.join(poopfs_path, fname)
            dirname = os.path.dirname(fpath)
            print "Receiving '{}{}'...".format(PREFIX_POOPFS, fname)
            if not os.path.isdir(dirname):
                os.makedirs(dirname)
            with open(fpath, "w") as fp:
                fp.write(src)

        channel = self.connection.channel()
        channel.exchange_declare(exchange=POOPFS_E, type='fanout')
        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=POOPFS_E, queue=queue_name)
        channel.basic_consume(
            download_callback, queue=queue_name, no_ack=False
        )
        print "-> poopFS mounted at '{}'".format(poopfs_path)

        #======================================================================
        # RECEIVE CODE
        #======================================================================

        def receive_code_callback(ch, method, properties, body):
            data = json.loads(body)
            fname = data["file_iname"]
            src = data["src"].decode("base64")
            fpath = os.path.join(code_path, fname)
            print "Recieving code '{}'...".format(fname)
            with open(fpath, "w") as fp:
                fp.write(src)

        channel = self.connection.channel()
        channel.exchange_declare(exchange=CODE_E, type='fanout')
        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=CODE_E, queue=queue_name)
        channel.basic_consume(
            receive_code_callback, queue=queue_name, no_ack=False
        )
        print "-> code storage at '{}'".format(code_path)

        channel.start_consuming()


#==============================================================================
# FUNCTIONS
#==============================================================================

def main():
    parser = argparse.ArgumentParser(prog='AMPoopQ')
    subparsers = parser.add_subparsers(help='Commands help')
    poop = None

    # Upload subparse
    def manage_upload(args):
        poop.upload(args.file_path, args.poopFS_path)

    upload_cmd = subparsers.add_parser('upload', help='upload file to poopFS')
    upload_cmd.add_argument('file_path', help='file to upload')
    upload_cmd.add_argument('poopFS_path', help='file path to upload')
    upload_cmd.set_defaults(func=manage_upload)

    # Deploy Subparse
    def manage_deploy(args):
        poop.deploy(args.path)

    deploy_cmd = subparsers.add_parser('deploy', help='Deploy AMPoopQ node')
    deploy_cmd.add_argument('path', help='file for storage code and poopFS')
    deploy_cmd.set_defaults(func=manage_deploy)

    # Run subparse
    def manage_run(args):
        poop.run(args.script, args.out)

    run_cmd = subparsers.add_parser('run', help='run script on AMPoopQ')
    run_cmd.add_argument('script', help='script to run')
    run_cmd.add_argument('out', help='output directory')
    run_cmd.set_defaults(func=manage_run)

    if not AMPOOPQ_URL:
        msg = "Enviroment variable '{}' not found".format(ENV_AMPOOPQ_URL)
        raise EnvironmentError(msg)

    print "*** Using broker '{}' ***".format(AMPOOPQ_URL)

    try:
        poop = AMPoopQ(AMPOOPQ_URL)
        args = parser.parse_args(sys.argv[1:])
        args.func(args)
    except Exception as err:
        parser.error(str(err))




if __name__ == "__main__":
    main()
