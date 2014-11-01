#!/usr/bin/env python
# -*- coding: utf-8 -*-

import shutil
import itertools
import os
import json
import argparse
import sys

import pika


#==============================================================================
# CONSTANTS
#==============================================================================

ENV_AMPOOPQ_URL = "AMPOOPQ_URL"

AMPOOPQ_URL = os.getenv(ENV_AMPOOPQ_URL)

POOPFS_E = "poopFS_exch"
CODE_E = "code_exch"


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
        channel = self.connection.channel()
        channel.exchange_declare(exchange=POOPFS_E, type='fanout')
        with open(file_path, "rb") as fp:
            src = fp.read().encode("base64")
        body = json.dumps({"poopFS_path": poopFS_path, "src": src})
        print "Uploading '{}' to 'poopFS://{}'...".format(
            file_path, poopFS_path
        )
        channel.basic_publish(exchange=POOPFS_E, routing_key='', body=body)

    def run(self, path):

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
        # DOWNLOAD CODE
        #======================================================================

        def download_callback(ch, method, properties, body):
            data = json.loads(body)
            fname = data["poopFS_path"]
            src = data["src"].decode("base64")
            fpath = os.path.join(poopfs_path, fname)
            dirname = os.path.dirname(fpath)
            print "Receiving '{}'...".format(fname)
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
        print "poopFS mounted at '{}'".format(poopfs_path)

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

    # Run Subparse
    def manage_run(args):
        poop.run(args.path)

    run_cmd = subparsers.add_parser('run', help='run')
    run_cmd.add_argument('path', help='file for storage code and poopFS')
    run_cmd.set_defaults(func=manage_run)

    if not AMPOOPQ_URL:
        msg = "Enviroment variable '{}' not found".format(ENV_AMPOOPQ_URL)
        raise EnvironmentError(msg)

    print "*** Using broker '{}' ***".format(AMPOOPQ_URL)

    poop = AMPoopQ(AMPOOPQ_URL)
    args = parser.parse_args(sys.argv[1:])
    args.func(args)




if __name__ == "__main__":
    main()
