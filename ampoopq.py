#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

import pika


#==============================================================================
# CONSTANTS
#==============================================================================

ENV_AMPOOPQ_URL = "AMPOOPQ_URL"

AMPOOPQ_URL = os.getenv(ENV_AMPOOPQ_URL)

PREFIX_POOPFS = "poopFS://"

POOPFS_E = "poopFS_exchange"
CODE_E = "code_exchange"
DISCOVER_E = "discover_exchange"
DISCOVERED_E = "discovered_exchange"
MAP_E = "map_exchange"
MAP_RESULT_E = "map_result_exchange"

DISCOVER_TIME = 1


#==============================================================================
#
#==============================================================================

class MapContext(object):

    def __init__(self, node_id, op, channel):
        self.channel = channel
        self.node_id = node_id
        self.op = op

    def write(self, key, value):
        body = pickle.dumps({
            "key": key, "value": value, "node_id": self.node_id, "op": self.op
        }).encode("base64")
        self.channel.exchange_declare(exchange=MAP_RESULT_E, type='fanout')
        self.channel.basic_publish(
            exchange=MAP_RESULT_E, routing_key='', body=body
        )
        print "je"



#==============================================================================
#
#==============================================================================

class AMPoopQ(object):

    def __init__(self, conn_str):
        self.conn_str = conn_str
        self.params = pika.URLParameters(conn_str)
        self.connection = pika.BlockingConnection(self.params)

    def _validate_poopfs_filename(self, poopFS_path):
        if not poopFS_path.startswith(PREFIX_POOPFS):
            raise ValueError(
                "poopFS path must start with '{}'".format(PREFIX_POOPFS)
            )

    def _split_fnames(self, fname, nodes_n):
        buff = list(fname)
        random.shuffle(buff)
        group_size = int(len(buff) / nodes_n)
        for _ in xrange(nodes_n):
            group = []
            while len(group) < group_size and buff:
                group.append(buff.pop())
            if len(buff) == 1:
                group.extend(buff)
            yield tuple(group)
        if buff:
            yield tuple(buff)

    def upload(self, file_path, poopFS_path):
        self._validate_poopfs_filename(poopFS_path)
        to_path = poopFS_path.replace(PREFIX_POOPFS, "", 1)
        channel = self.connection.channel()
        channel.exchange_declare(exchange=POOPFS_E, type='fanout')
        with open(file_path, "rb") as fp:
            src = fp.read()
        body = pickle.dumps({
            "poopFS_path": to_path, "src": src
        }).encode("base64")

        print "Uploading '{}' to '{}'...".format(file_path, poopFS_path)
        channel.basic_publish(exchange=POOPFS_E, routing_key='', body=body)

    #==========================================================================
    # RUN SCRIPTS ON AMPoopQ
    #==========================================================================

    def run(self, script_path, out_path, files=None):
        out_file = None
        try:
            # clean files
            files = frozenset(files) if files else ()
            map(self._validate_poopfs_filename, files)

            # prepare the output
            if not os.path.isdir(out_path):
                os.makedirs(out_path)
            out_file_path = os.path.join(out_path, uuid.uuid4().hex + ".txt")
            out_file = codecs.open(out_file_path, "w", encoding="utf8")

            # prepare chanel
            channel = self.connection.channel()
            nodes = set()

            #==================================================================
            # Get the list of nodes
            #==================================================================

            def discovered_callback(ch, method, properties, body):
                nodes.add(body)

            channel.exchange_declare(exchange=DISCOVERED_E, type='fanout')
            result = channel.queue_declare(exclusive=True)
            queue_name = result.method.queue
            channel.queue_bind(exchange=DISCOVERED_E, queue=queue_name)
            channel.basic_consume(
                discovered_callback, queue=queue_name, no_ack=False
            )

            channel.exchange_declare(exchange=DISCOVER_E, type='fanout')
            print "Discovering nodes for {} seconds...".format(DISCOVER_TIME)
            channel.basic_publish(
                exchange=DISCOVER_E, routing_key='', body="ping"
            )
            self.connection.add_timeout(
                DISCOVER_TIME, channel.stop_consuming
            )
            channel.start_consuming()

            #==================================================================
            # Distribute Code
            #==================================================================

            # create a new name for the file
            file_iname = '{}.py'.format(uuid.uuid4().hex)

            channel.exchange_declare(exchange=CODE_E, type='fanout')

            with open(script_path, "rb") as fp:
                src = fp.read()
            body = pickle.dumps({
                "file_iname": file_iname, "src": src
            }).encode("base64")

            print "Distributing Code '{}'...".format(script_path)
            channel.basic_publish(exchange=CODE_E, routing_key='', body=body)

            #==================================================================
            # MAP
            #==================================================================
            nodes_n = len(nodes)
            nodebuff = list(nodes)

            print "Spliting files in '{}' groups".format(nodes_n)
            channel.exchange_declare(exchange=MAP_E, type='fanout')
            for group in self._split_fnames(files, nodes_n):
                routing_key = nodebuff.pop()
                body = pickle.dumps({
                    "script": file_iname, "files": group,
                    "op": uuid.uuid4().hex
                }).encode("base64")
                channel.basic_publish(
                    exchange=MAP_E, routing_key=routing_key, body=body
                )

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

        myid = uuid.uuid4().hex
        print "-> My id: ".format(myid)

        #======================================================================
        # PATH
        #======================================================================

        poopfs_path = os.path.join(path, "_poopfs")
        if not os.path.isdir(poopfs_path):
            os.makedirs(poopfs_path)
        code_path = os.path.join(path, "_code")
        if not os.path.isdir(code_path):
            os.makedirs(code_path)

        channel = self.connection.channel()

        #======================================================================
        # DOWNLOAD FILES
        #======================================================================

        def download_callback(ch, method, properties, body):
            data = pickle.loads(body.decode("base64"))
            fname = data["poopFS_path"]
            src = data["src"]
            fpath = os.path.join(poopfs_path, fname)
            dirname = os.path.dirname(fpath)
            print "Receiving '{}{}'...".format(PREFIX_POOPFS, fname)
            if not os.path.isdir(dirname):
                os.makedirs(dirname)
            with open(fpath, "w") as fp:
                fp.write(src)

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
            data = pickle.loads(body.decode("base64"))
            fname = data["file_iname"]
            src = data["src"]
            fpath = os.path.join(code_path, fname)
            print "Recieving code '{}'...".format(fname)
            with open(fpath, "w") as fp:
                fp.write(src)

        channel.exchange_declare(exchange=CODE_E, type='fanout')
        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=CODE_E, queue=queue_name)
        channel.basic_consume(
            receive_code_callback, queue=queue_name, no_ack=False
        )
        print "-> code storage at '{}'".format(code_path)

        #======================================================================
        # DISCOVER
        #======================================================================

        def discovered_callback(ch, method, properties, body):
            print "Discovered!!!"
            channel.exchange_declare(exchange=DISCOVERED_E, type='fanout')
            channel.basic_publish(
                exchange=DISCOVERED_E, routing_key='', body=myid
            )

        channel.exchange_declare(exchange=DISCOVER_E, type='fanout')
        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=DISCOVER_E, queue=queue_name)
        channel.basic_consume(
            discovered_callback, queue=queue_name, no_ack=False
        )
        print "-> Ready to be discovered"

        #======================================================================
        # MAP
        #======================================================================

        def _load_files(files):
            for fname in files:
                fpath = os.path.join(
                    poopfs_path, fname.replace(PREFIX_POOPFS, "", 1)
                )
                if os.path.isfile(fpath):
                    with open(fpath) as fp:
                        yield fname, fp
                else:
                    yield fname, IOError(fname)

        def map_callback(ch, method, properties, body):
            data = pickle.loads(body.decode("base64"))
            op = data["op"]
            files = data["files"]
            script = data["script"]
            script_path = os.path.join(code_path, script)
            ctx = MapContext(myid, op, channel)

            def empty_map(context, files):
                print "No map found in {}".format(script)

            map_func = runpy.run_path(script_path).get("map", empty_map)
            for key, value in _load_files(files):
                map_func(ctx, key, value)

        channel.exchange_declare(exchange=MAP_E, type='fanout')
        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=MAP_E, queue=queue_name, routing_key=myid)
        channel.basic_consume(
            map_callback, queue=queue_name, no_ack=False
        )
        print "-> Ready to receive maps"


        print channel.start_consuming()


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
        poop.run(args.script, args.out, args.files)

    run_cmd = subparsers.add_parser('run', help='run script on AMPoopQ')
    run_cmd.add_argument('script', help='script to run')
    run_cmd.add_argument('out', help='output directory')
    run_cmd.add_argument(
        '--files', nargs='+', help='files of poopFS to process', default=()
    )
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
