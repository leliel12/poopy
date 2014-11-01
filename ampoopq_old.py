#!/usr/bin/env python
# -*- coding: utf-8 -*-

import shutil
import itertools
import os
import json

import pika


#==============================================================================
# CONSTANTS
#==============================================================================

FILE_E = "file_exchange"


#==============================================================================
#
#==============================================================================

class Node(object):

    def __init__(self, func, path):
        self.func = func
        self.path = path

    def __call__(self, *args, **kwargs):
        self.func(*args, **kwargs)

    def download(self, ch, method, properties, body):
        data = json.loads(body)
        fname = data["fname"]
        src = data["src"].decode("base64")
        fpath = os.path.join(self.path, fname)
        with open(fpath, "w") as fp:
            fp.write(src)

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

    def __init__(self, conn_str, path, nodeid):
        self.conn_str = conn_str
        self.params = pika.URLParameters(conn_str)
        self.connection = pika.BlockingConnection(self.params)
        self.map_context = Context()
        self.reduce_context = Context()
        self.mappers = []
        self.reducers = []
        self.path = path
        self.nodeid = nodeid

    def _split_params(self, params):
        mappers_n = float(len(self.mappers))
        if mappers_n == 0:
            raise ValueError("Not mappers found")
        elif mappers_n == 1:
            return list(params)
        params_groups = len(params) / mapper_n
        one_more = bool(len(params) % mapper_n)
        prev = 0
        for gn in range(mappers_n):
            if one_more and
                yield params[prev:prev+params_groups+1]
            else:
                yield params[prev:prev+params_groups]
            prev = params_groups

    def upload(self, path):
        channel = self.connection.channel()
        channel.exchange_declare(exchange=FILE_E, type='fanout')
        fname = os.path.basename(path)
        with open(path, "rb") as fp:
            src = fp.read().encode("base64")
        body = json.dumps({"fname": fname, "src": src})
        channel.basic_publish(exchange=FILE_E, routing_key='', body=body)

    def start_poopfs(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange=FILE_E, type='fanout')
        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=FILE_E, queue=queue_name)
        for node in self.nodes:
            channel.basic_consume(node.download, queue=queue_name, no_ack=False)
        channel.start_consuming()

    def run(self, params, out):
        channel = self.connection.channel()
        params_groups = self._split_params(params)






    def mapper(self, func):
        node = Node(func, self.path)
        self.mappers.append(node)
        return func

    def reducer(self, func):
        node = Node(func, self.path)
        self.reducers.append(node)
        return func

    def cliexe(self, cmd, *params):
        if cmd == "upload":
            for fpath in params:
                print "Uploading '{}' to poopFS...".format(fpath)
                self.upload(fpath)
        elif cmd == "poopfs":
            print "Starint poopFS daemon on '{}' ...".format(self.path)
            self.start_poopfs()
        elif cmd == "run":
            print "Running..."
            out = params.pop(-1)
            self.run(params, out)
            print "ouput '{}'".format(out)

    @property
    def nodes(self):
        return itertools.chain(self.mappers, self.reducers)

