#!/usr/bin/env python
# -*- coding: utf-8 -*-

# "THE WISKEY-WARE LICENSE":
# <jbc.develop@gmail.com> wrote this file. As long as you retain this notice
# you can do whatever you want with this stuff. If we meet some day, and you
# think this stuff is worth it, you can buy me a WISKEY in return Juan BC


#==============================================================================
# DOCS
#==============================================================================

"""Map node of Poopy

"""


#==============================================================================
# IMPORTS
#==============================================================================

import time
import os
import multiprocessing

from . import connection, conf, serializer, script, readers, poopyfs


#==============================================================================
# CONSTANTS
#==============================================================================

REDUCE_E = "reduce_exchange"

REDUCE_RESPONSE_E = "reduce_response_exchange"

logger = conf.getLogger("Reduce")


#==============================================================================
# RESULT SUBSCRIBER
#==============================================================================

class MapResultSubscriber(multiprocessing.Process):

    def __init__(self, conn, conf, uuids, *args, **kwargs):
        super(MapResultSubscriber, self).__init__(*args, **kwargs)
        self.conn = conn
        self.lconf = conf
        self.uuids = set(uuids)
        self._queue = multiprocessing.Queue()
        self._ready = set()
        self._buff = {}

    def responses(self):
        return self._buff if self._ready == self.uuids else {}

    def ended(self):
        while self._queue.qsize():
            body = self._queue.get_nowait()
            data = serializer.loads(body)
            self._ready.add(data["uuid"])
            k, v = data["k"], data["v"]
            self._buff.setdefault(k, []).append(v)
        return self._ready == self.uuids

    # callback
    def _callback(self, ch, method, properties, body):
        self._queue.put(body)

    def run(self):
        conn = connection.PoopyConnection(self.conn)
        conn.exchange_consume(MAP_RESPONSE_E, self._callback)


#==============================================================================
# SUBSCRIBER
#==============================================================================

class ReduceSubscriber(multiprocessing.Process):

    def __init__(self, conn, conf, *args, **kwargs):
        super(ReduceSubscriber, self).__init__(*args, **kwargs)
        self.conn = conn
        self.lconf = conf
        self._pconn = None

    # callback
    def _callback(self, ch, method, properties, body):
        logger.info("Map Data recived!")
        data = serializer.loads(body)
        iname = data["iname"]
        clsname = data["clsname"]
        results = data["results"]

        inamepath = os.path.join(self.lconf.SCRIPTS, iname)
        instance = script.cls_from_path(inamepath, clsname)()
        job = script.Job(instance, clsname, iname, self.lconf.UUID)

        logger.info("Preparing for run reduce for job '{}'".format(job.name))
        conn = connection.PoopyConnection(self.conn)
        channel = conn.channel()
        channel.exchange_declare(exchange=REDUCE_RESPONSE_E, type='fanout')

        def emiter(k, v):
            body = serializer.dumps({"k": k, "v": v, "uuid": self.lconf.UUID})
            channel.basic_publish(
                exchange=REDUCE_RESPONSE_E, routing_key='', body=body
            )

        context = script.Context(emiter, job)
        try:
            for k, v in results.items():
                logger.info("Running reduce for key '{}'".format(k))
                instance.reduce(k, v, context)
        except Exception as err:
            logger.error(str(err))

    def run(self):
        conn = connection.PoopyConnection(self.conn)
        conn.exchange_consume(REDUCE_E, self._callback, self.lconf.UUID)


#==============================================================================
# PUBLISHER
#==============================================================================

class ReducePublisher(multiprocessing.Process):

    def __init__(self, conn, conf, script,
                 iname, clsname, uuids, results, *args, **kwargs):
        super(ReducePublisher, self).__init__(*args, **kwargs)
        self.conn = conn
        self.lconf = conf
        self.script = script
        self.iname = iname
        self.clsname = clsname
        self.uuids = uuids
        self.results = results

    def split_responses(self, job):
        results = self.results.items()
        chunk_size = len(self.uuids)
        for idx, chunk_from in enumerate(range(0, len(results), chunk_size)):
            chunk_to = chunk_from + chunk_size
            chunk = results[chunk_from:chunk_to]
            yield self.uuids[idx], dict(chunk)

    def run(self):
        logger.info("Reading script {}...".format(self.script))
        Cls = script.cls_from_path(self.script, self.clsname)
        instance = Cls()

        logger.info("Reading {} configuration...".format(self.script))
        job = script.Job(instance, self.clsname, self.iname)

        # publishing
        conn = connection.PoopyConnection(self.conn)
        channel = conn.channel()
        channel.exchange_declare(exchange=REDUCE_E, type='fanout')

        for uuid, results in self.split_responses(job):
            msg = (
                "Sending Reduce Execution signal for job '{}' to node '{}'"
            ).format(job.name, uuid)
            logger.info(msg)
            body = serializer.dumps({
                "iname": self.iname, "clsname": self.clsname,
                "results": results
            })
            channel.basic_publish(
                exchange=REDUCE_E, routing_key=uuid, body=body
            )

