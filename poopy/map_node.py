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

MAP_E = "map_exchange"

MAP_RESPONSE_E = "map_response_exchange"

logger = conf.getLogger("Map")


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

    def results(self):
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

class MapSubscriber(multiprocessing.Process):

    def __init__(self, conn, conf, *args, **kwargs):
        super(MapSubscriber, self).__init__(*args, **kwargs)
        self.conn = conn
        self.lconf = conf
        self._pconn = None

    # callback
    def _callback(self, ch, method, properties, body):
        logger.info("Map Data recived!")
        data = serializer.loads(body)
        iname = data["iname"]
        clsname = data["clsname"]
        files = data["files"]
        inamepath = os.path.join(self.lconf.SCRIPTS, iname)
        instance = script.cls_from_path(inamepath, clsname)()
        job = script.Job(instance, clsname, iname, self.lconf.UUID)

        logger.info("Preparing for run map for job '{}'".format(job.name))
        conn = connection.PoopyConnection(self.conn)
        channel = conn.channel()
        channel.exchange_declare(exchange=MAP_RESPONSE_E, type='fanout')

        def emiter(k, v):
            body = serializer.dumps({"k": k, "v": v, "uuid": self.lconf.UUID})
            channel.basic_publish(
                exchange=MAP_RESPONSE_E, routing_key='', body=body
            )

        context = script.Context(emiter, job)
        try:
            for fpath, reader_name, kwargs in files:
                logger.info("Running map for '{}'".format(fpath))
                reader = readers.BaseReader.reader_by_name(reader_name)
                lpath = poopyfs.resolve_poopyfslocal(
                    fpath, self.lconf, clean=True
                )
                data = reader.load(lpath, **kwargs)
                instance.map(fpath, data, context)
        except Exception as err:
            logger.error(str(err))

    def run(self):
        conn = connection.PoopyConnection(self.conn)
        conn.exchange_consume(MAP_E, self._callback, self.lconf.UUID)


#==============================================================================
# PUBLISHER
#==============================================================================

class MapPublisher(multiprocessing.Process):

    def __init__(self, conn, conf, script,
                 iname, clsname, uuids, *args, **kwargs):
        super(MapPublisher, self).__init__(*args, **kwargs)
        self.conn = conn
        self.lconf = conf
        self.script = script
        self.iname = iname
        self.clsname = clsname
        self.uuids = uuids

    def split_files(self, job):
        files = job.input_path
        chunk_size = len(self.uuids)
        for idx, chunk_from in enumerate(range(0, len(files), chunk_size)):
            chunk_to = chunk_from + chunk_size
            chunk = []
            for filename in files[chunk_from:chunk_to]:
                if isinstance(filename, (tuple, list)) and len(filename)  == 3:
                    filename, parser, kwargs = filename
                    chunk.append((filename, parser.__name__, kwargs))
                elif isinstance(filename, (tuple, list)):
                    filename, parser, = filename
                    chunk.append((filename, parser.__name__, {}))
                elif isinstance(filename, basestring):
                    chunk.append((filename, readers.TxtReader.__name__, {}))
                else:
                    raise ValueError(
                        "Filename must be an string or an tuple or "
                        "list with 2 or 3 elemenets"
                    )
            yield self.uuids[idx], chunk

    def run(self):
        logger.info("Reading script {}...".format(self.script))
        Cls = script.cls_from_path(self.script, self.clsname)
        instance = Cls()

        logger.info("Reading {} configuration...".format(self.script))
        job = script.Job(instance, self.clsname, self.iname)

        # publishing
        conn = connection.PoopyConnection(self.conn)
        channel = conn.channel()
        channel.exchange_declare(exchange=MAP_E, type='fanout')

        for uuid, files in self.split_files(job):
            msg = (
                "Sending Map Execution signal for script '{}' to node '{}'"
            ).format(job.name, uuid)
            logger.info(msg)
            body = serializer.dumps({
                "iname": self.iname, "clsname": self.clsname, "files": files
            })
            channel.basic_publish(exchange=MAP_E, routing_key=uuid, body=body)

