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

from . import connection, conf, serializer, script


#==============================================================================
# CONSTANTS
#==============================================================================

MAP_E = "map_exchange"

logger = conf.getLogger("Map")


#==============================================================================
# CLASS
#==============================================================================

class MapSubscriber(multiprocessing.Process):

    def __init__(self, conn, conf, *args, **kwargs):
        super(MapSubscriber, self).__init__(*args, **kwargs)
        self.conn = conn
        self.lconf = conf

    # callback
    def _callback(self, ch, method, properties, body):
        logger.info("Map Data recived!")
        data = serializer.loads(body)
        iname = data["iname"]
        clsname = data["clsname"]
        inamepath = os.path.join(self.lconf.SCRIPTS, iname)
        instance = script.cls_from_path(inamepath, clsname)()
        job = script.Job(instance, clsname, iname)

        logger.info("Preparing for run map for job '{}'".format(job.name))



    def run(self):
        conn = connection.PoopyConnection(self.conn)
        conn.exchange_consume(MAP_E, self._callback, self.lconf.UUID)


class MapPublisher(multiprocessing.Process):

    def __init__(self, conn, conf, job, uuids, *args, **kwargs):
        super(MapPublisher, self).__init__(*args, **kwargs)
        self.conn = conn
        self.lconf = conf
        self.job = job
        self.uuids = uuids

    def run(self):
        conn = connection.PoopyConnection(self.conn)
        channel = conn.channel()
        channel.exchange_declare(exchange=MAP_E, type='fanout')
        for uuid in self.uuids:
            msg = (
                "Map Execution signal for script '{}' sended to node '{}'"
            ).format(self.job.name, uuid)
            logger.info(msg)
            body = serializer.dumps({
                "iname": self.job.iname, "clsname": self.job.clsname}
            )
            channel.basic_publish(exchange=MAP_E, routing_key=uuid, body=body)

