#!/usr/bin/env python
# -*- coding: utf-8 -*-

# "THE WISKEY-WARE LICENSE":
# <jbc.develop@gmail.com> wrote this file. As long as you retain this notice
# you can do whatever you want with this stuff. If we meet some day, and you
# think this stuff is worth it, you can buy me a WISKEY in return Juan BC


#==============================================================================
# DOCS
#==============================================================================

"""Connection base class

"""


#==============================================================================
# IMPORTS
#==============================================================================

import pika


#==============================================================================
# CLASS
#==============================================================================

class AMPoopQConnection(pika.BlockingConnection):

    def exchange_consume(self, exchange, callback):
        channel = self.channel()
        channel.exchange_declare(exchange=exchange, type='fanout')
        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=exchange, queue=queue_name)
        channel.basic_consume(callback, queue=queue_name, no_ack=False)
