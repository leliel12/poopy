#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import ampoopq

poop =  ampoopq.AMPoopQ(sys.argv[1], path=sys.argv[2], nodeid=sys.argv[3])


@poop.mapper
def my_map(key, value, context):
    pass



if __name__ == "__main__":
    poop.cliexe(sys.argv[4], sys.argv[5:])


