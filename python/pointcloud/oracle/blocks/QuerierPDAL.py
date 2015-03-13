#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time, math, subprocess
from itertools import groupby, count
from pointcloud import dbops, utils
from pointcloud.oracle.AbstractQuerier import AbstractQuerier

class QuerierPDAL(AbstractQuerier):        
    def query(self, queryId, iterationId, queriesParameters):
        return