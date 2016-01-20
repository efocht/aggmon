##########################################################################
#                                                                        #
#  Copyright (C) 2009-2015 NEC Deutschland GmbH, NEC HPC Europe.         #
#                                                                        #
#  These coded instructions, statements, and computer programs  contain  #
#  unpublished  proprietary  information of NEC HPC Europe, and are      #
#  are protected by Federal copyright law.  They  may  not be disclosed  #
#  to  third  parties  or copied or duplicated in any form, in whole or  #
#  in part, without the prior written consent of NEC HPC Europe.         #
#                                                                        #
##########################################################################
#
# $Id$
#
import logging
import math
import re
import time
from copy import deepcopy


log = logging.getLogger(__name__)
STATE_ZERO = { "UNKNOWN": 0, "OK": 0, "WARNING": 0, "CRITICAL": 0 }
STATE_PRINT_ORDER = [ "CRITICAL", "WARNING", "OK", "UNKNOWN" ]
TRISTATE_MATCH = re.compile( "^(OK|WARNING|CRITICAL)[:]*" )


def tristate_aggregate( states ):
    counts = deepcopy( STATE_ZERO )
    for state in states:
        counts[state] += 1
    worst = None
    out = []
    for state in STATE_PRINT_ORDER:
        if counts[state] > 0:
            if worst is None:
                worst = state
            out.append( "%s=%d" % (state, counts[state]) ) 
    return worst, " ".join( out )


class Quantiles(object):
    def __init__( self, values, num_quantiles=10 ):
        assert num_quantiles > 1, "num_quantiles (%s) must be larger than 1!" % str(num_quantiles)
        self.num_quantiles = num_quantiles
        self.sorted_vec = sorted( values )
        self.q = [0] * (self.num_quantiles + 1)
        self._mean = None
        self.update()

    def update( self, append=None ):
        if append is not None:
            assert isinstance( append, list ), "To quantiles you must append a list!"
            self.sorted_vec = sorted( self.sorted_vec + append )
        num_elems = len(self.sorted_vec)
        is_float = False
        if isinstance( self.sorted_vec[0], float ):
            is_float = True
        if num_elems > 1:
            factor = float(len(self.sorted_vec)) / float(self.num_quantiles)
            self.q[0] = self.sorted_vec[0]                       # min
            self.q[self.num_quantiles] = self.sorted_vec[-1]     # max
            for i in xrange(1, self.num_quantiles):
                idx = int(math.floor(i*factor))
                if is_float:
                    rest = float((i * factor) - idx)
                else:
                    rest = int((i * factor) - idx)
                if idx == 0:
                    self.q[i] = self.sorted_vec[0]
                else:
                    # interpolate value
                    prev_val = self.sorted_vec[idx - 1]
                    self.q[i] = prev_val + rest * (self.sorted_vec[idx] - prev_val)
            self._mean = sum( self.sorted_vec ) / float( num_elems )
        elif num_elems == 1:
            for i in xrange(self.num_quantiles + 1):
                self.q[i] = self.sorted_vec[0]
            self._mean = self.sorted_vec[0]
        else:
            # no elements
            self._mean = 0

    def quantiles( self ):
        return self.q

    def mean( self ):
        return self._mean


from aggmon.quantiles import Quantiles as CQuantiles
def aggregate( agg, values ):
    value = None
    if agg == "worst":
        states = []
        for value in values:
            m = TRISTATE_MATCH.match( value.upper() )
            if m is not None:
                states.append( m.group(1) )
        result, output = tristate_aggregate( states )
        # does it make sense to set the value? Could use it for caching.
        value = result + ":" + output
    elif agg == "quant10":
        v = []
        for element in values:
            if element is None:
                continue
            v.append( element )
        log.debug("quant10: v=%r" % v)
        q = CQuantiles( v )
        value = [q.quantiles(), q.mean()]
    else:
        #
        # and now the RRD metrics
        #
        n = 0
        _sum = 0.0
        _min = 1e+32
        _max = -_min
        for value in values:
            if value is None:
                continue
            n += 1
            if agg in ("sum", "avg"):
                _sum += value
            elif agg == "max":
                if value > _max:
                    _max = value
            elif agg == "min":
                if value < _min:
                    _min = value
        result = {}
        value = None
        if agg == "sum":
            value = _sum
        elif agg == "max":
            value = _max
        elif agg == "min":
            value = _min
        elif agg == "avg":
            if n > 0:
                value = _sum/float(n)
    return value

