#########################################################################
#                                                                        #
#  Copyright (C) 2009-2013 NEC Deutschland GmbH, NEC HPC Europe.         #
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


__all__ = ["AttributeMD"]


class AttributeMD(dict):
    """
    Use AttributeMD class instances to describe attribute properties of ContainerObject.
    Parameters:
      type: attribute value type (string, int, list, ...)
      note: description string for the attribute, mainly for the GUI
      serialize: decides whether attribute will be serialized eg. to hg, fs, xml
      constant: if the attribute is constant, here is its value
      function: the attribute's value is obtained by invoking the function with the object as single argument
      function_result: how should the function result be used:
            "volatile" : result is volatile, call function each time (default)
            "once" : function result is constant for the lifetime of the daemon, call it just once
            "cache" : cache the result for certain time, call function if the last value has timed out.
            "cache_tree" : cached as long as the tree version of the root object doesn't change.
      cache_time : seconds to cache the function result (if cacheable)
      
    The mechanisms using these attributes are implemented in ContainerObject.
    """
    def __init__( self, _type, note, serialize=True, constant=None, function=None, function_result="volatile", cache_time=0 ):
        self["type"] = _type
        self["note"] = note
        assert isinstance( serialize, bool ), "attributemd serialize must be boolean!"
        self["serialize"] = serialize
        if constant is not None:
            self["constant"] = constant
        if function is not None:
            # for functions: don't serialize the attributes! Otherwise they will get back in the constructor.
            self["serialize"] = False
            assert callable( function ), "attributemd function %s is not callable" % str( type( function ) )
            self["function"] = function
            assert function_result in ["volatile", "once", "cache", "cache_tree"], \
                "attributemd function_result must be one of volatile, once, cache, cache_tree"
            self["function_result"] = function_result
            if function_result == "cache":
                assert isinstance( cache_time, int ), "attributemd cache_time must be an integer!"
                self["cache_time"] = cache_time
