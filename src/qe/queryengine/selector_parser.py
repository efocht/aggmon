#

import os
import sys
import parsing
from tokens import *
from common_nonterms import *

class FunctionCall:
    def __init__(self, path, arguments):
        self.path = path
        self.arguments = arguments
    def __repr__( self ):
        return repr(self.path) + "(" + ",".join( repr(element) for element in self.arguments ) + ")"

class Selector(parsing.Nonterm):
    "%nonterm"
    def reduceAttribute(self, identifier):
        "%reduce Identifier"
        self.name = identifier.value
        self.arguments = None
    def reduceFunctionCall(self, identifier, parenOpen, arguments, parenClose):
        "%reduce Identifier ParenOpen IdentifierList ParenClose"
        self.name = identifier.value
        self.arguments = arguments.value
    def reduceFunctionCall2(self, identifier, parenOpen, parenClose):
        "%reduce Identifier ParenOpen ParenClose"
        self.name = identifier.value
        self.arguments = []
    def __repr__( self ):
        result = repr(self.name)
        if self.arguments is not None:
            result += '(' + ','.join( [ repr(arg) for arg in self.arguments ] ) + ')'
        return result


class SelectorList(parsing.Nonterm):
    "%nonterm"
    def reduceStart(self, sel):
        "%reduce Selector"
        self.value = [ sel ]
    def reduceAppend(self, oldList, comma, sel):
        "%reduce SelectorList Comma Selector"
        self.value = oldList.value
        self.value.append( sel )
    def __repr__( self ):
        return repr(self.value)

class Result(parsing.Nonterm):
    "%start"
    def reduce(self, squareOpen, selector_list, squareClose):
        "%reduce SquareOpen SelectorList SquareClose"
        self.value = selector_list.value
    def __repr__( self ):
        return repr(self.value)

spec = parsing.Spec(sys.modules[__name__],
		    pickleFile=None,
		    skinny=False,
#		    logFile="selector_parser.log",
		    graphFile=None,
		    verbose=False)
