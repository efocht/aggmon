#

import os
import sys
import parsing
from tokens import *
from common_nonterms import *
import operators as ops


class Condition(parsing.Nonterm):
    "%nonterm"
    def reduce_and( self, condition1, _and, condition2 ):
        "%reduce Condition And Condition"
        self.value = ops.Binary( ops.Binary.AND, condition1.value, condition2.value )
    def reduce_comma( self, condition1, _comma, condition2 ):
        "%reduce Condition Comma Condition"
        self.value = ops.Binary( ops.Binary.AND, condition1.value, condition2.value )
    def reduce_or( self, condition1, _or, condition2 ):
        "%reduce Condition Or Condition"
        self.value = ops.Binary( ops.Binary.OR, condition1.value, condition2.value )
    def reduce_not( self, _not, condition ):
        "%reduce Not Condition"
        self.value = condition.value
        self.value.invert = not self.value.invert
    def reduce_parentheses( self, paren1, condition, paren2 ):
        "%reduce ParenOpen Condition ParenClose"
        self.value = condition.value
    def reduce_equal( self, attribute, equal, value ):
        "%reduce Identifier Equal Identifier"
        self.value = ops.StringEqual( attribute.value, value.value )
    def reduce_not_equal( self, attribute, equal, value ):
        "%reduce Identifier NotEqual Identifier"
        self.value = ops.StringEqual( attribute.value, value.value, invert=True )
    def reduce_match( self, attribute, tilde, pattern ):
        "%reduce Identifier Tilde Identifier"
        try:
            pattern = str(pattern.value)
        except Exception as e:
            raise parsing.SyntaxError( "cannot compile regexp: " + e.message )
        self.value = ops.RegexpMatch( attribute.value, pattern )
    def reduce_not_match( self, attribute, tilde, pattern ):
        "%reduce Identifier NotTilde Identifier"
        try:
            pattern = str(pattern.value)
        except Exception as e:
            raise parsing.SyntaxError( "cannot compile regexp: " + e.message )
        self.value = ops.RegexpMatch( attribute.value, pattern, invert=True )
    def reduce_contains( self, attribute, contains, value ):
        "%reduce Identifier Contains Identifier"
        self.value = ops.Contains( attribute.value, value.value )
    def reduce_not_contains( self, attribute, contains, value ):
        "%reduce Identifier NotContains Identifier"
        self.value = ops.Contains( attribute.value, value.value, invert=True )
    def __repr__( self ):
        return repr(self.value)

class KeyAttribute(parsing.Nonterm):
    "%nonterm"
    def reducePlain(self, element):
        "%reduce Identifier"
        self.element = element.value
    def __repr__( self ):
        return repr(self.element)

class FilterGroup(parsing.Nonterm):
    "%nonterm"
    def reducePlain(self, element, squareOpen, squareClose ):
        "%reduce Identifier SquareOpen SquareClose"
        self.element = element.value
        self.condition = None
    def reduceWithFilters(self, element, squareOpen, condition, squareClose):
        "%reduce Identifier SquareOpen Condition SquareClose"
        self.element = element.value
        self.condition = condition.value
    def __repr__( self ):
        if self.condition is not None:
            return repr(self.element) + "[" + repr(self.condition) + "]"
        else:
            return repr(self.element) + "[]"

class PathElement(parsing.Nonterm):
    "%nonterm"
    def reduceKeyAttribute(self, slash, key):
        "%reduce Slash KeyAttribute"
        self.name = key.element
        self.is_filter = False
    def reduceElement(self, slash, group):
        "%reduce Slash FilterGroup"
        self.name = group.element
        self.is_filter = True
        self.condition = group.condition
    def reduceSlash(self, slash):
        "%reduce Slash"
        self.name = None
        self.is_filter = False
    def __repr__( self ):
        if self.name is not None:
            result = repr(self.name)
        else:
            result = "/"
        if self.is_filter:
            if self.condition is not None:
                result += "[" + repr(self.condition) + "]"
            else:
                result += "[]"
        return result

class Path(parsing.Nonterm):
    "%nonterm"
    def reduceStart(self, element):
        "%reduce PathElement"
        self.value = [ element ]
    def reduceAppend(self, old_result, element):
        "%reduce Path PathElement"
        self.value = old_result.value
        self.value.append( element )
    def __repr__( self ):
        return repr(self.value)

class OIDList(parsing.Nonterm):
    "%nonterm"
    def reduceRID( self, rid ):
        "%reduce Identifier"
        self.value = [ rid ]
    def reduceAppend( self, old_result, _comma, rid ):
        "%reduce OIDList Comma Identifier"
        self.value = old_result.value
        self.value.append( rid )
    def __repr__( self ):
        return repr(self.value)

class QueryList(parsing.Nonterm):
    "%nonterm"
    def reduceQuery( self, query ):
        "%reduce Query"
        self.value = [ query ]
    def reduceAppend( self, old_result, _comma, query ):
        "%reduce QueryList Comma Query"
        self.value = old_result.value
        self.value.append( query )
    def __repr__( self ):
        return repr(self.value)

class Query(parsing.Nonterm):
    "%start"
    def reducePath(self, path):
        "%reduce Path"
        self.value = ( None, path )
    def reduceOIDs(self, squareOpen, identifiers, squareClose):
        "%reduce SquareOpen OIDList SquareClose"
        self.value = ( identifiers, None )
    def reduceRelativePath(self, squareOpen, identifiers, squareClose, path):
        "%reduce SquareOpen OIDList SquareClose Path"
        self.value = ( identifiers, path )
    def reduceNestedQuery(self, squareOpen, queries, squareClose):
        "%reduce SquareOpen QueryList SquareClose"
        self.value = ( queries, None )
    def reduceNestedQueryWithRelativePath(self, squareOpen, queries, squareClose, path):
        "%reduce SquareOpen QueryList SquareClose Path"
        self.value = ( queries, path )
    def __repr__( self ):
        if isinstance(self.value, OIDList):
            return '[' + repr(self.value) + ']'
        if isinstance(self.value, QueryList):
            return '[' + repr(self.value) + ']'
        else:
            return repr(self.value)


spec = parsing.Spec(sys.modules[__name__],
		    pickleFile=None,
		    skinny=False,
		    #logFile="query_parser.log",
		    graphFile=None,
		    verbose=False)
