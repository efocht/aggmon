#

import parsing


class IdentifierPart(parsing.Nonterm):
    "%nonterm"
    def reduceString(self, string):
        "%reduce String"
        self.value = string.text
    def reduceNumber(self, number):
        "%reduce Number"
        self.value = number.text
    def reduceAsterisk(self, token):
        "%reduce Asterisk"
        self.value = token
    def reduceDot(self, token):
        "%reduce Dot"
        self.value = token
    def reduceDoubleDot(self, token):
        "%reduce DoubleDot"
        self.value = token
    def reduceTripleDot(self, token):
        "%reduce TripleDot"
        self.value = token
    def __repr__( self ):
        return repr(self.value)

class Identifier(parsing.Nonterm):
    "%nonterm"
    def reduceAppend(self, oldIdentifier, append):
        "%reduce Identifier IdentifierPart"
        if isinstance(oldIdentifier.value, basestring):
            self.value = oldIdentifier.value
        else:
            self.value = str(oldIdentifier.value)
        if isinstance(append.value, basestring):
            self.value += append.value
        else:
            self.value += str(append.value)

    def reduceIdentifier(self, id):
        "%reduce IdentifierPart"
        self.value = id.value

    def __repr__( self ):
        return repr(self.value)

class IdentifierList(parsing.Nonterm):
    "%nonterm"
    def reduceStart(self, id):
        "%reduce Identifier"
        self.value = [ id ]
    def reduceAppend(self, old_result, comma, id):
        "%reduce IdentifierList Comma Identifier"
        self.value = old_result.value
        self.value.append( id )
    def __repr__( self ):
        return repr(self.value)
