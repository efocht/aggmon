#

import parsing


class PrecedenceNot(parsing.Precedence):
    "%fail"

class PrecedenceAnd(parsing.Precedence):
    "%left <PrecedenceNot"

class PrecedenceOr(parsing.Precedence):
    "%left <PrecedenceAnd"

class PrecedenceComma(parsing.Precedence):
    "%left <PrecedenceOr"

class Token(parsing.Token):
    def __init__( self, lexer ):
        parsing.Token.__init__(self, lexer.parser)
        self.position = lexer.position()

class SquareOpen(Token):
    "%token"
    def __repr__( self ):
        return "["

class SquareClose(Token):
    "%token"
    def __repr__( self ):
        return "]"

class ParenOpen(Token):
    "%token"
    def __repr__( self ):
        return "("

class ParenClose(Token):
    "%token"
    def __repr__( self ):
        return ")"

class Slash(Token):
    "%token"
    def __repr__( self ):
        return "/"

class Comma(Token):
    "%token [PrecedenceComma]"
    def __repr__( self ):
        return ","

class Equal(Token):
    "%token"
    def __repr__( self ):
        return "="

class NotEqual(Token):
    "%token"
    def __repr__( self ):
        return "!="

class Not(Token):
    "%token [PrecedenceNot]"
    def __repr__( self ):
        return "!"

class Asterisk(Token):
    "%token"
    def __repr__( self ):
        return "*"

class Tilde(Token):
    "%token"
    def __repr__( self ):
        return "~"

class NotTilde(Token):
    "%token"
    def __repr__( self ):
        return "!~"

class And(Token):
    "%token [PrecedenceAnd]"
    def __repr__( self ):
        return "&"

class Or(Token):
    "%token [PrecedenceOr]"
    def __repr__( self ):
        return "|"

class Contains(Token):
    "%token"
    def __repr__( self ):
        return "#"

class NotContains(Token):
    "%token"
    def __repr__( self ):
        return "!#"

class Colon(Token):
    "%token"
    def __repr__( self ):
        return ":"

class Dot(Token):
    "%token"
    def __repr__( self ):
        return "."

class DoubleDot(Token):
    "%token"
    def __repr__( self ):
        return ".."

class TripleDot(Token):
    "%token"
    def __repr__( self ):
        return "..."

class String(Token):
    "%token"
    def __init__( self, lexer, text ):
        Token.__init__( self, lexer )
        self.text = text
    def __repr__( self ):
        return "string("+repr(self.text)+")"

class Number(Token):
    "%token"
    def __init__( self, lexer, text ):
        Token.__init__( self, lexer )
        self.text = text
    def __repr__( self ):
        return "string("+repr(self.text)+")"
