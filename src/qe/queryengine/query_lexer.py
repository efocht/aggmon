#

from plex import *
import parsing
import tokens as tk


__all__ = [ "QueryLexer" ]


def returnEscaped():
    return lambda lexer, text: tk.String( lexer, text[1:2] )

def returnText():
    return lambda lexer, text: tk.String( lexer, text )

def returnToken( tokenType ):
    return lambda lexer, text: tokenType( lexer )

def returnString( string ):
    return lambda lexer, text: tk.String( lexer, string )

def returnNumber():
    return lambda lexer, text: tk.Number( lexer, text )

def returnRunawayError( message ):
    return lambda lexer, text: parsing.SyntaxError( "Lexer error: runaway %s at position %d" % ( message, lexer.scope_begin_position ) )


def beginScope( scope ):
    def func( lexer, text ):
        lexer.scope_begin_position = lexer.position()
        lexer.begin( scope )
        return None
    return func

def quote_end():
    def func( lexer, text ):
        lexer.scope_begin_position = lexer.position()
        lexer.begin( "" )
        return tk.String( lexer, "" )
    return func

class QueryLexer(Scanner):

    lexicon = Lexicon([
        State("", [
            (Str("["),                  returnToken(tk.SquareOpen)),
            (Str("]"),                  returnToken(tk.SquareClose)),
            (Str("("),                  returnToken(tk.ParenOpen)),
            (Str(")"),                  returnToken(tk.ParenClose)),
            (Str(","),                  returnToken(tk.Comma)),
            (Str("/"),                  returnToken(tk.Slash)),
            (Str("&"),                  returnToken(tk.And)),
            (Str("|"),                  returnToken(tk.Or)),
            (Str("#"),                  returnToken(tk.Contains)),
            (Str("!#"),                 returnToken(tk.NotContains)),
            (Str("="),                  returnToken(tk.Equal)),
            (Str("!="),                 returnToken(tk.NotEqual)),
            (Str("!"),                  returnToken(tk.Not)),
            (Str("~"),                  returnToken(tk.Tilde)),
            (Str("!~"),                 returnToken(tk.NotTilde)),
            (Str(":"),                  returnToken(tk.Colon)),
            (Str("."),                  returnToken(tk.Dot)),
            (Str(".."),                 returnToken(tk.DoubleDot)),
            (Str("..."),                returnToken(tk.TripleDot)),
            (Str("*"),                  returnToken(tk.Asterisk)),
            (Str('"'),                  beginScope("DoubleQuote")),
            (Str("'"),                  beginScope("SingleQuote")),
            (Str('\\n'),                returnString("\n")),
            (Str('\\r'),                returnString("\r")),
            (Str('\\t'),                returnString("\t")),
            (Str('\\') + AnyBut('rnt'), returnEscaped()),
            (Rep1(Range('09')),         returnNumber()),
            (Rep1(AnyBut('[]().*/,"\'=\\~&|!#:')),returnText()),
        ]),
        State("DoubleQuote", [
            (Rep1(AnyBut('"\\')),       returnText()),
            (Str('\\n'),                returnString("\n")),
            (Str('\\r'),                returnString("\r")),
            (Str('\\t'),                returnString("\t")),
            (Str('\\') + AnyBut('rnt'), returnEscaped()),
            (Str('"'),                  quote_end()),
            (Eol,                       returnRunawayError("double quote")),
        ]),
        State("SingleQuote", [
            (Rep1(AnyBut("'")),         returnText()),
            (Str("'"),                  quote_end()),
            (Eol,                       returnRunawayError("single quote")),
        ]),
    ])

    def __init__(self, parser, _input):

        Scanner.__init__(self, QueryLexer.lexicon, _input, None)

        self.parser = parser

    def position( self ):
        return Scanner.position( self )[2]

    def parse( self ):
        while True:
            self.currentToken = self.read()[0]
            if isinstance( self.currentToken, parsing.SyntaxError ):
                raise self.currentToken
            try:
                if self.currentToken is None:
                    self.parser.eoi()
                    break
                self.parser.token( self.currentToken )
            except parsing.SyntaxError as e:
                raise parsing.SyntaxError( "Parser error: %s at character %d in input string" % (e, self.position()) )
        return self.parser._start[0]


