#

import sys
from StringIO import StringIO
import parsing
from query_lexer import *
import query_parser as qp
import selector_parser as sp


if len( sys.argv ) > 1:
    stream = StringIO( sys.argv[1] )
else:
    stream = StringIO( "//Host[name=\\tx a,gtype=\"MDB\\\"\"]/../abc//Nid/[_type=Host]/...[foo=xx]" )

parser = parsing.Lr( qp.spec )
lexer = QueryLexer( parser, stream )

try:
    tree = lexer.parse( )
except parsing.SyntaxError as e:
    print e
    sys.exit(1)

print tree


if len( sys.argv ) > 2:
    stream = StringIO( sys.argv[2] )

    parser = parsing.Lr( sp.spec )
    lexer = QueryLexer( parser, stream )

    try:
        tree = lexer.parse( )
    except parsing.SyntaxError as e:
        print e
        sys.exit(1)

    print tree
