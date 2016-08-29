#

class RegexpMatch:

    def __init__( self, attribute, pattern, invert = False ):
        self.attribute = attribute
        self.pattern = pattern
        self.invert = invert
    def test( self, engine, obj ):
        _key, attr_value = engine.get_attribute( obj, self.attribute )
        return self.invert ^ ( self.pattern.search( str( attr_value ) ) is not None)
    def __repr__( self ):
        if self.invert:
            return repr(self.attribute) + "!~" + repr(self.pattern)
        else:
            return repr(self.attribute) + "~" + repr(self.pattern)

class StringEqual:

    def __init__( self, attribute, match, invert = False ):
        self.attribute = attribute
        self.match = match
        self.invert = invert
    def test( self, engine, obj ):
        _key, attr_value = engine.get_attribute( obj, self.attribute )
        return self.invert ^ ( str( attr_value ) == self.match )
    def __repr__( self ):
        if self.invert:
            return repr(self.attribute) + "!=" + repr(self.match)
        else:
            return repr(self.attribute) + "=" + repr(self.match)

class Contains:

    def __init__( self, attribute, element, invert = False ):
        self.attribute = attribute
        self.element = element
        self.invert = invert
    def test( self, engine, obj ):
        _key, attr_value = engine.get_attribute( obj, self.attribute )
        if not isinstance( attr_value, list ):
            return self.invert
        for element in attr_value:
            if str(element) == self.element:
                return not self.invert
        return self.invert
    def __repr__( self ):
        if self.invert:
            return repr(self.attribute) + "!#" + repr(self.element)
        else:
            return repr(self.attribute) + "#" + repr(self.element)

class Binary:

    AND = 1
    OR  = 2

    def __init__( self, operator, condition1, condition2, invert = False ):
        self.operator = operator
        self.condition1 = condition1
        self.condition2 = condition2
        self.invert = invert

    def test( self, engine, obj ):
        if self.operator == Binary.AND:
            return self.invert ^ ( self.condition1.test( engine, obj ) and self.condition2.test( engine, obj ) )
        else:
            return self.invert ^ ( self.condition1.test( engine, obj ) or self.condition2.test( engine, obj ) )
    def __repr__( self ):
        if self.invert:
            result = "!"
        else:
            result = ""
        if self.operator == Binary.AND:
            result += "(" + repr(self.condition1) + "&" + repr(self.condition2) + ")"
        else:
            result += "(" + repr(self.condition1) + "|" + repr(self.condition2) + ")"
        return result
