#
# Dummy class from the original queryengine built into LxDaemon.
# The functionality in the hierarchy_adapter client is a subset of this.
#
#
class QueryEngine:
    """
    Class that provides methods to access an object tree like those used in the hierarchy views.
    The access methods are named slightly similar to database nomenclature:
    - copy(obj_id)          : create a disconnected copy of an object and it's descendants
    - create(type, args)    : create a disconnected free floating happy object of class "type"
    - delete(obj_id)        : delete an object and all objects contained in it
    - describe(obj_id)      : return object type, attributes metadata and contains_types for the object specified by obj_id
    - insert(tgt_obj_id, obj_id) : add an object (complete? checked? validated?) into the target object specified as first arg
    - locate(query)         : returns array of obj_ids matching the "query" string (XPATH-like expression)
    - replace(old_obj_id, new_obj_id): replace an object by another one in the contains of the parent
    - select(query, selector) : retrieve "selector" (attributes values, etc...) for each object matched by the query string
    - update(obj_id, what)  : update an object's attributes
    - validate(obj_id)      : validate an object, check if it would throw (lx)asserts
    
    * Syntax of the query string:
    
      ** List of object IDs
      One possible format of the query string is a list of object IDs within '[', ']'.
      Examples:
        "[1466335]"                  -> matches object with ID '1466335'
        "[1466335,43225465,2233454]" -> matches the objects with the IDs 1466335, 43225465 or 2233454

      ** XPath-style expression
      The other format is to specify an XPath-like expression, to descend into the object tree.

      *** Path consisting of object key attributes
      The simplest method is to specify the key attributes of the objects separated by '/',
      descending the tree, starting from the root object.
      Examples:
        "/"                 -> matches root object
        "/universe"         -> matches universe group
        "/universe/sabi123" -> matches a child node named 'sabi123' of the group universe, e.g. a Host

      *** Selecting object types
      Instead of key attributes, the elements of the path may also be the names of object types/classes.
      If the class is a base class, derived types will also be matched.
      Examples:
        "/Group" -> matches all child of the Root objects which are of type 'Group' 
        "/universe/Host/Nid" -> matches all Nid objects of all Hosts in the 'universe' group
        "/universe/Host/Nic" -> matches all Nic objects of all Hosts in the 'universe' group,
                                and also all Nid objects, because Nid is a class derviced from Nic

      *** Arbitary depth selection
      The '//' operator can be used to search the tree downwards for matches at arbitrary depth.
      Note that depth level zero is also included, so the objects itself matched by the path
      before the '//' operator are also included in the search.
      Examples:
        "//Group"          -> matches all Groups, independent of the nesting level
        "//sabi123"        -> matches all Objects with key attribute 'sabi123', independent of their
                              location in the tree
        "/universe//Group" -> matches all Groups which are descendants of 'universe', including
                              the universe Group itself

      *** Wildcard object selection
      Using '*' as path element selects all children of objects selected by the preceding path expression
      "/universe/*"        -> matches all direct child objects of 'universe'
      "/universe/*/Adapter -> matches all objects of type Adapter (or types derived from Adapter)
                              which are a direct child of any direct child of 'universe'

      *** Parent selection operator
      The '..' path element selects the parent objects of the objects selected by the preceding path expression.
      Note that this may result in the same object appearing multiple times.
      Examples:
        "/universe/Group/sabi123/.." -> selects the Groups which contain an object named 'sabi123'
        "/universe/Host/../Host"     -> selects all Hosts in 'universe', but the result contains
                                        each Host n times if there are n Hosts in 'universe',
                                        so will return in total n^2 matching Hosts.

      *** Ancestor selection operator
      The '...' path element selects all ancestors of objects of the objects selected by 
      the preceding path expression at arbitrary level upwards in the tree.
      Equivalently to the '//' operator, depth level zero (the objects itself) is also included
      in the search.
      Note that this may result in the same object appearing multiple times in the result
      Examples:
        "/universe/Group/sabi123/..." -> selects all ancestors of the objects matched by 
                                         '/universe/Group/sabi123', including the 'sabi123' objects
                                         itself. So the 'sabi123' objects, the parent Groups of these
                                         objects, the 'universe' Group and the Root object are matched.

      *** Conditionals
      After any path element a condition expression may be provided to limit set the matched objects.
      The conditions can depend on the values of the attributes of the objects in question.
      The condition expression has to be enclosed in '[' ']'.

      **** Attribute exact string match
      The '=' operator specifies that the condition is fulfilled, when an attribute is equal to a
      given string.
      Examples:
        "/universe/Host[name=sabi123]" -> selects the Host in Group 'universe' with the name 'sabi123'.
                                          Note that other than '/universe/sabi123' it only matches objects
                                          of type 'Host'.
        "/universe/Host[name=sabi123]/Nic[dev=eth0]" -> matches the Nic object (which is a direct child of
                                                        the Host 'sabi123' matched in the previous example,
                                                        with the 'dev' attribute equal to the string 'eth0'.

      The inverse condition can be created using the '!=' (not equal) operator.
      Examples:
      "/universe/Group[name!=mdb]/Host" -> selects all Hosts in all direct child Groups of 'universe' 
                                           which do not have the name 'mdb'.

      **** Attribute regular expression match
      Equivalently the '~' and '!~' operators can be used to select objects with an attribute
      matching (respectively not matching for '!~') a gived regexp. All Python regexps are allowed.
      Examples:
        "/universe/Host[name~^sabi]" -> selects all Hosts in 'universe' with names beginning with 'sabi'
        "/universe/Host[name!~123$]" -> selects all Hosts in 'universe' with names not ending on '123'

      **** Not operator
      Specifying the '!' (not) operator before a condition expression inverts that condition.
      Examples:
        "/universe/Host[!name~^sabi]" -> selects all Hosts in 'universe' with names not beginning with 'sabi'
        "/universe/Host[!name!~123$]" -> selects all Hosts in 'universe' with names ending on '123'

      **** And, Or operators
      The operators ',' or '&' between two condition expressions have the meaning of an 'and' operation.
      The only difference between ',' and '&' is operator precedence. The operator '|' has the
      meaning of 'Or'.
      Examples:
        "/universe/Host/Nic[dev=eth0,ip=192.168.2.3]" -> selects all Nics of all Hosts in 'universe'
                                                         where the Nic's attribute
                                                        'dev' is equal 'eth0' and the attribute
                                                        'ip' is equal '192.168.2.3'.
        "/universe/Host[name~^sabi&name~123$]" -> selects all Hosts in Group 'universe'
                                                  which have a name beginning with 'sabi' and
                                                  ending on '123'.
        "/universe/Host[name~^sabi|name~123$]" -> selects all Hosts in Group 'universe'
                                                  which have a name beginning with 'sabi' or
                                                  ending on '123'.

      **** Parentheses
      The parentheses '(' and ')' can be used to group expressions to explicitly specify
      operator precedence.
      Examples:
        "//Group[(name~^uni|name~vm$)&gtype~^lxfs]" -> selects all Groups anywhere for which one of
                                                       the following conditions is true:
                                                       - have a name beginning with 'uni' and a gtype beginning with 'lxfs'
                                                       - have a name ending on 'vm'  and a gtype beginning with 'lxfs'
        "//Group[name~^uni|(name~vm$&gtype~^lxfs)]" -> selects all Groups anywhere for which one of
                                                       the following conditions is true:
                                                       - have a name beginning with 'uni'
                                                       - have a name ending on 'vm' and a gtype beginning with 'lxfs'

      **** Operator precedence
      The operators introduced above have precedences in the following order (beginning with the highest)
      - '!'
      - '&'
      - '|'
      - ','
      Examples:
        '[!a=x&b=y]'    is the same as '[!a=x&b=y]'
        '[a=x|b=y&c=z]' is the same as '[a=x|(b=y&c=z)]'
        '[a=x|b=y,c=z]' is the same as '[(a=x|b=y)&c=z]'

    * Selector string syntax

      Selectors are used to specify which attributes of the objects matched with the query string
      above, are of interest and will thus be included in the result. 
      Beside selecting attributes it also possible to select other objects and their attributes
      at locations specified relative to the objects matched by the query string.
      One more application of selectors is to call functions of the matched objects.

      ** List of attributes
      Basically a selector string is a list of attribute names separated by ',' and enclosed in '[', ']'
      Examples:
        "[name]"     -> selects only the 'name' attribute of the object
        "[name,_id]" -> selects both the 'name' and '_id' attributes of the object

      ** Selecting child objects by type
      Instead of attribute names it is also possible to specify class names to select
      direct child objects of a given type.
      Examples:
        "[name,_id,Host] -> selects both the 'name' and '_id' attributes of the object and all
                            direct child objects of type 'Host' and all their attributes

      ** Selecting child objects by key attribute
      Child objects may also be selected by the value of their key attribute instead of
      their type name.
      Examples:
        "[name,sabi123] -> selects the 'name' attribute of the object and the direct
                           child object with key attribute equal to 'sabi123' 
                           and all its attributes

      ** Wildcard, parent, ancestor object selector
      The wildcard operator '*', parent operator '..' and ancestor operator '...' may also be used
      to select objects. The meaning of these operators is as for the query string. Additionally
      there is the self operator '.' to specify the object itself. It can be used, e.g. to select
      all attributes of the matched objects additionally to some child objects.
      Examples:
        "[*]"   -> selects all direct child objects and  all their attributes
        "[.,*]" -> selects all attributes of the object and all direct child objects and their attributes
        "[..]"  -> selects the parent object and all its attributes
        "[Host,...]" -> selects all children of type 'Host', the object itself and all its
                        ancestors and all attributes of these objects

      ** Relative paths
      Paths relative to the matched object may be specified with the usual syntax, separating
      path elements by '/'.
      Examples:
        "[_id,Host/_id]" -> selects the '_id' attribute of the matched 'object' and all its
                            child objects of type 'Host', but only returns their '_id' attribute
        "[../name]"      -> return the name of the parent of the matched object
        "[../Host/name]" -> return the names of all 'Host' objects which are direct children 
                            of the parent of the matched object

      ** Nested selectors
      Identifiers refering to an object can be followed by a nested selector which operates relative
      the object. Selectors can be nested recursively multiple times. 
      Examples:
        "[name,Host[name,_id]]" -> select the 'name' attribute of the matched object, all its
                                   child objects of type 'Host', and returns their 
                                   ' name' and '_id' attributes.
        "[Host[Nid,Bmc]/dev]"   -> selects all child objects of type 'Nid' or 'Nic' of 
                                   all child objects of type 'Host' and return their 'dev' attribute

    * Lexer tokens and escaping syntax of query strings and selector strings

      The following tokens are recognized by the lexer as special and thus need to be escaped if
      they should appear inside identifiers (object names, attribute names, attribute values, regexps):
      - '[' ']' '(', ')'
      - '/'
      - ',' '&' '|' '!'
      - '=', '!=', '~', '!~'
      - '\'
      - single quotes and double quotes

      ** Escaping single characters

      The '\' escape character can be used to escape the next character following the '\'.
      It can be used on any character including itself. When used on one of the letters 'n' 'r' 't'
      it produces a newline, return or tab character.
      Example:
        "//Group[hpath=\/universe\/mdb]" -> matches the Group with hpath equal to the string '/universe/mdb'

      ** Protecting strings by quotes

      Both single quotes and double quotes can be used to protect character sequence from being 
      interpreted as special tokens. The difference between single quotes and double quotes is
      that within double quotes escaping by '\' is still interpreted as described above, while
      within single quotes a '\' is treated like any other character.
      Example:
        "//Group[hpath='/universe/mdb']" -> matches the Group with hpath equal to the string '/universe/mdb'
                                            (without the single quotes).

    """
    pass
