class HierarchyAdapter(object):
    """
    Hierarchy Adapter "abstract" class.
    Not all methods need to be implemented. Minimum is do_select().
    """
    def op_create( self, typename, content ):
        raise NotImplementedError("Method called in abstract class!")

    def op_delete( self, obj ):
        raise NotImplementedError("Method called in abstract class!")

    def op_describe( self, key ):
        raise NotImplementedError("Method called in abstract class!")

    def op_insert( self, parent, obj ):
        raise NotImplementedError("Method called in abstract class!")

    def op_update( self, obj, content ):
        raise NotImplementedError("Method called in abstract class!")

    def op_locate( self, query ):
        raise NotImplementedError("Method called in abstract class!")

    def op_select( self, query, selector ):
        raise NotImplementedError("Method called in abstract class!")
