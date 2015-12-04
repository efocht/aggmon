import module

list_in = [3.141, 2.718]
qobj = module.DClass()
qobj.setPyList( list_in )
list_in.append( 9.81 )
del list_in
qobj.dump()
list_out = qobj.getPyList()
print type( list_out )
print list_out

# the following will generate an exception
#list_in = module.List()
#list_in.append( "error" )

