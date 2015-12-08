import module

list_in = [3.141, 2.718]
qobj = module.DClass()
qobj.setPyList( list_in )
del list_in
qobj.dump()
list_out = qobj.getPyList()
print type( list_out )
print list_out

# the following will generate an exception
#list_in = module.List()
#list_in.append( "error" )

list_in = module.List()
list_in.append( 3.141 )
list_in.append( 2.718 )
qobj.setList( list_in )
del list_in
qobj.dump()
list_out = qobj.getList()
print type( list_out )
for e in list_out:
    print e

