import quantiles
import numpy

# calling the default constructor is not possible yet
o = quantiles.Quantiles( [] )
o.setSortVec( [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10] )
q = o.getQuantiles()
for e in q:
    print "%.2f " % round( e, 2 ),
print
print "vec:  ", o.getSortVec()
print "elem: ", o.getNumberOfElements()
print "max:  ", o.getMax()
print "min:  ", o.getMin()
print "mean: ", o.getMean()
print "q nr: ", o.getQuantilNumber()
for e in o.getQuantiles():
    print "%.2f " % round( e, 2 ),
print "\n"


v = ([148, 165, 160, 160, 160, 162, 163, 163, 163, 164, 165, 165, 175, 165, 155, 166,
      166, 166, 157, 148, 168, 169, 170, 170, 171, 172, 172, 172, 162, 163, 165, 175],
     [158, 160, 160, 160, 160, 162, 163, 163, 163, 164, 165, 165, 165, 165, 165, 166,
      166, 166, 167, 168, 168, 169, 170, 170, 171, 172, 172, 172, 172, 173, 174, 175,
      175, 175, 178, 179, 180, 181, 181, 183, 183, 183, 185, 185, 186, 186, 186, 190,
      190, 192, 194, 194, 198, 201, 204],
     [128, 104, 190, 130, 154, 132, 154, 143, 153, 154, 155, 145, 195, 131, 131, 136,
      166, 103, 165, 154, 154, 159, 170, 140, 174, 154, 532, 532, 112, 132, 134, 145,
      142, 145, 143, 179, 150, 181, 423, 142, 134, 123, 155, 183, 136, 185, 146, 193,
      494, 142, 143, 143, 143]
    )

for d in v:
    o = quantiles.Quantiles( d )
    q = o.getQuantiles()
    for e in q:
        print "%.2f " % round( e, 2 ),
    print

    a = numpy.array( d )
    q = numpy.percentile( a, (0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100), interpolation="linear")
    for e in q:
        print "%.2f " % round( e, 2 ),
    print "\n"

