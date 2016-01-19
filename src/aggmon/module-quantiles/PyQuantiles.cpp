#include <iostream>
#include <vector>
#include <boost/python/list.hpp>

// convert std::vector to Python list
template <typename T>
boost::python::list std_vec_to_py_list(const std::vector<T>& v) {
    boost::python::list l;
    for (unsigned long i = 0; i < v.size(); ++i)
        l.append(v[i]);
    return l;
}

// convert Python list to std::vector
template <typename T>
std::vector<T> py_list_to_std_vec(const boost::python::list& l) {
    std::vector<T> v(len(l));
    for (unsigned long i = 0; i < v.size(); ++i)
        v[i] = boost::python::extract<T>(l[i]);
    return v;
}

// Quantiles wrapper
// TODO: calling the default constructor is not possible yet
#include "Quantiles.h"
#include "MathUtils.h"
class PyQuantiles : public Quantiles {
    public:
        PyQuantiles(const boost::python::list& l, long n) : Quantiles(py_list_to_std_vec<mathType>(l), (unsigned int)n) {;}
        PyQuantiles(const boost::python::list& l) : Quantiles(py_list_to_std_vec<mathType>(l), NUMBER_QUANTILES) {;}

        void setSortVecByPyList(const boost::python::list& l) {
            std::vector<mathType> v = py_list_to_std_vec<mathType>(l);
            setSortVec(v);
               }

        boost::python::list getSortVecAsPyList() const {
            return std_vec_to_py_list<mathType>(getSortVec());
        }

        long getNumberOfElementsAsLong() {
            return (long)getNumberOfElements();
        }

        void setQuantilNumberByLong(long k) {
            setQuantilNumber((long)k);
        }

        long getQuantilNumberAsLong() const {
            return (long)getQuantilNumber();
        }

        boost::python::list getQuantilsAsPyList() {
            return std_vec_to_py_list<mathType>(getQuantils());
        }
};

// BOOST wrapper
#include <Python.h>
#include <boost/python/module.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>

using namespace boost::python;

BOOST_PYTHON_MODULE(quantiles)
{
    class_<PyQuantiles>("Quantiles", init<const boost::python::list&, long>())
        .def(init<const boost::python::list&>())
        .def("setSortVec", &PyQuantiles::setSortVecByPyList)
        .def("getSortVec", &PyQuantiles::getSortVecAsPyList)
        .def("getNumberOfElements", &PyQuantiles::getNumberOfElementsAsLong)
        .def("max", &Quantiles::getMax)
        .def("getMax", &Quantiles::getMax)
        .def("min", &Quantiles::getMin)
        .def("getMin", &Quantiles::getMin)
        .def("mean", &Quantiles::getMean)
        .def("getMean", &Quantiles::getMean)
        .def("setQuantilNumber", &PyQuantiles::setQuantilNumberByLong)
        .def("getQuantilNumber", &PyQuantiles::getQuantilNumberAsLong)
        .def("quantiles", &PyQuantiles::getQuantilsAsPyList)
        .def("getQuantiles", &PyQuantiles::getQuantilsAsPyList)
        ;
}

