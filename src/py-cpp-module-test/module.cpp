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

// C++ class
template <typename T>
class VClass {
    private:
        std::vector<T> l;
    public:
        void setList(const std::vector<T>& l) {
            this->l = l;
        }
        std::vector<T> getList() {
            return this->l;
        }
        void setPyList(const boost::python::list& l) {
            this->l = py_list_to_std_vec<T>(l);
        }
        boost::python::list getPyList() {
            return std_vec_to_py_list(l);
        }
        void dump() {
            for(auto &i : l) {
                std::cout << i << std::endl;
            }
        }
};

typedef VClass<double> DClass;
typedef std::vector<double> List;

// wrapper
#include <python2.7/Python.h>
#include <boost/python/module.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>

using namespace boost::python;

BOOST_PYTHON_MODULE(module)
{
    class_<List>("List")
        .def(vector_indexing_suite<List>())
        ;

    class_<DClass>("DClass")
        .def("getList", &DClass::getList)
        .def("setList", &DClass::setList)
        .def("getPyList", &DClass::getPyList)
        .def("setPyList", &DClass::setPyList)
        .def("dump", &DClass::dump)
        ;
}

