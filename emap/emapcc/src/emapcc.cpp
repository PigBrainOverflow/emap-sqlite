#include <pybind11/pybind11.h>

#include "db.h"
#include "core.h"


PYBIND11_MODULE(emapcc, mod) {
    pybind11::class_<emapcc::EmapccHandle>(mod, "EmapccHandle").def(pybind11::init<const std::string&, int, int>());
    mod.def("build_from_json", &emapcc::db::build_from_json, "A C++ implementation of build_from_json()");
}