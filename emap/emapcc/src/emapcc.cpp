#include <pybind11/pybind11.h>

#include "db.h"


PYBIND11_MODULE(emapcc, mod) {
    mod.def("build_from_json", &emapcc::db::build_from_json, "A C++ implementation of build_from_json()");
}