#ifndef CORE_H
#define CORE_H

#include <pybind11/stl.h>
#include <sqlite3.h>


class EmapccHandle {
private:
    sqlite3* db;
    int B, M;

public:
    EmapccHandle(const std::string& db_file, int B, int M) : B(B), M(M) {
        if (sqlite3_open(db_file.c_str(), &db) != SQLITE_OK) {
            throw std::runtime_error("Failed to open database: " + std::string(sqlite3_errmsg(db)));
        }
    }

    ~EmapccHandle() {
        sqlite3_close(db);
    }

    void build_from_json
};





#endif