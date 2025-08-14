#ifndef DB_H
#define DB_H

#include <pybind11/pytypes.h>
#include <pybind11/stl.h>
#include <sqlite3.h>


namespace emapcc::db {

#include <vector>
#include <stdexcept>

std::vector<int> _get_bits_of_wirevec(sqlite3* db, int id) {
    sqlite3_stmt* stmt;
    const char* sql = "SELECT wire FROM wirevec_members WHERE wirevec = ? ORDER BY idx";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK)
        throw std::runtime_error("Failed to prepare statement: " + std::string(sqlite3_errmsg(db)));

    sqlite3_bind_int(stmt, 1, id);

    std::vector<int> bits;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        bits.push_back(sqlite3_column_int(stmt, 0));
    }

    sqlite3_finalize(stmt);
    return bits;
}

int _create_or_lookup_wirevec(sqlite3* db, const std::vector<int>& bits, int B, int M) {
    // compute hash
    int h = 0;
    for (int bit : bits) {
        h = (h * B + bit) % M;
    }

    // lookup existing wirevec by hash
    sqlite3_stmt* lookup_stmt;
    const char* lookup_sql = "SELECT id FROM wirevecs WHERE hash = ?";
    if (sqlite3_prepare_v2(db, lookup_sql, -1, &lookup_stmt, nullptr) != SQLITE_OK)
        throw std::runtime_error("Failed to prepare lookup statement: " + std::string(sqlite3_errmsg(db)));

    sqlite3_bind_int(lookup_stmt, 1, h);
    while (sqlite3_step(lookup_stmt) == SQLITE_ROW) {
        int id = sqlite3_column_int(lookup_stmt, 0);
        if (_get_bits_of_wirevec(db, id) == bits) {
            sqlite3_finalize(lookup_stmt);
            return id;
        }
    }
    sqlite3_finalize(lookup_stmt);

    char* errmsg;
    if (sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, &errmsg) != SQLITE_OK)
        throw std::runtime_error("Failed to begin transaction: " + std::string(errmsg));

    // insert new wirevec
    sqlite3_stmt* insert_wirevec;
    const char* insert_sql = "INSERT INTO wirevecs (hash) VALUES (?)";
    if (sqlite3_prepare_v2(db, insert_sql, -1, &insert_wirevec, nullptr) != SQLITE_OK)
        throw std::runtime_error("Failed to prepare insert statement: " + std::string(sqlite3_errmsg(db)));

    sqlite3_bind_int(insert_wirevec, 1, h);
    if (sqlite3_step(insert_wirevec) != SQLITE_DONE) {
        sqlite3_finalize(insert_wirevec);
        throw std::runtime_error("Failed to insert wirevec: " + std::string(sqlite3_errmsg(db)));
    }
    sqlite3_finalize(insert_wirevec);

    int new_id = static_cast<int>(sqlite3_last_insert_rowid(db));

    // insert bit members
    sqlite3_stmt* insert_member;
    const char* member_sql = "INSERT INTO wirevec_members (wirevec, wire, idx) VALUES (?, ?, ?)";
    if (sqlite3_prepare_v2(db, member_sql, -1, &insert_member, nullptr) != SQLITE_OK)
        throw std::runtime_error("Failed to prepare insert member statement: " + std::string(sqlite3_errmsg(db)));

    for (size_t i = 0; i < bits.size(); ++i) {
        sqlite3_bind_int(insert_member, 1, new_id);
        sqlite3_bind_int(insert_member, 2, bits[i]);
        sqlite3_bind_int(insert_member, 3, static_cast<int>(i));
        if (sqlite3_step(insert_member) != SQLITE_DONE) {
            sqlite3_finalize(insert_member);
            throw std::runtime_error("Failed to insert wirevec member: " + std::string(sqlite3_errmsg(db)));
        }
        sqlite3_reset(insert_member);  // reuse statement
    }
    sqlite3_finalize(insert_member);

    if (sqlite3_exec(db, "COMMIT;", nullptr, nullptr, &errmsg) != SQLITE_OK)
        throw std::runtime_error("Failed to commit transaction: " + std::string(errmsg));

    return new_id;
}


void _add_aby_cell(sqlite3* db, const std::string& type, const std::vector<int>& a, const std::vector<int>& b, const std::vector<int>& y, int B, int M) {
    auto wva = _create_or_lookup_wirevec(db, a, B, M);
    auto wvb = _create_or_lookup_wirevec(db, b, B, M);
    auto wvy = _create_or_lookup_wirevec(db, y, B, M);
    sqlite3_stmt* stmt;
    const char* sql = "INSERT INTO aby_cells (type, a, b, y) VALUES (?, ?, ?, ?)";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare insert aby cell statement: " + std::string(sqlite3_errmsg(db)));
    }
    sqlite3_bind_text(stmt, 1, type.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, wva);
    sqlite3_bind_int(stmt, 3, wvb);
    sqlite3_bind_int(stmt, 4, wvy);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        sqlite3_finalize(stmt);
        throw std::runtime_error("Failed to insert aby cell: " + std::string(sqlite3_errmsg(db)));
    }
    sqlite3_finalize(stmt);
}

void _add_dff(sqlite3* db, const std::vector<int>& d, const std::vector<int>& q, int B, int M) {
    auto wvd = _create_or_lookup_wirevec(db, d, B, M);
    auto wvq = _create_or_lookup_wirevec(db, q, B, M);
    sqlite3_stmt* stmt;
    const char* sql = "INSERT INTO dffs (d, q) VALUES (?, ?)";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare insert dff statement: " + std::string(sqlite3_errmsg(db)));
    }
    sqlite3_bind_int(stmt, 1, wvd);
    sqlite3_bind_int(stmt, 2, wvq);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        sqlite3_finalize(stmt);
        throw std::runtime_error("Failed to insert dff: " + std::string(sqlite3_errmsg(db)));
    }
    sqlite3_finalize(stmt);
}

void _add_absy_cell(sqlite3* db, const std::string& type, const std::vector<int>& a, const std::vector<int>& b, const std::vector<int>& s, const std::vector<int>& y, int B, int M) {
    auto wva = _create_or_lookup_wirevec(db, a, B, M);
    auto wvb = _create_or_lookup_wirevec(db, b, B, M);
    auto wvs = _create_or_lookup_wirevec(db, s, B, M);
    auto wvy = _create_or_lookup_wirevec(db, y, B, M);
    sqlite3_stmt* stmt;
    const char* sql = "INSERT INTO absy_cells (type, a, b, s, y) VALUES (?, ?, ?, ?, ?)";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare insert absy cell statement: " + std::string(sqlite3_errmsg(db)));
    }
    sqlite3_bind_text(stmt, 1, type.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, wva);
    sqlite3_bind_int(stmt, 3, wvb);
    sqlite3_bind_int(stmt, 4, wvs);
    sqlite3_bind_int(stmt, 5, wvy);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        sqlite3_finalize(stmt);
        throw std::runtime_error("Failed to insert absy cell: " + std::string(sqlite3_errmsg(db)));
    }
    sqlite3_finalize(stmt);
}

void _add_ay_cell(sqlite3* db, const std::string& type, const std::vector<int>& a, const std::vector<int>& y, int B, int M) {
    auto wva = _create_or_lookup_wirevec(db, a, B, M);
    auto wvy = _create_or_lookup_wirevec(db, y, B, M);
    sqlite3_stmt* stmt;
    const char* sql = "INSERT INTO ay_cells (type, a, y) VALUES (?, ?, ?)";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare insert ay cell statement: " + std::string(sqlite3_errmsg(db)));
    }
    sqlite3_bind_text(stmt, 1, type.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, wva);
    sqlite3_bind_int(stmt, 3, wvy);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        sqlite3_finalize(stmt);
        throw std::runtime_error("Failed to insert ay cell: " + std::string(sqlite3_errmsg(db)));
    }
    sqlite3_finalize(stmt);
}

int _bit_to_int(const pybind11::handle& bit) {
    if (pybind11::isinstance<pybind11::int_>(bit)) {
        return bit.cast<int>();
    }
    else {
        auto bit_str = bit.cast<std::string>();
        if (bit_str[0] == 'x') {
            return -1;
        }
        else if (bit_str[0] == '0') {
            return 0;
        }
        else {
            return 1;
        }
    }
}


std::tuple<int, int> build_from_json(
    const std::string& db_file,
    const pybind11::dict& mod,
    const std::string& clk_name,
    int B, int M    // rolling hash parameters
) {
    // open sqlite3 connection
    sqlite3* db;
    if (sqlite3_open(db_file.c_str(), &db) != SQLITE_OK) {
        throw std::runtime_error("Failed to open database: " + std::string(sqlite3_errmsg(db)));
    }

    const auto& ports = mod["ports"].cast<std::map<std::string, pybind11::dict>>();
    const auto& cells = mod["cells"].cast<std::map<std::string, pybind11::dict>>();
    int clk = 0;

    // build inputs & outputs
    for (const auto& [name, port] : ports) {
        const auto& direction = port["direction"].cast<std::string>();
        const auto& raw_bits = port["bits"].cast<pybind11::list>();
        std::vector<int> bits;
        for (const auto& raw_bit : raw_bits) {
            bits.push_back(_bit_to_int(raw_bit));
        }
        if (direction == "input") {
            if (name == clk_name) {
                if (bits.size() != 1) {
                    throw std::runtime_error("Clock port must have exactly one bit");
                }
                clk = bits[0];
            }
            auto id = _create_or_lookup_wirevec(db, bits, B, M);
            sqlite3_stmt* stmt;
            const char* sql = "INSERT INTO from_inputs (source, name) VALUES (?, ?)";
            if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
                throw std::runtime_error("Failed to prepare from_inputs statement: " + std::string(sqlite3_errmsg(db)));
            }
            sqlite3_bind_int(stmt, 1, id);
            sqlite3_bind_text(stmt, 2, name.c_str(), -1, SQLITE_STATIC);
            if (sqlite3_step(stmt) != SQLITE_DONE) {
                sqlite3_finalize(stmt);
                throw std::runtime_error("Failed to insert from_inputs: " + std::string(sqlite3_errmsg(db)));
            }
            sqlite3_finalize(stmt);
        }
        else if (direction == "output") {
            auto id = _create_or_lookup_wirevec(db, bits, B, M);
            sqlite3_stmt* stmt;
            const char* sql = "INSERT INTO as_outputs (sink, name) VALUES (?, ?)";
            if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
                throw std::runtime_error("Failed to prepare as_outputs statement: " + std::string(sqlite3_errmsg(db)));
            }
            sqlite3_bind_int(stmt, 1, id);
            sqlite3_bind_text(stmt, 2, name.c_str(), -1, SQLITE_STATIC);
            if (sqlite3_step(stmt) != SQLITE_DONE) {
                sqlite3_finalize(stmt);
                throw std::runtime_error("Failed to insert as_outputs: " + std::string(sqlite3_errmsg(db)));
            }
            sqlite3_finalize(stmt);
        }
        else {
            throw std::runtime_error("Unknown port direction: " + direction);
        }
    }

    // build cells
    char* errmsg;

    int cell_count = 0;
    for (const auto& [name, cell] : cells) {
        if (cell_count % 1000 == 0) {
            std::cout << "Processing cell " << cell_count << "/" << cells.size() << ": " << name << "\n";
        }
        ++cell_count;

        std::string type = cell["type"].cast<std::string>();
        auto params = cell["parameters"].cast<pybind11::dict>();
        auto conns = cell["connections"].cast<pybind11::dict>();

        if (type == "$and" || type == "$or" || type == "$xor" ||
            type == "$add" || type == "$sub" || type == "$mul" || type == "$mod") {
            
            // bool a_signed = params.contains("A_SIGNED") && pybind11::cast<bool>(params["A_SIGNED"]);
            // bool b_signed = params.contains("B_SIGNED") && pybind11::cast<bool>(params["B_SIGNED"]);
            // type += (a_signed && b_signed) ? "s" : "u";
            type += "s";

            std::vector<int> a, b, y;
            for (auto bit : conns["A"].cast<pybind11::list>()) a.push_back(_bit_to_int(bit));
            for (auto bit : conns["B"].cast<pybind11::list>()) b.push_back(_bit_to_int(bit));
            for (auto bit : conns["Y"].cast<pybind11::list>()) y.push_back(_bit_to_int(bit));
            _add_aby_cell(db, type, a, b, y, B, M);  // implement similarly to Python version
        }
        else if (type == "$dff") {
            // int clk_polarity = params.contains("CLK_POLARITY") ? pybind11::cast<int>(params["CLK_POLARITY"]) : 1;
            // if (!clk_polarity) throw std::runtime_error("$dff with negative clock polarity is not supported");

            auto D = conns["D"].cast<pybind11::list>();
            auto CLK = conns["CLK"].cast<pybind11::list>();
            auto Q = conns["Q"].cast<pybind11::list>();

            if (CLK.size() != 1 || pybind11::cast<int>(CLK[0]) != clk)
                throw std::runtime_error("Clock does not match global clock");

            std::vector<int> d, q;
            for (auto bit : D) d.push_back(_bit_to_int(bit));
            for (auto bit : Q) q.push_back(_bit_to_int(bit));

            if (d.size() != q.size()) throw std::runtime_error("D and Q bit widths mismatch");

            _add_dff(db, d, q, B, M);  // implement similarly to Python version
        }
        else if (type == "$mux") {
            std::vector<int> a, b, s, y;
            for (auto bit : conns["A"].cast<pybind11::list>()) a.push_back(_bit_to_int(bit));
            for (auto bit : conns["B"].cast<pybind11::list>()) b.push_back(_bit_to_int(bit));
            for (auto bit : conns["S"].cast<pybind11::list>()) s.push_back(_bit_to_int(bit));
            for (auto bit : conns["Y"].cast<pybind11::list>()) y.push_back(_bit_to_int(bit));

            if (s.size() != 1 || a.size() != b.size() || a.size() != y.size())
                throw std::runtime_error("Invalid mux connection widths");

            _add_absy_cell(db, type, a, b, s, y, B, M);  // implement similarly to Python version
        }
        else if (type == "$not" || type == "$logic_not") {
            std::vector<int> a, y;
            for (auto bit : conns["A"].cast<pybind11::list>()) a.push_back(_bit_to_int(bit));
            for (auto bit : conns["Y"].cast<pybind11::list>()) y.push_back(_bit_to_int(bit));
            _add_ay_cell(db, type, a, y, B, M);  // implement similarly to Python version
        }
        else if (type == "$eq" || type == "$ge" || type == "$le" || type == "$gt" || type == "$lt" ||
                 type == "$logic_and" || type == "$logic_or") {
            std::vector<int> a, b, y;
            for (auto bit : conns["A"].cast<pybind11::list>()) a.push_back(_bit_to_int(bit));
            for (auto bit : conns["B"].cast<pybind11::list>()) b.push_back(_bit_to_int(bit));
            for (auto bit : conns["Y"].cast<pybind11::list>()) y.push_back(_bit_to_int(bit));

            _add_aby_cell(db, type, a, b, y, B, M);
        }
        // else {
        //     auto attrs = cell["attributes"].cast<pybind11::dict>();
        //     if (attrs.contains("module_not_derived") && pybind11::cast<bool>(attrs["module_not_derived"])) {
        //         std::vector<std::pair<std::string, std::vector<int>>> sigs;
        //         for (auto item : conns) {
        //             std::string port = pybind11::str(item.first);
        //             std::vector<int> signal;
        //             for (auto bit : item.second.cast<pybind11::list>()) signal.push_back(pybind11::cast<int>(bit));
        //             sigs.emplace_back(port, signal);
        //         }
        //         _add_blackbox_cell(name, type, params, sigs);
        //     }
        //     else {
        //         throw std::runtime_error("Unsupported cell type: " + type);
        //     }
        // }
    }

    // set cnt
    sqlite3_stmt* stmt;
    const char* cnt_sql = "SELECT MAX(wire) FROM wirevec_members";
    if (sqlite3_prepare_v2(db, cnt_sql, -1, &stmt, nullptr) != SQLITE_OK)
        throw std::runtime_error("Failed to prepare count statement: " + std::string(sqlite3_errmsg(db)));
    int cnt = 1;
    if (sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_type(stmt, 0) != SQLITE_NULL)
        cnt = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    sqlite3_close(db);

    return {clk, cnt};
}

}

#endif