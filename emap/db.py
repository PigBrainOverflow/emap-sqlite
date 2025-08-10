import sqlite3
import json
from typing import Iterable, Any
from . import utils


class NetlistDB(sqlite3.Connection):
    _clk: int | None
    _cnt: int
    _rhash: utils.RollingHash

    @staticmethod
    def bit_to_int(bit: str | int) -> int:
        return -1 if bit == "x" else int(bit)

    @staticmethod
    def param_to_int(param: str | int) -> int:
        return param if isinstance(param, int) else int(param, base=2)

    @property
    def auto_id(self) -> int:
        self._cnt += 1
        return self._cnt

    def __init__(self, schema_file: str, db_file: str = ":memory:", cnt: int = 0):
        super().__init__(db_file)
        with open(schema_file, "r") as f:
            self.executescript(f.read())
        self._clk = None
        self._cnt = cnt
        self._rhash = utils.RollingHash()

    def dump_tables(self) -> dict:
        # get all tables except sqlite internal tables
        cur = self.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%';")
        db = {}
        for (table,) in cur.fetchall():
            cur.execute(f"SELECT * FROM {table}")
            rows = cur.fetchall()
            db[table] = [dict(zip([col[0] for col in cur.description], row)) for row in rows]
        return db

    def _create_or_lookup_wirevec(self, wv: list[int]) -> int:
        h = self._rhash.hash(wv)
        cur = self.execute("SELECT id FROM wirevecs WHERE hash = ?", (h,))
        rows = cur.fetchall()
        for (id,) in rows:  # lookup
            cur.execute("SELECT wire FROM wirevec_members WHERE wirevec = ? ORDER BY idx", (id,))
            if [w for (w,) in cur] == wv:
                return id
        # not found, insert
        cur.execute("INSERT INTO wirevecs (hash) VALUES (?) RETURNING id", (h,))
        id = cur.fetchone()[0]
        self.executemany(
            "INSERT INTO wirevec_members (wirevec, idx, wire) VALUES (?, ?, ?)",
            ((id, i, w) for i, w in enumerate(wv))
        )
        self.commit()
        return id

    def _add_input(self, name: str, source: list[int]):
        ws = self._create_or_lookup_wirevec(source)
        self.execute("INSERT INTO from_inputs (source, name) VALUES (?, ?)", (ws, name))
        self.commit()

    def _add_output(self, name: str, sink: list[int]):
        ws = self._create_or_lookup_wirevec(sink)
        self.execute("INSERT INTO as_outputs (sink, name) VALUES (?, ?)", (ws, name))
        self.commit()

    def _add_dff(self, d: list[int], q: list[int]):
        wvd = self._create_or_lookup_wirevec(d)
        wvq = self._create_or_lookup_wirevec(q)
        self.execute("INSERT OR IGNORE INTO dffs (d, q) VALUES (?, ?)", (wvd, wvq))
        self.commit()

    def _add_ay_cell(self, type_: str, a: list[int], y: list[int]):
        wva, wvy = self._create_or_lookup_wirevec(a), self._create_or_lookup_wirevec(y)
        self.execute("INSERT OR IGNORE INTO ay_cells (type, a, y) VALUES (?, ?, ?)", (type_, wva, wvy))
        self.commit()

    def _add_aby_cell(self, type_: str, a: list[int], b: list[int], y: list[int]):
        wva, wvb, wvy = self._create_or_lookup_wirevec(a), self._create_or_lookup_wirevec(b), self._create_or_lookup_wirevec(y)
        self.execute("INSERT OR IGNORE INTO aby_cells (type, a, b, y) VALUES (?, ?, ?, ?)", (type_, wva, wvb, wvy))
        self.commit()

    def _add_absy_cell(self, type_: str, a: list[int], b: list[int], s: list[int], y: list[int]):
        wva, wvb, wvs, wvy = self._create_or_lookup_wirevec(a), self._create_or_lookup_wirevec(b), self._create_or_lookup_wirevec(s), self._create_or_lookup_wirevec(y)
        self.execute("INSERT OR IGNORE INTO absy_cells (type, a, b, s, y) VALUES (?, ?, ?, ?, ?)", (type_, wva, wvb, wvs, wvy))
        self.commit()

    def _add_blackbox_cell(self, name: str, module: str, params: dict[str, Any], signals: list[tuple[str, list[int]]]):
        self.execute("INSERT INTO instances (name, params, module) VALUES (?, ?, ?)", (name, json.dumps(params), module))
        self.executemany("INSERT INTO instance_ports (instance, port, signal) VALUES (?, ?, ?)", ((name, port, self._create_or_lookup_wirevec(signal)) for port, signal in signals))
        self.commit()

    def build_from_json(self, mod: dict[str, Any], clk: str = "clk"):
        # NOTE: only support single global clock
        ports: dict[str, Any] = mod["ports"]
        cells: dict[str, Any] = mod["cells"]

        # build inputs & outputs
        for name, port in ports.items():
            direction, bits = port["direction"], [self.bit_to_int(bit) for bit in port["bits"]]
            if direction == "input":
                if name == clk:
                    if len(bits) != 1:
                        raise ValueError("Clock port must have exactly one bit")
                    self._clk = bits[0]
                self._add_input(name, bits)
            elif direction == "output":
                self._add_output(name, bits)
            else:
                raise ValueError(f"Unsupported port direction: {direction}")

        # build cells
        for name, cell in cells.items():
            type_: str = cell["type"]
            params: dict[str, Any] = cell["parameters"]
            conns: dict[str, Any] = cell["connections"]
            # TODO: for simplicity, we treat bitwise logic gates as word-level operations
            if type_ in {
                "$and", "$or", "$xor",
                "$add", "$sub", "$mul", "$mod"
            }:
                type_ += "s" if self.param_to_int(params["A_SIGNED"]) and self.param_to_int(params["B_SIGNED"]) else "u"
                a = [self.bit_to_int(bit) for bit in conns["A"]]
                b = [self.bit_to_int(bit) for bit in conns["B"]]
                y = [self.bit_to_int(bit) for bit in conns["Y"]]
                # assert len(a) == len(b) == len(y)
                self._add_aby_cell(type_, a, b, y)
            elif type_ == "$dff":
                if not self.param_to_int(params["CLK_POLARITY"]):
                    raise ValueError("$dff with negative clock polarity is not supported")
                if self._clk is None:
                    raise ValueError("Global clock is not defined")
                d, clk, q = conns["D"], conns["CLK"], conns["Q"]
                if len(clk) != 1 or self.bit_to_int(clk[0]) != self._clk:
                    raise ValueError(f"Clock {clk} does not match global clock {self._clk}")
                d = [self.bit_to_int(bit) for bit in d]
                q = [self.bit_to_int(bit) for bit in q]
                assert len(d) == len(q)
                self._add_dff(d, q)
            elif type_ == "$mux":
                a = [self.bit_to_int(bit) for bit in conns["A"]]
                b = [self.bit_to_int(bit) for bit in conns["B"]]
                s = [self.bit_to_int(bit) for bit in conns["S"]]
                y = [self.bit_to_int(bit) for bit in conns["Y"]]
                assert len(s) == 1 and len(a) == len(b) == len(y)
                self._add_absy_cell(type_, a, b, s, y)
            elif type_ in {"$not", "$logic_not"}:
                a = [self.bit_to_int(bit) for bit in conns["A"]]
                y = [self.bit_to_int(bit) for bit in conns["Y"]]
                self._add_ay_cell(type_, a, y)
            elif type_ in {
                "$eq", "$ge", "$le", "$gt", "$lt",
                "$logic_and", "$logic_or"
            }:
                a = [self.bit_to_int(bit) for bit in conns["A"]]
                b = [self.bit_to_int(bit) for bit in conns["B"]]
                y = [self.bit_to_int(bit) for bit in conns["Y"]]
                # assert len(a) == len(b)
                self._add_aby_cell(type_, a, b, y)
            else:
                attrs = cell["attributes"]
                if "module_not_derived" in attrs and self.param_to_int(attrs["module_not_derived"]): # blackbox cell
                    self._add_blackbox_cell(name, type_, params, [(port, [self.bit_to_int(bit) for bit in signal]) for port, signal in conns.items()])
                else:
                    raise ValueError(f"Unsupported cell type: {type_}")

        self.commit()
        # set cnt
        self._cnt = self.execute("SELECT MAX(wire) FROM wirevec_members").fetchone()[0] or 1

    def _merge_cells(self) -> list[list[int]]:
        # TODO: for now, we only check aby_cells
        cur = self.execute("SELECT type, a, b, y FROM aby_cells")
        wires: dict[tuple[str, int, int], list[int]] = {}
        for type_, a, b, y in cur:
            if (type_, a, b) not in wires:
                wires[(type_, a, b)] = []
            wires[(type_, a, b)].append(y)
        for (type_, a, b), ys in wires.items():
            if len(ys) > 1:
                cur.execute("DELETE FROM aby_cells WHERE type = ? AND a = ? AND b = ?", (type_, a, b))
                cur.execute("INSERT INTO aby_cells (type, a, b, y), VALUES (?, ?, ?, ?)", (type_, a, b, ys[0])) # keep the first y
        self.commit()
        return [ws for ws in wires.values() if len(ws) > 1]

    def _merge_wires(self, wires_to_merge: Iterable[list[int]]):
        for ws in wires_to_merge:
            for i in range(1, len(ws)):
                cur = self.execute("SELECT wirevec, idx FROM wirevec_members WHERE wire = ?", (ws[i],))
                for wirevec, hash_, idx in cur.fetchall(): # update wirevec's hash
                    cur.execute("UPDATE wirevecs SET hash = ? WHERE id = ?", (self._rhash.update(hash_, idx, ws[i], ws[0]), wirevec))
                cur.execute("UPDATE wirevec_members SET wire = ? WHERE wire = ?", (ws[0], ws[i]))   # update wirevec_members
        self.commit()

    def _merge_wirevecs(self):
        cur = self.execute("SELECT id, hash FROM wirevecs")
        wirevecs: dict[int, list[int]] = {}
        # TODO: merge wirevecs by hash

    def rebuild(self, max_iter: int = 1000):
        # union
        # merge_cells -> merge_wires -> merge_wirevecs -> merge_cells -> ...
        # all phases are batched processing
        # TODO: parallelize them
        for _ in range(max_iter):
            wires_to_merge = self._merge_cells()
            if not wires_to_merge:
                return
            self._merge_wires(wires_to_merge)
            self._merge_wirevecs()