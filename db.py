import sqlite3
from typing import Iterable, Any
import utils


class NetlistDB(sqlite3.Connection):
    _clk: int | None
    _cnt: int
    _rhash: utils.RollingHash

    @staticmethod
    def bit_to_int(bit: str | int) -> int:
        return -1 if bit == "x" else int(bit)

    def __init__(self, schema_file: str, db_file: str, cnt: int = 0):
        super().__init__(db_file)
        with open(schema_file, "r") as f:
            self.executescript(f.read())
        self._clk = None
        self._cnt = cnt
        self._rhash = utils.RollingHash()

    def _add_wirevec(self, wv: list[int]) -> int:
        h = self._rhash.hash(wv)
        cur = self.execute("SELECT id FROM wirevecs WHERE hash = ?", (h,))
        rows = cur.fetchall()
        for (id,) in rows:  # lookup
            cur.execute("SELECT wire FROM wirevec_members WHERE wirevec = ? ORDER BY index", (id,))
            if [w for (w,) in cur] == wv:
                return id
        # not found, insert
        cur.execute("INSERT INTO wirevecs (hash) VALUES (?) RETURNING id", (h,))
        id = cur.fetchone()[0]
        self.executemany(
            "INSERT INTO wirevec_members (wirevec, index, wire) VALUES (?, ?, ?)",
            ((id, i, w) for i, w in enumerate(wv))
        )
        self.commit()
        return id

    def _add_input(self, name: str, wv: list[int]):
        id = self._add_wirevec(wv)
        self.execute("INSERT INTO ports (source, name) VALUES (?, ?)", (id, name))
        self.commit()

    def _add_output(self, name: str, wv: list[int]):
        id = self._add_wirevec(wv)
        self.execute("INSERT INTO ports (sink, name) VALUES (?, ?)", (id, name))
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
        for cell in cells.values():
            type_: str = cell["type"]
            params: dict[str, Any] = cell["parameters"]
            conns: dict[str, Any] = cell["connections"]
            if type_ in {"$and", "$or", "$xor", "$add", "$sub", "$mul", "$mod"}:
                type_ += "s" if NetlistDB.to_int(params["A_SIGNED"]) and NetlistDB.to_int(params["B_SIGNED"]) else "u"
                a, b, y = NetlistDB.to_str(conns["A"]), NetlistDB.to_str(conns["B"]), NetlistDB.to_str(conns["Y"])
                self.execute("INSERT OR IGNORE INTO aby_cells (type, a, b, y) VALUES (?, ?, ?, ?)", (type_, a, b, y))
            elif type_ == "$dff":
                if not NetlistDB.to_int(params["CLK_POLARITY"]):
                    raise ValueError("$dff with negative clock polarity is not supported")
                d, clk, q = NetlistDB.to_str(conns["D"]), NetlistDB.to_str(conns["CLK"]), NetlistDB.to_str(conns["Q"])
                self.execute("INSERT OR IGNORE INTO dffs (d, clk, q) VALUES (?, ?, ?)", (d, clk, q))
            elif type_ == "$mux":
                a, b, s, y = NetlistDB.to_str(conns["A"]), NetlistDB.to_str(conns["B"]), NetlistDB.to_str(conns["S"]), NetlistDB.to_str(conns["Y"])
                self.execute("INSERT OR IGNORE INTO absy_cells (type, a, b, s, y) VALUES (?, ?, ?, ?, ?)", ("$mux", a, b, s, y))
            elif type_ in {"$not", "$logic_not"}:
                a, y = NetlistDB.to_str(conns["A"]), NetlistDB.to_str(conns["Y"])
                self.execute("INSERT OR IGNORE INTO ay_cells (type, a, y) VALUES (?, ?, ?)", (type_, a, y))
            elif type_ in {
                "$eq", "$ge", "$le", "$gt", "$lt",
                "$logic_and", "$logic_or"
            }:
                a, b, y = NetlistDB.to_str(conns["A"]), NetlistDB.to_str(conns["B"]), NetlistDB.to_str(conns["Y"])
                self.execute("INSERT OR IGNORE INTO aby_cells (type, a, b, y) VALUES (?, ?, ?, ?)", (type_, a, b, y))
            else:
                attrs = cell["attributes"]
                if "module_not_derived" in attrs and NetlistDB.to_int(attrs["module_not_derived"]): # blackbox cell
                    self.execute("INSERT INTO instances (id, module) VALUES (?, ?)", (name, type_))
                    self.executemany(
                        "INSERT INTO instance_params (instance, param, val) VALUES (?, ?, ?)",
                        ((name, param, val) for param, val in params.items())
                    )
                    self.executemany(
                        "INSERT INTO instance_ports (instance, port, wire) VALUES (?, ?, ?)",
                        ((name, port, NetlistDB.to_str(conns[port])) for port in conns)
                    )
                else:
                    raise ValueError(f"Unsupported cell type: {type_}")

        self.commit()


    def _build_from_json(self, mod: dict[str, Any], clk: str = "clk"):
        # NOTE: only support single global clock
        ports: dict[str, Any] = mod["ports"]
        cells: list[dict[str, Any]] = [cell for cell in mod["cells"].values()]
        wires: dict[int, Wire] = {}
        wire_from: dict[int, int | None] = {}  # maps wire index to cell index, None if the wire is a constant or input

        # build constants
        wires[-1] = self.let("x", Wire.from_input("x")) # DC wire, represented as "x" in the netlist
        wires[0] = self.let("0", Wire.from_input("0"))  # GND wire, represented as "0" in the netlist
        wires[1] = self.let("1", Wire.from_input("1"))  # VCC wire, represented as "1" in the netlist
        wire_from.update({-1: None, 0: None, 1: None})  # map constants to None

        # build inputs & outputs
        for name, port in ports.items():
            direction, bits = port["direction"], port["bits"]
            if direction == "input":
                if name == clk:
                    if len(bits) != 1:
                        raise ValueError("Clock port must have exactly one bit")
                    self._clk = Netlist.bit_to_int(bits[0])
                for i, bit in enumerate(bits):
                    w, ename = Netlist.bit_to_int(bit), f"{name}[{i}]"
                    ew = self.let(ename, Wire.from_input(ename))
                    wires[w] = ew
                    wire_from[w] = None  # input wires are not connected to any cell
            elif direction == "output":

            else:
                raise ValueError(f"Unsupported port direction: {direction}")

        # TODO: build blackboxes' outputs

        # build dffs' q ports
        dffs: list[dict[str, Any]] = [cell for cell in cells if cell["type"] == "$dff"]
        for dff in dffs:
            conns, params = dff["connections"], dff["parameters"]
            if not self.param_to_int(params["CLK_POLARITY"]):
                raise ValueError("$dff with negative clock polarity is not supported")
            clk, q = conns["CLK"], conns["Q"]
            if len(clk) != 1 or Netlist.bit_to_int(clk[0]) != self._clk:
                raise ValueError(f"Clock {clk} does not match global clock {self._clk}")
            for wq in q:
                wq = Netlist.bit_to_int(wq)
                wires[wq] = self.let(str(wq), Wire.from_input(self.auto_id)) # this is a placeholder, will be unioned later
                wire_from[wq] = None

        # build cells
        # NOTE: the cells may not be in topological order, so dfs is used to ensure all dependencies are resolved
        for i, cell in enumerate(cells):
            if cell["type"] != "$dff":
                wire_from.update((Netlist.bit_to_int(bit), i) for bit in Netlist.cell_to_outputs(cell))
        visited = set()
        def dfs(i: int | None):
            if i is None or i in visited:
                return
            cell = cells[i]
            type_, params, conns = cell["type"], cell["parameters"], cell["connections"]
            if type_ in {"$and", "$or", "$xor"}:    # bitwise logic gates, apply bitblast
                for wa, wb, wy in zip(conns["A"], conns["B"], conns["Y"]):
                    wa, wb, wy = Netlist.bit_to_int(wa), Netlist.bit_to_int(wb), Netlist.bit_to_int(wy)
                    dfs(wire_from[wa])
                    dfs(wire_from[wb])
                    wires[wy] = self.let(str(wy), Netlist.make_wire(type_, wires[wa], wires[wb]))
            elif type_ == "$not":
                a, y = conns["A"], conns["Y"]
                a, y = Netlist.bit_to_int(a[0]), Netlist.bit_to_int(y[0])
                dfs(wire_from[a])
                wires[y] = self.let(str(y), Netlist.make_wire(type_, wires[a]))
            elif type_ in {"$add", "$mul"}:  # word-level arithmetic operations
                # NOTE: it's hard to handle weird input widths, signed & unsigned, etc. in a generic way
                # NOTE: also it's hard to deal with different styles of extension, e.g., $signed(a) vs {16{a[15]}, a}
                a_signed, b_signed = Netlist.param_to_int(params["A_SIGNED"]), Netlist.param_to_int(params["B_SIGNED"])
                a, b, y = conns["A"], conns["B"], conns["Y"]
                if len(a) < len(y): # apply extension
                    adjusted_a = [Netlist.bit_to_int(bit) for bit in a] + [Netlist.bit_to_int(a[-1]) if a_signed else 0] * (len(y) - len(a))
                else:   # apply truncation if necessary
                    adjusted_a = [Netlist.bit_to_int(bit) for bit in a[:len(y)]]
                if len(b) < len(y): # apply extension
                    adjusted_b = [Netlist.bit_to_int(bit) for bit in b] + [Netlist.bit_to_int(b[-1]) if b_signed else 0] * (len(y) - len(b))
                else:   # apply truncation if necessary
                    adjusted_b = [Netlist.bit_to_int(bit) for bit in b[:len(y)]]
                [dfs(wire_from[wa]) for wa in adjusted_a]
                [dfs(wire_from[wb]) for wb in adjusted_b]
                wva = self.let(self.auto_id, WireVec(egglog.Vec(*(wires[wa] for wa in adjusted_a))))
                wvb = self.let(self.auto_id, WireVec(egglog.Vec(*(wires[wb] for wb in adjusted_b))))
                wvy = Netlist.make_wirevec(type_, len(y), wva, wvb)
                for j, wy in enumerate(y):
                    wy = Netlist.bit_to_int(wy)
                    wires[wy] = self.let(str(wy), wvy[j])
            elif type_ == "$mux":
                # NOTE: it deserves considering whether it's a bitwise mux or a word-level mux
                # for me, I think it's better to treat it as bitwise
                a, b, s, y = conns["A"], conns["B"], conns["S"], conns["Y"]
                assert len(s) == 1, "Mux must have exactly one select bit"
                ws = Netlist.bit_to_int(s[0])
                dfs(wire_from[ws])  # dfs on select wire
                for wa, wb, wy in zip(a, b, y):
                    wa, wb, wy = Netlist.bit_to_int(wa), Netlist.bit_to_int(wb), Netlist.bit_to_int(wy)
                    dfs(wire_from[wa])
                    dfs(wire_from[wb])
                    wires[wy] = self.let(str(wy), Wire.mux(wires[wa], wires[wb], wires[ws]))
            elif type_ == "$dff":
                return
            else:
                attrs = cell["attributes"]
                if "module_not_derived" in attrs and self.param_to_int(attrs["module_not_derived"]):    # blackbox cell
                    raise RuntimeError("Blackbox cells are not supported")
                else:
                    raise ValueError(f"Unsupported cell type: {type_}")
            visited.add(i)

        # dfs from outputs
        for name, port in ports.items():
            direction, bits = port["direction"], port["bits"]
            if direction == "output":
                self._outputs[name] = self.let(name, WireVec(egglog.Vec(*(wires[Netlist.bit_to_int(bit)] for bit in bits))))

        # dfs from dffs' d ports
        for dff in dffs:
            conns = dff["connections"]
            for wd, wq in zip(conns["D"], conns["Q"]):
                wd, wq = Netlist.bit_to_int(wd), Netlist.bit_to_int(wq)
                dfs(wire_from[wd])
                # union dff's q port with from_dff(d port)
                self.register(egglog.union(wires[wq]).with_(Wire.from_dff(wires[wd])))


    def dump_tables(self) -> dict:
        cur = self.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%';")
        db = {}
        for (table,) in cur.fetchall():
            cur.execute(f"SELECT * FROM {table}")
            rows = cur.fetchall()
            db[table] = [dict(zip([col[0] for col in cur.description], row)) for row in rows]
        return db









class NetlistDB_(sqlite3.Connection):
    cnt: int

    # auxiliary functions
    @staticmethod
    def to_str(v: Iterable) -> str:
        return ",".join(str(x) for x in v)

    @staticmethod
    def width_of(s: str) -> int:
        return s.count(",") + 1 if s else 0

    @staticmethod
    def to_bits(s: str) -> list[int | str]:
        return [x if x in {"0", "1", "x"} else int(x) for x in s.split(",")]

    @staticmethod
    def to_int(x: str | int) -> int:
        return x if isinstance(x, int) else int(x, base=2)

    def find_or_create_aby_cell(self, width: int, type_: str, a: str, b: str) -> str:
        """
        Return wire y
        """
        cur = self.execute("SELECT y FROM aby_cells WHERE type = ? AND a = ? AND b = ?", (type_, a, b))
        res = cur.fetchone()
        if res is None:  # not exists
            y = self.next_wires(width)
            self.execute("INSERT INTO aby_cells (type, a, b, y) VALUES (?, ?, ?, ?)", (type_, a, b, y))
            return y
        else:
            return res[0]

    def find_or_create_dff(self, width: int, d: str, clk: str) -> str:
        """
        Return wire q
        """
        cur = self.execute("SELECT q FROM dffs WHERE d = ? AND clk = ?", (d, clk))
        res = cur.fetchone()
        if res is None:
            q = self.next_wires(width)
            self.execute("INSERT INTO dffs (d, clk, q) VALUES (?, ?, ?)", (d, clk, q))
            return q
        else:
            return res[0]

    def tables_startswith(self, prefix: str) -> list[str]:
        cur = self.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?;", (prefix + "%",))
        return [row[0] for row in cur.fetchall()]

    def next_wire(self) -> str:
        self.cnt += 1
        return str(self.cnt)

    def next_wires(self, n: int) -> str:
        return ",".join(self.next_wire() for _ in range(n))

    def __init__(self, schema_file: str, db_file: str, cnt: int = 0):
        """
        Arguments:
        - `schema_file`: Path to the SQL schema file.
        - `db_file`: Path to the SQLite database file. Use ":memory:" for an in-memory database.
        - `cnt`: Initial count for wire generation. Defaults to 0.
        """
        super().__init__(db_file)
        with open(schema_file, "r") as f:
            self.executescript(f.read())
        self.cnt = cnt
        self.create_function("width_of", 1, NetlistDB.width_of)

    def build_from_json(self, mod: dict[str, Any]):
        ports: dict = mod["ports"]
        cells: dict = mod["cells"]

        # build ports
        db_ports = [(name, NetlistDB.to_str(port["bits"]), port["direction"]) for name, port in ports.items()]
        self.executemany("INSERT INTO ports (name, wire, direction) VALUES (?, ?, ?)", db_ports)

        # build cells
        for name, cell in cells.items():
            type_: str = cell["type"]
            params: dict = cell["parameters"]
            conns: dict = cell["connections"]
            if type_ in {"$and", "$or", "$xor", "$add", "$sub", "$mul", "$mod"}:
                type_ += "s" if NetlistDB.to_int(params["A_SIGNED"]) and NetlistDB.to_int(params["B_SIGNED"]) else "u"
                a, b, y = NetlistDB.to_str(conns["A"]), NetlistDB.to_str(conns["B"]), NetlistDB.to_str(conns["Y"])
                self.execute("INSERT OR IGNORE INTO aby_cells (type, a, b, y) VALUES (?, ?, ?, ?)", (type_, a, b, y))
            elif type_ == "$dff":
                if not NetlistDB.to_int(params["CLK_POLARITY"]):
                    raise ValueError("$dff with negative clock polarity is not supported")
                d, clk, q = NetlistDB.to_str(conns["D"]), NetlistDB.to_str(conns["CLK"]), NetlistDB.to_str(conns["Q"])
                self.execute("INSERT OR IGNORE INTO dffs (d, clk, q) VALUES (?, ?, ?)", (d, clk, q))
            elif type_ == "$mux":
                a, b, s, y = NetlistDB.to_str(conns["A"]), NetlistDB.to_str(conns["B"]), NetlistDB.to_str(conns["S"]), NetlistDB.to_str(conns["Y"])
                self.execute("INSERT OR IGNORE INTO absy_cells (type, a, b, s, y) VALUES (?, ?, ?, ?, ?)", ("$mux", a, b, s, y))
            elif type_ in {"$not", "$logic_not"}:
                a, y = NetlistDB.to_str(conns["A"]), NetlistDB.to_str(conns["Y"])
                self.execute("INSERT OR IGNORE INTO ay_cells (type, a, y) VALUES (?, ?, ?)", (type_, a, y))
            elif type_ in {
                "$eq", "$ge", "$le", "$gt", "$lt",
                "$logic_and", "$logic_or"
            }:
                a, b, y = NetlistDB.to_str(conns["A"]), NetlistDB.to_str(conns["B"]), NetlistDB.to_str(conns["Y"])
                self.execute("INSERT OR IGNORE INTO aby_cells (type, a, b, y) VALUES (?, ?, ?, ?)", (type_, a, b, y))
            else:
                attrs = cell["attributes"]
                if "module_not_derived" in attrs and NetlistDB.to_int(attrs["module_not_derived"]): # blackbox cell
                    self.execute("INSERT INTO instances (id, module) VALUES (?, ?)", (name, type_))
                    self.executemany(
                        "INSERT INTO instance_params (instance, param, val) VALUES (?, ?, ?)",
                        ((name, param, val) for param, val in params.items())
                    )
                    self.executemany(
                        "INSERT INTO instance_ports (instance, port, wire) VALUES (?, ?, ?)",
                        ((name, port, NetlistDB.to_str(conns[port])) for port in conns)
                    )
                else:
                    raise ValueError(f"Unsupported cell type: {type_}")

        self.commit()

    def dump_tables(self) -> dict:
        # get all tables
        cur = self.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%';")
        db = {}
        for (table,) in cur.fetchall():
            cur.execute(f"SELECT * FROM {table}")
            rows = cur.fetchall()
            db[table] = [dict(zip([col[0] for col in cur.description], row)) for row in rows]

        return db

    def _merge_wire(self, a: str, b: str):
        # merge a to b
        # check aby_cells
        if a == b:
            return
        cur = self.execute("SELECT rowid, a FROM aby_cells WHERE instr(',' || a || ',', ?)", (',' + a + ',',))
        self.executemany(
            "UPDATE OR IGNORE aby_cells SET a = ? WHERE rowid = ?",
            ((("," + a_ + ",").replace("," + a + ",", "," + b + ",")[1:-1], rowid) for rowid, a_ in cur)
        )
        cur.execute("SELECT rowid, b FROM aby_cells WHERE instr(',' || b || ',', ?)", (',' + a + ',',))
        self.executemany(
            "UPDATE OR IGNORE aby_cells SET b = ? WHERE rowid = ?",
            ((("," + b_ + ",").replace("," + a + ",", "," + b + ",")[1:-1], rowid) for rowid, b_ in cur)
        )

    def rebuild(self):
        # TODO: handle blackbox cells correctly
        # not efficient, but works for now
        # NOTE: the best way is to build a table for sequences of integers
        outputs: set[str] = set()   # outputs should be kept
        cur = self.execute("SELECT wire FROM ports WHERE direction = 'output'")
        for (wire,) in cur.fetchall():
            outputs.update(wire.split(","))

        modified = True
        while modified:
            modified = False
            # check aby_cells
            cur.execute("SELECT type, a, b FROM aby_cells GROUP BY type, a, b HAVING COUNT(*) > 1 LIMIT 1")
            res = cur.fetchone()
            if res is not None:
                type_, a, b = res
                cur.execute("SELECT y FROM aby_cells WHERE type = ? AND a = ? AND b = ?", (type_, a, b))
                ys = [row[0].split(",") for row in cur]
                cur.execute("DELETE FROM aby_cells WHERE type = ? AND a = ? AND b = ?", (type_, a, b))
                for i in range(0, len(ys[0])):
                    for y in ys:
                        if y[i] not in outputs:
                            self._merge_wire(y[i], ys[0][i])
                cur.execute("INSERT INTO aby_cells (type, a, b, y) VALUES (?, ?, ?, ?)", (type_, a, b, ",".join(ys[0])))
                modified = True