from typing import Iterable
from ..db import NetlistDB


def ematch_comm(db: NetlistDB, target_types: list[str]) -> Iterable[tuple[str, int, int, int]]:
    """
    Return a list of tuples (type, a, b, y) for commutative cells.
    """
    cur = db.execute(
        "SELECT type, a, b, y FROM aby_cells WHERE type IN ({})".format(",".join("?" * len(target_types))),
        target_types
    )
    return cur

def apply_comm(db: NetlistDB, matches: Iterable[tuple[str, int, int, int]]) -> int:
    """
    Return the number of rows rewritten by applying commutative matches.
    """
    cur = db.executemany(
        "INSERT OR IGNORE INTO aby_cells (type, a, b, y) VALUES (?, ?, ?, ?)",
        ((type_, b, a, y) for type_, a, b, y in matches)
    )
    db.commit()
    return cur.rowcount


# TODO: separate it
def rewrite_assoc_to_right(db: NetlistDB, target_types: list[str]) -> int:
    """
    Rewrite associative cells to right associative form.
    E.g. (a + b) + c => a + (b + c)
    NOTE: The width of b + c should be the same as (a + b) + c to preserve the semantics.
    Return the number of rows rewritten.
    """
    cur = db.execute("""
        SELECT cell1.type, cell1.a, cell1.b, cell2.b, cell2.y
        FROM aby_cells AS cell1 JOIN aby_cells AS cell2 ON cell1.y = cell2.a
        WHERE cell1.type = cell2.type AND cell1.type IN ({})
        """.format(",".join("?" * len(target_types))),
        target_types
    )

    newrows = []
    for type_, a, b, c, y in cur.fetchall():
        cur.execute("SELECT MAX(idx) FROM wirevec_members WHERE wirevec = ?", (y,))
        width_b_add_c = cur.fetchone()[0] + 1
        cur.execute(
            "SELECT y FROM aby_cells WHERE type = ? AND a = ? AND b = ? AND (SELECT MAX(idx) + 1 FROM wirevec_members WHERE wirevec = y) = ? LIMIT 1",
            (type_, b, c, width_b_add_c)
        )
        row = cur.fetchone()
        newrows.append((type_, a, db._add_wirevec([db.auto_id for _ in range(width_b_add_c)]) if row is None else row[0], y))
    cur.executemany("INSERT OR IGNORE INTO aby_cells (type, a, b, y) VALUES (?, ?, ?, ?)", newrows)
    db.commit()
    return cur.rowcount