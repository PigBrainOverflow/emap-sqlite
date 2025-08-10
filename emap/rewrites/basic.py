from ..db import NetlistDB
from .utils import rewrite_tags


@rewrite_tags(post_rebuild=False, batched=True)
def rewrite_comm(db: NetlistDB, target_types: list[str]) -> int:
    """
    Rewrite commutative cells by swapping inputs.
    Return the number of rows rewritten.
    """
    cur = db.execute(
        "SELECT type, a, b, y FROM aby_cells WHERE type IN ({})".format(",".join("?" * len(target_types))),
        target_types
    )
    cur.executemany(
        "INSERT OR IGNORE INTO aby_cells (type, a, b, y) VALUES (?, ?, ?, ?)",
        [(type_, b, a, y) for type_, a, b, y in cur]
    )
    db.commit()
    return cur.rowcount

@rewrite_tags(post_rebuild=True, batched=True)
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

    rows = cur.fetchall()
    newrows = []
    for type_, a, b, c, y in rows:
        cur.execute("SELECT MAX(idx) FROM wirevec_members WHERE wirevec = ?", (y,))
        width_b_add_c = cur.fetchone()[0] + 1
        cur.execute(
            "SELECT y FROM aby_cells WHERE type = ? AND a = ? AND b = ? AND (SELECT MAX(idx) + 1 FROM wirevec_members WHERE wirevec = y) = ? LIMIT 1",
            (type_, b, c, width_b_add_c)
        )
        row = cur.fetchone()
        if row is None:
            # b + c does not exist, create it
            newrows.append((type_, a, db._create_or_lookup_wirevec([db.auto_id for _ in range(width_b_add_c)], y)))
        else:
            # b + c exists, use it
            newrows.append((type_, a, row[0], y))
    cur.executemany("INSERT OR IGNORE INTO aby_cells (type, a, b, y) VALUES (?, ?, ?, ?)", newrows)
    db.commit()
    return cur.rowcount