CREATE TABLE IF NOT EXISTS wirevecs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hash INTEGER NOT NULL
);  -- not sure whether we need length, we can get it from max(index) + 1 in wirevec_members

CREATE TABLE IF NOT EXISTS wirevec_members (
    wirevec INTEGER,
    index INTEGER,
    wire INTEGER NOT NULL,
    PRIMARY KEY (wirevec, index),
    FOREIGN KEY (wirevec) REFERENCES wirevecs(id)
);

CREATE TABLE IF NOT EXISTS as_outputs (
    sink INTEGER NOT NULL,
    name VARCHAR(16) PRIMARY KEY,
    FOREIGN KEY (sink) REFERENCES wirevecs(id)
);

CREATE TABLE IF NOT EXISTS from_inputs (
    source INTEGER NOT NULL,
    name VARCHAR(16) PRIMARY KEY,
    FOREIGN KEY (source) REFERENCES wirevecs(id)
);

CREATE TABLE IF NOT EXISTS ay_cells (
    type VARCHAR(16),
    a INTEGER,
    y INTEGER,
    PRIMARY KEY (type, a, y),
    FOREIGN KEY (a) REFERENCES wirevecs(id),
    FOREIGN KEY (y) REFERENCES wirevecs(id)
);  -- not sure whether we need a bitwise version of it

CREATE TABLE IF NOT EXISTS aby_cells (
    type VARCHAR(16),
    a INTEGER,
    b INTEGER,
    y INTEGER,
    PRIMARY KEY (type, a, b, y),
    FOREIGN KEY (a) REFERENCES wirevecs(id),
    FOREIGN KEY (b) REFERENCES wirevecs(id),
    FOREIGN KEY (y) REFERENCES wirevecs(id)
);

CREATE TABLE IF NOT EXISTS absy_cells (
    type VARCHAR(16),
    a INTEGER,
    b INTEGER,
    s INTEGER,
    y INTEGER,
    PRIMARY KEY (type, a, b, s, y),
    FOREIGN KEY (a) REFERENCES wirevecs(id),
    FOREIGN KEY (b) REFERENCES wirevecs(id),
    FOREIGN KEY (s) REFERENCES wirevecs(id),
    FOREIGN KEY (y) REFERENCES wirevecs(id)
);

CREATE TABLE IF NOT EXISTS dffs (
    d INTEGER,
    q INTEGER,
    PRIMARY KEY (d, q),
    FOREIGN KEY (d) REFERENCES wirevecs(id),
    FOREIGN KEY (q) REFERENCES wirevecs(id)
);  -- we assume there's a global clock wire

CREATE TABLE IF NOT EXISTS instances (
    name VARCHAR(16) PRIMARY KEY,
    params JSON,    -- no need to process this, just store it
    module VARCHAR(16) NOT NULL
);

CREATE TABLE IF NOT EXISTS instance_ports (
    instance VARCHAR(16),
    port VARCHAR(16),
    signal INTEGER NOT NULL,    -- in RTLIL, a signal is everything that can be applied to a cell port
    direction VARCHAR(16),  -- 'input', 'output', null
    PRIMARY KEY (instance, port),
    FOREIGN KEY (instance) REFERENCES instances(name),
    FOREIGN KEY (signal) REFERENCES wirevecs(id)
);
