import emap
import emap.rewrites as rewrites
import json

TESTS_PATH = "../tests/designs/systolic"
netlist = emap.NetlistDB("emap/schema.sql")
import time
start = time.time()
with open(f"{TESTS_PATH}/systolic.json", "r") as f:
    netlist.build_from_json(json.load(f)["modules"]["systolic"])
print(f"Built netlist in {time.time() - start:.2f} seconds")

netlist.rebuild()

cnt = 1
while cnt > 0:
    comm_matches = rewrites.ematch_comm(netlist, ["$adds", "$addu", "$subs", "$subu", "$muls", "$mulu"])
    assoc_to_right_matches = rewrites.ematch_assoc_to_right(netlist, ["$adds", "$addu", "$subs", "$subu", "$muls", "$mulu"])
    assoc_to_left_matches = rewrites.ematch_assoc_to_left(netlist, ["$adds", "$addu", "$subs", "$subu", "$muls", "$mulu"])
    dff_forward_aby_cell_matches = rewrites.ematch_dff_forward_aby_cell(netlist, ["$adds", "$addu", "$subs", "$subu", "$muls", "$mulu"])

    cnt = 0
    cnt += rewrites.apply_comm(netlist, comm_matches)
    cnt += rewrites.apply_assoc_to_right(netlist, assoc_to_right_matches)
    cnt += rewrites.apply_assoc_to_left(netlist, assoc_to_left_matches)
    cnt += rewrites.apply_dff_forward_aby_cell(netlist, dff_forward_aby_cell_matches)

    print(f"Applied {cnt} rewrites")
    netlist.rebuild()

# with open("systolic.json", "w") as f:
#     json.dump(netlist.dump_tables(), f, indent=2)

# while netlist.rebuild_once():
#     print("Rebuilt once")

# with open("systolic_out.json", "w") as f:
#     json.dump(netlist.dump_tables(), f, indent=2)