import emap
import emap.rewrites as rewrites
import json
import time

TESTS_PATH = "../tests/designs/systolic"
netlist = emap.NetlistDB("emap/schema.sql", "file:netlist?mode=memory&cache=shared")
start = time.time()
with open(f"{TESTS_PATH}/systolic.json", "r") as f:
    netlist.build_from_json_cpp(json.load(f)["modules"]["systolic"])
print(f"Built netlist in {time.time() - start:.2f} seconds")

with open("systolic.json", "w") as f:
    json.dump(netlist.dump_tables(), f, indent=2)

# while netlist.rebuild_once():
#     print("Rebuilt once")

# with open("systolic_out.json", "w") as f:
#     json.dump(netlist.dump_tables(), f, indent=2)