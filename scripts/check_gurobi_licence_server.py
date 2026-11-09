import socket
import gurobipy as gp

print("host:", socket.gethostname(), "| gurobipy", gp.gurobi.version())
try:
    with gp.Env() as env:
        m = gp.Model(env=env)
        x = m.addVar(lb=0, ub=1, name="x")
        m.setObjective(x, gp.GRB.MAXIMIZE)
        m.optimize()
        print("OK: license acquired, solved, x =", x.X)
except gp.GurobiError as e:
    print("FAILED:", e)
    raise SystemExit(1)
