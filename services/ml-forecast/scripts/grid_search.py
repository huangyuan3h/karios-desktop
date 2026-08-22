#!/usr/bin/env python3
import sys, pathlib, subprocess, json, time
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]/"src"))
# simple grid: run_trian sequentially and collect Auc
import subprocess, pathlib

grid = [
    ("tcn", 10, 8, 60),
    ("tcn", 5, 5, 60),
    ("lstm", 20, 8, 60),
    ("lstm", 10, 8, 60),
    # ("tcn",20,8,30),
]

for model,n,x,L in grid:
    cmd = ["services/ml-forecast/.venv/bin/python","-u","services/ml-forecast/scripts/run_train.py","--model",model,"--n",str(n),"--x",str(x),"--epochs","25"]
    # patch run_train to support L via env?
    env = {"DATABASE_URL":"postgresql://admin:admin123@localhost:5432/karios-desktop"}
    print(f"\n=== GRID {model} N{n} X{x} L{L} ===")
    # need to edit run_train L default 60, but we can sed: just run with default 60 for now
    # For L30 we would need to pass, skip
    log = pathlib.Path(f"/tmp/grid_{model}_N{n}_X{x}.log")
    with open(log,"w") as f:
        p = subprocess.Popen(cmd, env={**__import__("os").environ, **env}, stdout=f, stderr=subprocess.STDOUT)
        p.wait()
    print(open(log).read()[-2000:])
    time.sleep(2)
