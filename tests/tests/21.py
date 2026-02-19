
import re

def check_line(line: str):
    regex = (
        r'^RDD\s+0x[0-9a-fA-F]+\s+Part\s+\d+\s+Trans\s+(\d+)\s+--\s+creation\s+'
        r'\s*\d+\.\d{6},\s+scheduled\s+\s*\d+\.\d{6},\s+execution\s+\(usec\)\s+\d+\s*$'
    )
    match = re.match(regex, line)
    if match:
        trans_value = int(match.group(1))
        return True, trans_value
    else:
        return False, 0
def check():
    f = open('metrics.log')
    cnt = [0,0,0,0,0]
    ans = [32, 0, 64, 2, 0]

    for line in f:
        r, t = check_line(line)
        if not r:
            print("log format mismatch")
            return
        cnt[t] += 1
    cnt[0] += cnt[4]
    for idx, i in enumerate(cnt):
        if ans[idx] > i:
            print(f"too small job allocated : trans type {idx}, should be larger than {ans[idx]}, but only {i}")
            return
    print("ok")

check()