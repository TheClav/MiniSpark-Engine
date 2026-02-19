
def checker():
    f = open('./tests/20.out.ref', 'r')
    d = dict()
    cnt = 0
    for i in f:
        k, v = i.split()
        d[k] = v
        cnt += 1
    for i in range(cnt):
        k,v = input().split()
        if k not in d:
            print(f"key {k} should not be in the output")
            return False
        if d[k] != v:
            print(f"key {k} should be {d[k]}, but {v}")
            return False
    return True
        

if checker():
    print("ok")
    

