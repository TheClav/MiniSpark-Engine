

for i in range(1024):
    f = open(f'./test_files/{i}', 'w')
    f.write(f'{i}\t{i*2}\nasdf\t1\nqwer\t{i}')
#    f.write(f'{i}\t{i*2}\n')
    f.close()

s = 'abcdefghijklmnopqrstuvwxyz'

f1 = open('./test_files/one', 'w')
f2 = open('./test_files/two', 'w')
for i in range(1024):
    f1.write(f"{s[((i+(i%3))%26):]} ")
    f2.write(f"{s[((2*i+(i%3))%26):]} ")

f1.close()
f2.close()
    

import random
random.seed(1234)

k = [i for i in range(512*1024)]

random.shuffle(k)
for j in range(512):
    f1 = open(f'./test_files/largevals{j}.txt', 'w')
    for i in range(1024):
        key = k[j*1024+i]
        f1.write(f"{key}\t{random.randrange(10000)}\n")
    f1.close()

random.shuffle(k)
for j in range(512,1024):
    f1 = open(f'./test_files/largevals{j}.txt', 'w')
    for i in range(1024):
        key = k[(j-512)*1024+i]
        f1.write(f"{key}\t{random.randrange(10000)}\n")
    f1.close()