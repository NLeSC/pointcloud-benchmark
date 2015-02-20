import os

ofile = open('mem_monitor','w')

while True:
    lines = os.popen('head -n 4 /proc/meminfo').read().split('\n')
    fields = []
    for line in lines:
        if line != '':
            fields.append(line.split()[1])
    ofile.write(' '.join(fields) + '\n')
    ofile.flush()
    os.system('sleep 1')
