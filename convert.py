import sys
f = sys.argv[1]
o = sys.argv[2]
s = '######'
c = '    #'

start = False
with open(o,'w') as out:
    with open(f,'r') as infile: 
        for line in infile:
            if s in line:
                start = not start 
            if start:
                if c not in line:
                    out.write('\n') 
                else:
                    out.write(line)
            else:
                out.write(line)


