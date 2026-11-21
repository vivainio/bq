import sys,re,collections,xml.etree.ElementTree as ET
t=ET.parse(sys.argv[1]).getroot()
files=collections.defaultdict(lambda:[set(),set()])  # all lines, hit lines
meths=collections.defaultdict(list)
for m in t.iter('method'):
    fn=m.get('filename') or ''
    if '<>' in (m.get('class') or '') and False: pass
    stm=list(m.iter('statement'))
    if not stm: continue
    hit=any(int(s.get('counter'))>0 for s in stm)
    for s in stm:
        files[fn][0].add(int(s.get('line'))); 
        if int(s.get('counter'))>0: files[fn][1].add(int(s.get('line')))
    if not hit: meths[fn].append((m.get('class'),m.get('name'),m.get('line')))
tot=hh=0
rows=[]
for f,(a,h) in files.items():
    tot+=len(a);hh+=len(h)
    rows.append((len(a)-len(h),f.split('/')[-1],len(h),len(a)))
rows.sort(reverse=True)
print("total lines %d hit %d = %.1f%%"%(tot,hh,100*hh/tot))
for r in rows[:int(sys.argv[2]) if len(sys.argv)>2 else 40]: print("%5d missed  %-38s %d/%d"%r)
if len(sys.argv)>3:
    for f,(a,h) in files.items():
        if f.endswith('/'+sys.argv[3]):
            miss=sorted(a-h); print(sys.argv[3],"missed lines:",miss)
