import os

datadir='data'

#files in datadir
fns=\
{'data':'data.hdf5'
,'majid':'msdresults.hdf5'
#,'seqids':'seqids.txt'
#,'devids':'devids.txt'
}

for an,af in fns.iteritems(): fns[an]=os.path.join(datadir,af)

