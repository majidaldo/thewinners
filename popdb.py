from tables import *

try:h5f=openFile('data.hdf5',mode='w')
except ValueError: pass #already open

class traind(IsDescription):
    """
    devid: id of device that gen the data 
    t: unix time 1/1/70
    x accelerometer in x coord
    y
    z
    """
    devid=UInt32Col()
    t=UInt64Col() #needed 64bits b/c it's microseconds
    x=Float32Col()
    y=Float32Col()
    z=Float32Col()

class testd(IsDescription):
    """
    devid: id of device that gen the data 
    t: unix time 1/1/70
    x accelerometer in x coord
    y
    z
    """
    seqid=UInt64Col()
    t=UInt64Col()   
    x=Float32Col()
    y=Float32Col()
    z=Float32Col()
    
class questionsd(IsDescription):
    """
    id of questions, sequence id, quizdevice"""
    qid=UInt32Col()
    seqid=UInt32Col()
    quiz=UInt32Col()

    
def DELETEandcreatetbls():
    try: h5f.removeNode('/train');
    except:pass
    h5f.createTable('/','train',traind,"training data"
                       ,expectedrows=30e6)
    try: h5f.removeNode('/test')
    except:pass;    
    h5f.createTable('/','test',testd,"testing data"
                       ,expectedrows=30e6)
    try: h5f.removeNode('/questions')
    except:pass;    
    h5f.createTable('/','questions',questionsd,"question data"
                       ,expectedrows=100e3)


def filltrain(trainfile,traintbl):
    trainfile.seek(0);
    trainfile.readline()#go past header line
    r=traintbl.row
    for al in trainfile:
        s=al.split(',')
        try: r['t'],       r['x'],     r['y'],     r['z']   ,r['devid']=s
        except:#stupid inconsistent file has 123e567 notation when
        #last few digits are 0
            r['t'],       r['x'],     r['y'],     r['z']   ,r['devid']=\
            int(float(s[0]))   ,s[1]        ,s[2]      ,s[3]     ,s[4]
        r.append()
    traintbl.flush()
    

def filltest(testfile,testtbl):
    testfile.seek(0);
    testfile.readline()#go past header line
    r=testtbl.row
    for al in testfile:
        s=al.split(',')
        try: r['t'],       r['x'],     r['y'],     r['z']   ,r['seqid']=s
        except:#stupid inconsistent file has 123e567 notation when
        #last few digits are 0
            r['t'],       r['x'],     r['y'],     r['z']   ,r['seqid']=\
            int(float(s[0]))   ,s[1]        ,s[2]      ,s[3]     ,s[4]
        r.append()
    testtbl.flush()

def fillqs(qsfile,qstbl):
    qsfile.seek(0);
    qsfile.readline()#go past header line
    r=qstbl.row
    for al in qsfile:
        r['qid'], r['seqid'], r['quiz']=al.split(',')
        r.append()
    qstbl.flush()
    

def fillerup():
    DELETEandcreatetbls()
    for af,fillf in \
    [('train.csv',filltrain),('test.csv',filltest),('questions.csv',fillqs)]:
        with open(af) as df: fillf(df,h5f.getNode('/'+af[:-4]))
    

    