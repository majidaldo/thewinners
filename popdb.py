from tables import *
import numpy as np
from numpy  import unique

#make an empty file data.hdf5 to start
h5f=openFile('data.hdf5',mode='a') #w creates it?


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
    seqid=UInt32Col()
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

class acceld(IsDescription):
    t=UInt64Col()       
    x=Float32Col()
    y=Float32Col()
    z=Float32Col()
    
def DELETEandcreatetbls():
    
    try: h5f.removeNode('/dump',recursive=True);
    except: pass
    h5f.createGroup('/','dump')
    for ads in ['train','test']:
        try: h5f.removeNode('/'+ads,recursive=True)
        except:pass
        h5f.createGroup('/',ads)
    h5f.createTable('/dump','train',traind,"training data"
                       ,expectedrows=30e6)    
    h5f.createTable('/dump','test',testd,"testing data"
                       ,expectedrows=30e6)
    try: h5f.removeNode('/questions');
    except:pass
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
        if af!='questions.csv':fl='/dump/';
        else:fl='/'
        with open(af) as df: fillf(df,h5f.getNode(fl+af[:-4]))
    #h5f.getNode('/dump/train').cols.devid.createCSIndex()
    #h5f.getNode('/dump/test').cols.seqid.createCSIndex()
    
    import warnings
    with warnings.catch_warnings(): #it warns b/c a digit is used to
    #id a deviceid or a seqid...a number can't be used as a python
    #obj attrib
        warnings.simplefilter("ignore")
        #each sensor into its table 
        trt=h5f.getNode('/dump/train')
        dids=np.unique(trt.cols.devid[:])
        for adid in dids:
            sdid=str(adid)
            tbl=h5f.createTable('/train',sdid,acceld,expectedrows=75e3)
            tbl.append([(r['t'],r['x'],r['y'],r['z']) 
                for r in trt.where('devid=='+sdid)])
            tbl.flush()
        #each seq into its tabl
        tst=h5f.getNode('/dump/test')
        sids=np.unique(tst.cols.seqid[:])
        for asid in sids:
            sid=str(asid)
            tbl=h5f.createTable('/test',sid,acceld,expectedrows=300)
            tbl.append([(r['t'],r['x'],r['y'],r['z']) 
                for r in tst.where('seqid=='+sid)])
            tbl.flush()
    h5f.removeNode('/dump',recursive=True)
    h5f.close()



if __name__=='__main__': fillerup()    
    
    
