import struct, datetime, decimal, itertools, re, json, os, requests, sys, logging, codecs
reload(sys)
sys.setdefaultencoding('utf-8')
 

def dbfreader(f):

    f.seek(0) 

    numrec, lenheader = struct.unpack('<xxxxLH22x', f.read(32))    
    numfields = (lenheader - 33) // 32
 
    export_data =[]
    
    fields = []
    for fieldno in xrange(numfields):
        name, typ, size, deci = struct.unpack('<11sc4xBB14x', f.read(32))
        name = name.replace('\0', '')         
        if(typ != '\x00'):
            fields.append((name, typ, size)) 

    header = [field[0] for field in fields]
    column = [tuple(field[1:]) for field in fields]
    #print header, column
    while( f.read(1)== '\x00' ): pass
 
    fields.insert(0, ('DeletionFlag', 'C', 1, 0))
    fmt = ''.join(['%ds' % fieldinfo[2] for fieldinfo in fields])
    fmtsiz = struct.calcsize(fmt)
     
    for i in xrange(numrec):
 
        xline    = f.read(fmtsiz).decode('latin-1').encode('utf8') #
        #print xline
        i        = 1
        line_pos = 0
        data     = {}
        
        for col in column:
            type = col[0]
            size = col[1]
            col_name = header[i-1] 
 
            if type == 'N':  
                try:
                    data[col_name]= xline[line_pos: line_pos+size].strip()
                except:
                    #print col_name, xline[line_pos: line_pos+size].strip()
                    data[col_name]=""

                line_pos      = line_pos + size

            if type == 'C': 
                data[col_name]= xline[line_pos:line_pos+size].strip()
                line_pos      = line_pos + size

            if type == 'D': 
                tmp           = xline[line_pos:line_pos+size]
                data[col_name]= tmp[0:4]+"-"+tmp[4:6]+"-"+tmp[6:8]
                line_pos      = line_pos + size

            if type == 'W':
                data[col_name]= ""
                line_pos      = line_pos + size

            if type == 'M':
                data[col_name]= ""
                line_pos      = line_pos + size

            i=i+1 
            #print col_name,": ", data[col_name]
        export_data.append( json.dumps(data) ) 
    f.close() 

    return "["+','.join(export_data)+"]"

def get_dbf(file):
    logging.info("Import data from %s" % file)

    if(os.path.exists(file)):
        f = open(file, 'rb')  
        d = dbfreader(f) 
    else:
        logging.error("DataFile no found: %s" % file)

 
    myfile = open(file+".json", 'w') 
    myfile.write(d) 
    myfile.close()
 

def upload_data(url, file):
    logging.info("Upload json from %s" % file)
    
    if(os.path.exists(file)):

        files  = {'upload'   : open(file,'rb')}
        values = {'filename' : os.path.basename(file) }
        r      = requests.post(url, files=files, data=values)
        logging.info(r.content)
    else:
        logging.error("File JSON no found: %s" % file)


if __name__ == '__main__':

    logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')

    with open('config.json') as json_file:
        logging.info('Started')
    
        config = json.load(json_file)
        for file in config["dbf_files"]:
            get_dbf(config["folder_data"]+file)
            upload_data(config["host"], config["folder_data"]+file+".json")
    
        logging.info('Finished')