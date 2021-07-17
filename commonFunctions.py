import logging
import os
import yaml
import re
import gzip

# File reading

def read_config_file(filePath):
    with open(filePath,'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)
            
def replacer(string,char):
    pattren= char+'{2,}'              # 2 is used if someone add file__test correct name is file_test
    string = re.sub(pattren,char,string)
    return string

def col_header_val(df,table_config):
    # Replace whitespaces in the column and standardized column names
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace('[^\w]','_',regex=True)
    df.columns = list(map(lambda x: x.strip('_'),list(df.columns)))
    df.columns = list(map(lambda x: replacer(x,'_'),list(df.columns)))
    
    expected_col = list(map(lambda x: x.lower(), table_config['columns']))
    
    expected_col.sort()
    df.columns = list (map(lambda x:x.lower(),list(df.columns)))
    df = df.reindex(sorted(df.columns),axis= 1)
    
    if len(df.columns) == len(expected_col) and list(expected_col) == list(df.columns):
        print('Columns name and column length validation passed')
        return 1
    else:
        print('Columns name and column length validation failed')
        mismatched_columns_file = list(set(df.columns).difference(expected_col))
        print ('Following File columns are not in the YAML file',mismatched_columns_file)
        missing_YAML_file = list(set(expected_col).difference(df.columns))
        print ('Following File columns are not in the following uploaded file',missing_YAML_file)
        logging.info(f'df columns:{df.columns}')
        logging.info(f'expected columns:{expected_col}')
        return 0
    
    
def df_stats(df,filename,columnCount,rowCount,rawColumncounts):
    
    total_rows = rowCount
    total_columns = columnCount
    total_raw_columns = rawColumncounts
    
    
    print ("Total Number of Columns in the raw data set: "+ str(total_raw_columns))
    total_columns = len(df.columns)
    print ("Total Number of Columns in the data set: "+str( total_columns))
    print ("Total Number of Rows in the raw data set: "+ str(total_rows))

    file_size = os.path.getsize(filename)/1024
    print("File Size is :", file_size, "KB")


def df_Creategzfile(df,filename):    
   sourceFile = filename+'.txt'  
   df.to_csv(sourceFile, header=True, index=False, sep='|', mode='a')
   
   f_in = open(sourceFile,'rb')
   f_out = gzip.open(filename+'.gz', 'wb')
   f_out.writelines(f_in)
   f_out.close()
   f_in.close()    
   os.remove(sourceFile)

 
    
    