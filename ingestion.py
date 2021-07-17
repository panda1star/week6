import commonFunctions as util
import pandas as pd

#config_data = util.read_config_file('water_potability.csv')

#Read the file using Config file 
config_data = util.read_config_file("file.yaml")
file_type = config_data['file_type']
file_name = config_data['file_name']
dest_file_name = config_data['dest_file_name']


file_delimeter = config_data['inbound_delimeter']


#print(config_data['file_type'])

source_file = "./"+f'{file_name}'+f'.{file_type}'

states= ['AK','AL','AR'',AZ'',BC','CA','CO','CT','DC','DE','DP','FL','FO','GA','GV','HI','IA','ID','IL','IN','KA','KS','KY','LA','MA','MB','MD','ME','MI','MN','MO','MS','MT','MX','NB','NC','ND'
,'NE','NH','NJ','NM','NS','NT','NV','NY','OH','OK','ON','OR','PA','PE','PR','QB','RI','SC','SD','SK','TN','TX','UT','VA','VT','WA','WI','WV','WY','YT']



df_chunk = pd.read_csv(source_file,delimiter=file_delimeter, chunksize=100000)

# Validate the header of the file



chunk_list = []  # append each chunk df here 

# Each chunk is in df format
rowCounts = []
for chunk in df_chunk:  
    rawColcounts= len(chunk.columns)
    # perform data filtering 
    util.col_header_val(chunk,config_data)    
    chunk_new = chunk[['summons_number','violation_code','issue_date','plate_id','registration_state','violation_location']]

    chunk_new = chunk_new [chunk_new.registration_state.isin (states)]

    chunk_new['violation_location'] = chunk_new['violation_location'].fillna('0')

    
    # Once the data filtering is done, append the chunk to list
    chunk_list.append(chunk_new)
    total_rows = len(chunk_new)
    rowCounts.append(total_rows) 
    
    
# concat the list into dataframe 
df_concat = pd.concat(chunk_list)
rowCounts= sum(rowCounts)
colProdCounts= len(df_concat.columns)


util.df_stats(df_concat,source_file,colProdCounts,rowCounts,rawColcounts)


# read the large csv file with specified chunksize 
#df_chunk = pd.read_csv(r'../input/data.csv', chunksize=1000000)

# Validate the header of the file

#util.col_header_val(df,config_data)



# Selected cloumn list for analysis
# [Summons Number],[Violation Code],[Issue Date],[Plate ID],[Registration State] ,[Violation Location]

# [summons_number,violation_code,[Issue Date],[Plate ID],[Registration State] ,[Violation Location]

#df_new = df[['summons_number','violation_code','issue_date','plate_id','registration_state','violation_location']]
#
#states= ['AK','AL','AR'',AZ'',BC','CA','CO','CT','DC','DE','DP','FL','FO','GA','GV','HI','IA','ID','IL','IN','KA','KS','KY','LA','MA','MB','MD','ME','MI','MN','MO','MS','MT','MX','NB','NC','ND'
#,'NE','NH','NJ','NM','NS','NT','NV','NY','OH','OK','ON','OR','PA','PE','PR','QB','RI','SC','SD','SK','TN','TX','UT','VA','VT','WA','WI','WV','WY','YT']
#
#df_new = df_new [df_new.registration_state.isin (states)]
#
#df_new['violation_location'] = df_new['violation_location'].fillna('0')


#util.df_stats(df_new,source_file)

#util.df_Creategz(df,'WaterProd.txt',"./")
#util.df_Creategzfile(df_new,dest_file_name)
util.df_Creategzfile(df_concat,dest_file_name)



  
