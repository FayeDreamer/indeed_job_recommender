import pandas as pd # For converting results to a dataframe and bar chart plots
import csv  # To save to csv file
from datetime import datetime, timedelta 
import mysql.connector
from mysql.connector import errorcode
import ast

skill_keywords = ['r','python','java','ruby','perl', 'matlab','javascript','scala', 'excel',
 'tableau', 'd3.js','sas', 'spss', 'd3', 'hadoop','mapreduce','spark','pig','hive','shark','oozie','zookeeper','flume',
 'mahout','sql','nosql','hbase','cassandra','mongodb']
degree_keywords = ['ms','master', 'phd','bachelor', 'graduate', 'masters', 'bachelors', 'msc', 'college', 'university']

def save_data_sql(skill_df):
    '''
	This function is used for insert data into MySQL database.
	Input: job description and job type
    '''
    config = {
    'user': 'root',
    'password': 'root',
    'host': '127.0.0.1',
    'database': 'indeed_db',
    'raise_on_warnings': True,
    }

    try:
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor()
        
        # Insert INDEED job description
        add_skill = ("""INSERT INTO jobs
               (Job_ID, Job_URL, Search_Date, Job_Type, Job_Title, Company, Exp_Low, Exp_High, Elapsed_Dates, Created_Date, R, Python, Java,
                     Ruby, Perl, Matlab, Javascript, Scala, Excel,Tableau, D3, SAS, SPSS, Hadoop, MapReduce, Spark, Pig, 
                     Hive, Shark, Oozie, Zookeeper, Flume, Mahout, S_Q_L, NoSql, HBase, Cassandra, 
                     Mongodb, Degree, Experience )    
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")
        
        for index, row in skill_df.iterrows():
            if row.job_id == '08878324adf3f7dd':
                print(row)
            data_skill = (row['job_id'], row['job_url'],row['search_date'], row['job_type'], row['job_title'], row['company'], row['lowest_exp'],row['highest_exp'], row['elapsed_dates'], row['created_date'], 
                  row['r'], row['python'], row['java'],row['ruby'], row['perl'], 
                row['matlab'], row['javascript'], row['scala'], row['excel'], row['tableau'], row['d3.js'], row['sas'], row['spss'], 
                row['hadoop'], row['mapreduce'], row['spark'], row['pig'], row['hive'], row['shark'], row['oozie'], row['zookeeper'], 
                row['flume'], row['mahout'], row['sql'], row['nosql'], row['hbase'], row['cassandra'], row['mongodb'], row['degree'], row['experience'])
            cursor.execute(add_skill, data_skill)
        
        # Make sure data is committed to the database
        cnx.commit()
        cursor.close()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cnx.close()

def read_csv(filename):
	df = pd.read_csv(filename)
	if 'text' in df.columns:
            for index, row in df.iterrows():  
                text_list = ast.literal_eval(row.text)
                skill_set = set(text_list).intersection(skill_keywords)
                for skill in skill_keywords:
                    df.loc[index,skill] = int(skill in skill_set)  
                degree_set = set(text_list).intersection(degree_keywords)
                for degree in degree_keywords:
                    df.loc[index,degree] = int(degree in degree_set)
            print(df.columns)
	else:
	    df.columns = ['search_date', 'job_type', 'job_title', 'company', 'exp_low', 'exp_high', 'elapsed_dates', 'skill_set', 'degree_set']
	    df = skill_df.loc[skill_df['search_date'] == datetime.today().strftime('%Y-%m-%d')]
	return df

def deduplication(df):
    df = df.drop_duplicates(subset = 'job_id', keep='first', inplace=False)
    config = {
    'user': 'root',
    'password': 'root',
    'host': '127.0.0.1',
    'database': 'indeed_db',
    'raise_on_warnings': True,
    }
    try:
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor()
        
        # Insert INDEED job description
        cursor.execute('SELECT Job_ID FROM indeed_db.jobs')      
        # Make sure data is committed to the database
        table_rows = cursor.fetchall()  
        result_list = [x[0] for x in table_rows]
        df = df[~df.job_id.isin(result_list)]
        cnx.commit()
        cursor.close()

        return df

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        return 
    else:
        cnx.close()
        return 


def etl_data(df):
	#for index, row in df.iterrows():
	#	for skill in skill_keywords:
	#		df.loc[index, skill] = int(skill in row['skill_set'])
	#	for degree in degree_keywords:	
	#		df.loc[index, degree] = int(degree in row['degree_set'])
	
        print(df.columns)
        for index, row in df.iterrows():
                if (row['d3'] == 1):
                    df.loc[index, 'd3.js'] = 1
                if row['college'] == 1:
                    df.loc[index, 'degree'] = 'College'
                elif (row['bachelor'] == 1) or (row['bachelors'] == 1) or (row['university'] == 1):
                    df.loc[index, 'degree'] = 'Bachelor'
                elif (row['master'] == 1) or (row['ms'] == 1) or (row['masters'] == 1) or (row['graduate'] == 1) or (row['msc'] == 1):
                    df.loc[index, 'degree'] = 'Master'  
                elif row['phd'] == 1:
                    df.loc[index, 'degree'] = 'Phd'
                else:
                    df.loc[index, 'degree'] = 'NoReq'

                if (row['lowest_exp'] < 3) and (row['lowest_exp'] > 0):
                    df.loc[index, 'experience'] = 'Junior'
                elif (row['lowest_exp'] >= 3) and (row['lowest_exp'] < 7):
                    df.loc[index, 'experience'] = 'Intermediate'
                elif (row['lowest_exp'] >= 7) and (row['lowest_exp'] < 20):
                    df.loc[index, 'experience'] = 'Senior'
                else:
                    df.loc[index, 'experience'] = 'NoReq'
        
                df.loc[index, 'created_date'] = (datetime.today() - timedelta(days=row['elapsed_dates'])).strftime('%Y-%m-%d')
        for degree in degree_keywords:
            df = df.drop(columns=degree,axis=1)

        df = df.drop(columns='d3')
        print(df.columns)
        return df

if __name__ == "__main__":
	df = read_csv('indeed_raw_data.csv')
	df = deduplication(df)
	df = etl_data(df)
	
	save_data_sql(df)

#def indeed_etl():
#	skill_df = read_csv('indeed_raw_data.csv')
#	skill_df = etl_data(skill_df)
#	save_data_sql(skill_df)
