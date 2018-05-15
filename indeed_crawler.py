from bs4 import BeautifulSoup  # For HTML parsing
import urllib  # Website connections
import re # Regular expressions
from time import sleep # To prevent overwhelming the server between connections
from collections import Counter # Keep track of our term counts
from nltk.corpus import stopwords # Filter out stopwords, such as 'the', 'or', 'and'
import pandas as pd # For converting results to a dataframe and bar chart plots
import csv  # To save to csv file
import os  # To remove old csv file
from datetime import datetime, timedelta  # To handle datetime calculation


#skill_keywords = ['r','python','java','ruby','perl', 'matlab','javascript','scala', 'excel',
# 'tableau', 'd3.js','sas', 'spss', 'd3', 'hadoop','mapreduce','spark','pig','hive','shark','oozie','zookeeper','flume',
# 'mahout','sql','nosql','hbase','cassandra','mongodb']
#degree_keywords = ['ms','master', 'phd','bachelor', 'graduate', 'masters', 'bachelors', 'msc', 'college', 'university','graduate']

base_url = 'http://ca.indeed.com'

def indeed_url(city='Toronto', state='ON', job_type = 'data_scientist'):
    '''
    This function generate search request url according to city, state and job type.
    Inputs: city, state and job type
    Outputs: job title, company, post elapsed days, skill set, degree set and experience required.
    '''
    job_type = job_type.replace('_', '+')
    # Make sure the city specified works properly if it has more than one word (such as San Francisco)
    if city is not None:
        final_city = city.split() 
        final_city = '+'.join(word for word in final_city)
        final_url_list = ['https://ca.indeed.com/jobs?q=', job_type, '&l=', final_city,
                    '%2C+', state, '&filter=0'] # Join all of our strings together so that indeed will search correctly
    else:
        final_url_list = ['http://ca.indeed.com/jobs?q="', job_type, '"']

    final_url = ''.join(final_url_list) # Merge the html address together into one string
    print(final_url)
    return final_url

def job_extractor(website, job_type):
    '''
    This function just cleans up the raw html so that I can look at it.
    Inputs: a URL to investigate
    Outputs: Cleaned text only
    '''
    try:
        site = urllib.request.urlopen(website).read() # Connect to the job posting
    except: 
        return  # Need this in case the website isn't there anymore or some other weird connection problem 
    
    try:  # we collect job title, company , job_id and job post elapsed dates
        soup_obj = BeautifulSoup(site, 'html5lib') # Get the html from the site
        job_title = soup_obj.find('b',class_='jobtitle').get_text()
        company = soup_obj.find('span',class_='company').get_text()
        job_id = re.findall(r"p_([0-9a-zA-Z]+)",str(soup_obj.find('td', class_='snip')))[0]
        elapsed = int(re.findall(r"([0-9]+)",soup_obj.find('span',class_='date').get_text())[0])
    except:
        return 
    
    for script in soup_obj(["script", "style"]):
        script.extract() # Remove these two elements from the BS4 object

    text = soup_obj.get_text() # Get the text from this
    lines = (line.strip() for line in text.splitlines()) # break into lines
    chunks = (phrase.strip() for line in lines for phrase in line.split("  ")) # break multi-headlines into a line each

    def chunk_space(chunk):
        chunk_out = chunk + ' ' # Need to fix spacing issue
        return chunk_out  

    text = ''.join(chunk_space(chunk) for chunk in chunks if chunk).encode('utf-8') # Get rid of all blank lines and ends of line

    # Now clean out all of the unicode junk (this line works great!!!)
    try:
        text = text.decode('unicode_escape') # Need this as some websites aren't formatted
    except:                                                            # in a way that this works, can occasionally throw
        return                                                         # an exception

    #print(re.findall(r"([0-9]+)[\s]+-[\s]+([0-9]+)[\s]+years",text))

    try:  # we collect experience requirement here
        exp_low = int(re.findall(r"([0-9]+)[\s]*-[\s]*([0-9]+)[\s]*years",text)[0][0])
        exp_high = int(re.findall(r"([0-9]+)[\s]*-[\s]*([0-9]+)[\s]*years",text)[0][1])
    except:
        try:
            exp_low = int(re.findall(r"([0-9]+)[\s\+]*years",text)[0])
            exp_high = 0
        except:
            exp_low = 0
            exp_high = 0
    
    text = re.sub("[^a-zA-Z.+3]"," ", text)  # Now get rid of any terms that aren't words (include 3 for d3.js)
                                                # Also include + for C++

    text = text.lower().split()  # Go to lower case and split them apart
    stop_words = set(stopwords.words("english")) # Filter out any stop words
    text = [w for w in text if not w in stop_words]
    text = list(set(text)) # Last, just get the set of these. Ignore counts (we are just looking at whether a term existed
                            # or not on the website)
    #if job_type == 'data_scientist':
    #    skill_keywords = data_scientist_skill_keywords
    #elif job_type == 'data_analyst':
    #    skill_keywords = data_analyst_skill_keywords
    #elif job_type == 'data_engineer':
    #    skill_keywords = data_engineer_skill_keywords
    #else:
    #    skill_keywords = business_analyst_skill_keywords

    # we collect skill requirements and degree requirements here 
    #skill_set = set(text).intersection(skill_keywords)
    #degree_set = set(text).intersection(degree_keywords)
    job_description = {}
    job_description['job_id'] = job_id
    job_description['job_url'] = website
    job_description['job_title'] = job_title
    job_description['job_type'] = job_type
    job_description['company'] = company
    job_description['exp_low'] = exp_low
    job_description['exp_high'] = exp_high
    job_description['elapsed'] = elapsed
    #job_description['skill_set'] = skill_set
    #job_description['degree_set'] = degree_set
    job_description['text'] = text
    return job_description

def data_extract(page_url, job_type):
    '''
    This function just cleans up the raw html so that all information about the position can be read.
    Inputs: a post URL to investigate
    Outputs: job title, company, post elapsed days, skill set, degree set and experience required.
    '''
    try:
        html = urllib.request.urlopen(page_url).read() # Open up the front page of our search first
    except:
        print('That city/state combination did not have any jobs. Exiting . . .') # In case the city is invalid
    
    soup = BeautifulSoup(html, 'lxml') # Get the html from the first page
    # Now find out how many jobs there were

    num_jobs_area = soup.find(id = 'searchCount').string # Now extract the total number of jobs found
    job_numbers = re.findall('\d+', num_jobs_area) # Extract the total jobs found from the search result
    # ['12', '122', '1111']

    if len(job_numbers) > 3: # Have a total number of jobs greater than 1000
        # 1, 22, 119
        total_num_jobs = (int(job_numbers[2])*1000) + int(job_numbers[3])
    else:
        total_num_jobs = int(job_numbers[2]) 

    print('There were ', total_num_jobs, ' jobs found,') 
    num_pages = total_num_jobs // 10 # This will be how we know the number of times we need to iterate over each new
                                      # search result page
    job_titles = [] # Store all our descriptions in this list
    print(num_pages)
    for i in range(1,num_pages+2): # Loop through all of our search result pages
        print('Getting page', i)
        start_num = str((i-1)*10) # Assign the multiplier of 10 to view the pages we want
        current_page = ''.join([page_url, '&start=', start_num])
        # Now that we can view the correct 10 job returns, start collecting the text samples from each
        print(current_page)
        html_page = urllib.request.urlopen(current_page).read() # Get the page

        page_obj = BeautifulSoup(html_page, 'lxml') # Locate all of the job links
        job_link_area = page_obj.find(id = 'resultsCol') # The center column on the page where the job postings exist

        job_urls = [str(base_url) + str(link.get('href')) for link in job_link_area.find_all('a')] # Get the URLS for the jobs
        
        job_urls = list(filter(lambda x:'clk' in x, job_urls)) # Now get just the job related URLS
        for j in range(0,len(job_urls)):
            print('job url:',job_urls[j])
            job_description = job_extractor(job_urls[j], job_type)
            print(job_description)
            if job_description: # So that we only append when the website was accessed correctly
                job_titles.append(job_description['job_title'])
                save_data_csv(job_description, job_type)
                #save_data_sql(job_description)
            sleep(0.001) # So that we don't be jerks. If you have a very fast internet connection you could hit the server a lot! 

    print('Done with collecting the job postings!')    
    print('There were ' + str(len(job_titles)) + ' jobs successfully found.')

def init_csv():
    if os.path.isfile('indeed_raw_data.csv'):
        os.remove("indeed_raw_data.csv")
        print("indeed_raw_data.csv is Removed!")
    file_name = 'indeed_raw_data.csv'
    with open(file_name, 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile)
        spamwriter.writerow(['job_id','job_url','search_date','job_type','job_title', 'company', 'lowest_exp',
                             'highest_exp', 'elapsed_dates', 'created_date',
                             'text'])

def save_data_csv(job_description, job_type):
    '''
    This function just cleans up the raw html so that all information about the position can be read.
    Inputs: a post URL to investigate
    Outputs: job title, company, post elapsed days, skill set, degree set and experience required.
    '''
#    skill_str = ' | '.join(i for i in job_description['skill_set'])
#    degree_str = ' | '.join(i for i in job_description['degree_set'])
    created_date  = datetime.today() - timedelta(days=job_description['elapsed'])
    
    file_name = 'indeed_raw_data.csv'
    with open(file_name, 'a', newline='') as csvfile:
        spamwriter = csv.writer(csvfile)
        spamwriter.writerow([job_description['job_id'], job_description['job_url'], datetime.today().strftime('%Y-%m-%d'),
                             job_description['job_type'],job_description['job_title'], job_description['company'], job_description['exp_low'],
                             job_description['exp_high'], job_description['elapsed'], 
                             created_date.strftime('%Y-%m-%d'), job_description['text']])

#    file_name = 'indeed_raw_data.csv'
#    with open(file_name, 'a', newline='') as csvfile:
#        spamwriter = csv.writer(csvfile)
#        spamwriter.writerow([datetime.today().strftime('%Y-%m-%d'),job_type,job_description['job_title'], job_description['company'], job_description['exp_low'],
#                             job_description['exp_high'], job_description['elapsed'], 
#                             list(job_description['skill_set']), list(job_description['degree_set'])])
#                             skill_str, degree_str,
                            # int('r' in job_description['skill_set']),   #R
                            # int('python' in job_description['skill_set']),  #Python
                            # int('java' in job_description['skill_set']),  #Java
                            # int('ruby' in job_description['skill_set']),  #Ruby
                            # int('perl' in job_description['skill_set']),  #Perl
                            # int('matlab' in job_description['skill_set']),  #Matlab
                            # int('javascript' in job_description['skill_set']),   #JavaScript
                            # int('scala' in job_description['skill_set']),    #Scala
                            # int('excel' in job_description['skill_set']),   #Excel
                            # int('tableau' in job_description['skill_set']),   #Tableau
                            # int('d3.js' in job_description['skill_set'] or 'd3' in job_description['skill_set']), #D3.js
                            # int('sas' in job_description['skill_set']),    #SAS
                            # int('spss' in job_description['skill_set']),   #SPSS
                            # int('hadoop' in job_description['skill_set']),  #Hadoop
                            # int('mapreduce' in job_description['skill_set']),  #MapReduce
                            # int('spark' in job_description['skill_set']),    #Spark
                            # int('pig' in job_description['skill_set']),   #Pig
                            # int('hive' in job_description['skill_set']),    #Hive
                            # int('shark' in job_description['skill_set']),    #Shark
                            # int('oozie' in job_description['skill_set']),    #Oozie
                            # int('zookeeper' in job_description['skill_set']),   #Zookeeper
                            # int('flume' in job_description['skill_set']),     #Flume
                            # int('mahout' in job_description['skill_set']),    #Mahout
                            # int('sql' in job_description['skill_set']),    #SQL
                            # int('nosql' in job_description['skill_set']),   #NoSQL
                            # int('hbase' in job_description['skill_set']),   #HBase
                            # int('cassandra' in job_description['skill_set']),  #Cassandra
                            # int('mongodb' in job_description['skill_set']),   #MongoDB
                            # int('phd' in job_description['degree_set']),     #Phd
                            # int('master' in job_description['degree_set'] or 'masters' in job_description['degree_set'] or 'ms' in job_description['degree_set'] 
                            # or 'msc' in job_description['degree_set'] or 'graduate' in job_description['degree_set']), #Master
                            # int('bachelor' in job_description['degree_set'] or 'university' in job_description['degree_set']),   #Bachelor
                            # int('college' in job_description['degree_set'])])       #College

if __name__ == "__main__":
    init_csv()
    for job_type in ['data_scientist','data_analyst','data_engineer', 'business_analyst']:
        request_url = indeed_url('Toronto','ON', job_type)
        data_extract(request_url, job_type)
#def indeed_crawl():
#    for job_type in ['data_scientist','data_analyst','data_engineer', 'business_analyst']:
#        request_url = indeed_url('Toronto','ON', job_type)
#        data_extract(request_url, job_type)