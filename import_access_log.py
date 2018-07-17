import endpoint
from elasticsearch import Elasticsearch
from datetime import datetime
import time
import re

es = Elasticsearch([endpoint.ELS_ENDPOINT])
log_tag = 'imported-python'

def time_to_format(log_timestamp):
    log_formated_time = time.strftime('%Y-%m-%dT%H:%M:%S.000+0800', time.localtime(log_timestamp))

    return log_formated_time

def clean_time(log_time):
    log_timestamp_change = datetime.strptime(log_time, '%d/%b/%Y:%H:%M:%S%z').timetuple()

    log_timezone = "".join(re.findall('\+\d\d\d\d', log_time))

    if log_timezone == '+0000':
        to_be_add_timestamp = 28800*2

    elif log_timezone == '+0800':
        to_be_add_timestamp = 28800

    log_timestamp = int(time.mktime(log_timestamp_change))+to_be_add_timestamp
    print(log_timestamp)

    log_formated_time = time_to_format(log_timestamp)

    return log_formated_time
    

def handler(log_path):
    with open(log_path, 'r') as fp:

        for line in fp.readlines():
            try:
                log_time = str(re.findall('- \[.+\+\d\d\d\d', line)).replace('[', '').replace(']', '').replace('-', '').replace(' ', '').replace('\'', '')
                print(log_time)

                log_formated_time = clean_time(log_time)

                data_es = { 
                    "message": line,
                    "@timestamp": log_formated_time,
                    "tag": log_tag
                } 

                es.index( index="imported", doc_type="imported-python", body=data_es )

            except:
                print("import successed "+line)
        
        else:
            print("import failed ")


    fp.close()

if __name__ == '__main__':
    handler('/var/log/nginx/access.log')