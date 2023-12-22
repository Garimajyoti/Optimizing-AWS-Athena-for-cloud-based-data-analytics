import csv
import os
import re
import traceback
from datetime import datetime
import random
def parse_log_entry(log_entry,pattern):
    # Define the regex pattern to extract fields
    # pattern = 

    # Match the log entry against the pattern
    match = pattern.match(log_entry)
    
    if match:
        return match.groupdict()
    else:
        return None
def process_hadoop_data():
    # open hadoop folder and one by onde open the subfolders
    hadoop_folder = os.path.join(os.getcwd(), 'hadoop')
    count_files = 0
    #open a new csv in write mode to write the data
    with open('./processed_data/hadoop.csv', 'w') as f:
        headers = ["Date", "Time", "Level", "ProcessId", "Component", "Message"]
        csv_writer = csv.DictWriter(f, fieldnames=headers)
        csv_writer.writeheader()
    
        for folder in os.listdir(hadoop_folder):
            folder_path = os.path.join(hadoop_folder, folder)
            if "application" not in folder_path:
                continue
            # open the subfolder and one by one open the files
            for file in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file)
                count_files+=1
                # open the file and read the log file data, delimit by space
                with open(file_path, 'r') as f:
                    # create file reader
                    log_entries=f.readlines()
                    
                    for log_entry in log_entries:
                        try:
                            parsed_data = parse_log_entry(log_entry,pattern=re.compile(r'(?P<Date>\d{4}-\d{2}-\d{2}) (?P<Time>\d{2}:\d{2}:\d{2},\d{3}) (?P<Level>\w+) \[(?P<ProcessId>.*?)\] (?P<Component>.*?)\: (?P<Message>.*)'))
                            if parsed_data:
                                
                                csv_writer.writerow(parsed_data)
                        except:

                            print("log entry: ", log_entry)
                            input()
    
def process_data(input_files,output_file,headers,pattern,encoding='utf-8'):

    with open(output_file, 'w') as csv_file:
        
        csv_writer = csv.DictWriter(csv_file, fieldnames=headers)
        csv_writer.writeheader()
        
        for file_path in input_files:
            with open(file_path, 'r', encoding=encoding) as f:
                log_entries=f.readlines()
                levels=[]
                for log_entry in log_entries:
                    try:
                        
                        parsed_data = parse_log_entry(log_entry,pattern = pattern)

                        if parsed_data:
                            # print(parsed_data)
                            if('Date' not in parsed_data):
                                if("Month" in parsed_data) and parsed_data["Month"] in ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]:
                                    #convert month to number
                                    parsed_data['Month'] = datetime.strptime(parsed_data['Month'], "%b").strftime("%m")
                                if("Day" in parsed_data):
                                    # if day is single digit, add 0 before it
                                    parsed_data['Day'] = parsed_data['Day'].zfill(2)
                                if("TimeStamp" in parsed_data):
                                    # convert epoch time to date time
                                    parsed_data['Date'] = datetime.fromtimestamp(int(parsed_data['TimeStamp'])).strftime('%Y-%m-%d')
                                # input()
                                elif("Year" in parsed_data and "Month" in parsed_data and "Day" in parsed_data):
                                    parsed_data['Date'] = parsed_data['Year'] + "-" + parsed_data['Month'] + "-" + parsed_data['Day']
                                    
                                elif("Month" in parsed_data and "Day" in parsed_data):
                                    parsed_data['Date'] = "2020" + "-" + parsed_data['Month'] + "-" + parsed_data['Day']
                                # input()
                            if "ProcessId" in parsed_data and not parsed_data["ProcessId"]:
                                parsed_data["ProcessId"] = "1"
                            if "ProcessId" not in parsed_data:
                                parsed_data["ProcessId"] = "1"
                            if "Level" not in parsed_data:
                                # randomly select level between "INFO", "WARNING", "ERROR", "CRITICAL"
                                parsed_data["Level"] = random.choice(["INFO", "WARNING", "ERROR", "CRITICAL"])
                            if parsed_data["Level"]=="combo":
                                parsed_data["Level"] = random.choice(["INFO", "WARNING", "ERROR", "CRITICAL"])
                            if parsed_data["Level"] not in levels:
                                levels.append(parsed_data["Level"])
                            csv_writer.writerow(parsed_data)
                        
                    except:
                        print(parsed_data)
                        print("log entry: ", log_entry)
                        print(traceback.format_exc())
                        input()
                # print(levels)
                # input()

def main():
    # for hadoop
    process_hadoop_data()
    # for zookeeper
    process_data(input_files=['./Zookeeper.log'],output_file='./processed_data/zookeeper.csv', headers= ["Date", "Time", "Level", "Component", "Message", "ProcessId"],pattern=re.compile(r'(?P<Date>\d{4}-\d{2}-\d{2}) (?P<Time>\d{2}:\d{2}:\d{2},\d{3}) - (?P<Level>\w+) +\[(?P<Component>.*?)\] - (?P<Message>.*)'))
    # process_zookeeper_data()
    
    # for openstack
    process_data(input_files=["openstack_normal1.log", "openstack_normal2.log","openstack_abnormal.log"],output_file='./processed_data/openstack.csv', headers= ["log_record", "Date", "Time", "ProcessId", "Level", "Component", "addr", "Message"],pattern=re.compile(r'(?P<log_record>[^\s]+) (?P<Date>\d{4}-\d{2}-\d{2}) (?P<Time>\d{2}:\d{2}:\d{2}\.\d+) (?P<ProcessId>\d+) (?P<Level>\w+) (?P<Component>[^\[]+) \[(?P<addr>[^\]]+)\] (?P<Message>.*)'))
    
    # for linux
    process_data(input_files=["Linux.log"],output_file='./processed_data/linux.csv', headers= ["Month", "Day", "Time", "Level", "Component", "ProcessId","Message","Date"],pattern=re.compile(r'^(?P<Month>[A-Za-z]{3})\s+(?P<Day>\d{1,2})\s(?P<Time>\d{2}:\d{2}:\d{2})\s(?P<Level>\S+)\s(?P<Component>[^:\[]+)(?:\[(?P<ProcessId>[^\]]+)\])?\s*:\s*(?P<Message>.+)$'),encoding='latin-1')

    # for openssh
    process_data(input_files=["SSH.log"],output_file='./processed_data/openssh.csv', headers= ["Month", "Day", "Time", "Component", "ProcessId", "Message","Date","Level"],pattern=re.compile(r'(?P<Month>\w{3})\s+(?P<Day>\d{1,2})\s+(?P<Time>\d{2}:\d{2}:\d{2})\s+(?P<Component>\S+)\s+sshd\[(?P<ProcessId>\d+)\]:\s+(?P<Message>.*)'))

    # for android
    process_data(input_files=["HealthApp.log","HealthApp 2.log","HealthApp 3.log"],output_file='./processed_data/android.csv', headers= ["Year","Month", "Day", "Time","Component", "ProcessId", "Message","Date","Level"],pattern=re.compile(r'(?P<Year>\d{4})(?P<Month>\d{2})(?P<Day>\d{2})-(?P<Time>\d{2}:\d{2}:\d{2}:\d{3})\|(?P<Component>[^\|]+)\|(?P<ProcessId>\d+)\|(?P<Message>.*)'))
    
    # for hpc
    process_data(input_files=["HPC.log"],output_file='./processed_data/hpc.csv', headers= ["LogId", "Node", "Component","State", "TimeStamp", "Flag","Message","Date","Level","ProcessId"],pattern=re.compile(r'(?P<LogId>\d+)\s+(?P<Node>[\w-]+)\s+(?P<Component>[\w.]+)\s+(?P<State>\S+)\s+(?P<TimeStamp>\d+)\s+(?P<Flag>\d+)\s+(?P<Message>.*)'))

def split_file(file_path, headers):
    print(file_path)
    print(headers)
    data = {}
    folder = os.path.join(os.getcwd(), 'split_data')
    count_files = 0
    #open a new csv in write mode to write the data
    with open(file_path, 'r') as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            date = row["Date"]
            level = row["Level"]
            if date not in data:
                data[date] = {}
            if level not in data[date]:
                data[date][level] = []
            data[date][level].append(row)
    for date in data:
        for level in data[date]:
            if not os.path.exists(os.path.join(folder,date,level)):
                os.makedirs(os.path.join(folder,date,level))
            with open(os.path.join(folder,date,level,file_path.split("/")[-1]), 'w') as f:
                new_headers = ["Date", "Level", "ProcessId", "Component", "Message"]
                csv_writer = csv.DictWriter(f, fieldnames=new_headers)
                # csv_writer.writeheader()
                
                for row in data[date][level]:
                    new_row = {}
                    for header in row:
                        if header in new_headers:
                            new_row[header] = row[header]
                    # order the keys in the row in order of new_headers
                    new_row = {k: new_row[k] for k in new_headers}
                    # print(row)
                    # input()
                    csv_writer.writerow(new_row)
def split_data():

    # for hadoop
    split_file(file_path="./processed_data/hadoop.csv", headers=["Date", "Time", "Level", "ProcessId", "Component", "Message"])
    # for zookeeper
    split_file(file_path="./processed_data/zookeeper.csv", headers=["Date", "Time", "Level", "Component", "Message","ProcessId"])
    # for openstack
    split_file(file_path="./processed_data/openstack.csv", headers=["log_record", "Date", "Time", "ProcessId", "Level", "Component", "addr", "Message"])
    # for linux
    split_file(file_path="./processed_data/linux.csv", headers=["Month", "Day", "Time", "Level", "Component", "ProcessId","Message","Date"])
    # for openssh
    split_file(file_path="./processed_data/openssh.csv", headers=["Month", "Day", "Time", "Component", "ProcessId", "Message","Date","Level"])
    # for android
    split_file(file_path="./processed_data/android.csv", headers=["Year","Month", "Day", "Time","Component", "ProcessId", "Message","Date","Level"])
    # for hpc
    split_file(file_path="./processed_data/hpc.csv", headers=["LogId", "Node", "Component","State", "TimeStamp", "Flag","Message","Date","Level","ProcessId"])
    
# main()
split_data()