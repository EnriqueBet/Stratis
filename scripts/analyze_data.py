'''
#program:
    analyze_data.py
#summary:
    Returns the number of total events per hour, the most used direction of all public IP's
    the total duration of the idle movements for the IP 127.0.0.1 into a json format.
#author:
    Enrique Betancourt
#email:
    fis.ebetancourt@tgmail.com
'''

import sys,os,json
import pandas as pd
from datetime import date

def __json2dict__(json_file):
    '''
    Reads a json file
    Arguments:
        json_file - full name of the json file
    Returns:
        A list of dictionaries
    '''
    with open(json_file,"r") as data:
        return json.load(data)

def __timestamp2date__(time):
    '''
    Converts a timestamp into yyy-mm-dd hh:mm:ss format
    '''
    return date.fromtimestamp(int(time)).strftime("%Y-%m-%d %H:%M:%S.%f")

def __createDict__(ip,total,start,end,direction,events):
    '''
    Creates a dictionary and writes it into a json file
    Arguments:
        file_name = name of the output file.
        ip        = [string] IP from where the event was triggered
        total     = [string] Total time of the events
        start     = [string] Start time and date in yyyy-mm-dd hh:mm:ss format
        end       = [string] End time and date in yyyy-mm-dd hh:mm:ss format
        direction = [list] A list that contains the directions time (up,right,left,down 
                    and idle)
        events    = [string] Number of evets registered
    '''
    up,down,left,right,idle = direction[:5]

    dict = {"events_per_ip":
            [{
                "ip":ip,
                "total_time":"%s"%total,
                "start_time":"%s"%start,
                "end_time":"%s"%end,
                "total_direction_time":{
                    "up":"%s"%up,
                    "down":"%s"%down,
                    "left":"%s"%left,
                    "right":"%s"%right,
                    },
                "total_idle_time":"%s"%idle,
                "total_events":"%s"%events
                }
                ]
            }
    return dict

def main():
    # Path to data
    try:
        data_path = "data"
    except:
        data_path = "../data"

    ### Reads each file as a dataframe and then merge them together into one dataframe
    data_frame_lst = []
    for f in os.listdir(data_path):
        # Full name of the file
        file_name = os.path.join(data_path,f)
        # Loads the file as a dictionary
        dict = __json2dict__(file_name)[0]
        # The directions are separated to make the dictionary flat
        actions = dict["actions"]
        # Removes the actions key from the dictionary
        dict.pop("actions")
        # Creates the new keys
        up = sum([int(d["duration"]) for d in actions if d["direction"] == "up"])
        down = sum([int(d["duration"]) for d in actions if d["direction"] == "down"])
        left = sum([int(d["duration"]) for d in actions if d["direction"] == "left"])
        right = sum([int(d["duration"]) for d in actions if d["direction"] == "right"])
        idle = sum([int(d["duration"]) for d in actions if d["direction"] == "idle"])
        events = len(actions)
        # Appends the new keys to the dictionary
        dict.update({"up":[up],"down":[down],"left":[left],
                     "right":[right],"idle":[idle],
                     "total_events":events
                   })
        # Appends the data frame to a list
        data_frame_lst.append(pd.DataFrame.from_dict(dict))

    # Cocatenates the data frame
    df = pd.concat(data_frame_lst)
    # print(df)

    # Creates a list of unique Ip's
    ip_lst = df.ip.unique()

    # Fills a new dictionary with the information of interest
    dict_lst = []
    for ip in ip_lst:
        ip_df = df.loc[df["ip"] == ip]
        up = ip_df["up"].sum()
        down = ip_df["down"].sum()
        left = ip_df["left"].sum()
        right = ip_df["right"].sum()
        idle = ip_df["idle"].sum()
        directions = [up,down,left,right,idle]
        total_time = sum(directions)
        end_time = __timestamp2date__(int(min(ip_df["timestamp"])) + total_time)
        start_time = __timestamp2date__(min(ip_df["timestamp"]))
        events = ip_df["total_events"].sum()
        dict = __createDict__(ip,total_time,start_time,end_time,directions,events)
        dict_lst.append(dict)
        print(dict,"\n")

    # Creates a new json file with all the output information
    with open("output.json","w") as writer:
        json.dump(dict_lst,writer,indent=4)

    private_ips = ["10.0.1.2","192.168.1.45","172.78.105.90"]

    # Helps to identify the most used direction
    for dict in dict_lst:
        if dict["events_per_ip"][0]["ip"] not in private_ips:
            up    += int(dict["events_per_ip"][0]["total_direction_time"]["up"])
            down  += int(dict["events_per_ip"][0]["total_direction_time"]["down"])
            left  += int(dict["events_per_ip"][0]["total_direction_time"]["left"])
            right += int(dict["events_per_ip"][0]["total_direction_time"]["right"])
            idle  += int(dict["events_per_ip"][0]["total_idle_time"])

    print("up: %s\ndown: %s\nleft: %s\nright:%s\nidle: %s\n"%(up,down,left,right,idle))

if __name__ == "__main__":
    main()
