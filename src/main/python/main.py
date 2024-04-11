import os

import threading
import queue
import sys
import multiprocessing as mp
from functools import partial


import sqlite3
from tqdm import tqdm


from collections import defaultdict, Counter
from csv import reader 
from timeit import default_timer as timer


currg = None


stations = None


def attempt_1 (file:str, N:int):

    with open (file, 'r') as f:
        stations = {}
        head = [next(f) for _ in range(N)]
        # for data in f:
        for data in head:
            data_split = data.rstrip().split(";")

            station = data_split[0]
            temp = float(data_split[1])

            if station not in stations.keys():
                stations[station] = {"sum":temp, "n":1, "min":temp, "max":temp}
            else:
                # stations[station]["all"].append(temp)
                stations[station]["sum"] += temp
                stations[station]["n"] += 1
                if temp < stations[station]["min"]:
                    stations[station]["min"] = temp 
                if temp > stations[station]["max"]:
                    stations[station]["max"] = temp
                    # stations[station]["avg"] = stations[station]["sum"] / stations[station]["n"]

        # print(stations)
    sorted_stations = dict(sorted(stations.items()))
    for s in sorted_stations:
        avg = round(stations[s]["sum"]/stations[s]["n"],1)
        # print(f"{s}={stations[s]["min"]}/{avg}/{stations[s]["max"]}")
        pass


def do_work(line:str, stations:dict)->dict:
    data = line.rstrip().split(";")
    station = data[0]
    temp = float(data[1])

    if station not in stations.keys():
        stations[station] = [temp, 1, temp, temp]
    else:
        sums=stations[station][0]+temp
        n=stations[station][1]+1
        if temp < stations[station][2]:
            mins = temp 
        else:
            mins=stations[station][2]
        if temp > stations[station][3]:
            maxs=temp
        else:
            maxs=stations[station][3]

        stations[station] = [sums, n, mins, maxs]

    return stations
        # stations[station]["all"].append(temp) 

def attempt_2(file:str, N:int):
    mins = defaultdict(lambda: float(0))
    maxes = defaultdict(lambda: float(0))
    sums = defaultdict(float)
    stations = Counter()
    results = {}

    with open(file, 'r') as f:
        r = reader(f, delimiter=';')
        i=0
        for row in r:
            name, temp = str(row[0]), float(row[1])
            stations.update([name])
            mins[name] = min(mins[name], temp)
            maxes[name] = max(maxes[name], temp)
            sums[name] += temp
            i+=1
            if i == N:
                break
    for station, num in stations.items():
        avg=sums[station]/num
        results[station]=f"{station}={mins[station]}/{avg}/{maxes[station]}"
    sorted_results =dict(sorted(results.items()))

    for station in sorted_results.values():
        # print(station)
        pass




def do_work_iter(head:list[str], stations:dict):

    for line in head:
        data = line.rstrip().split(";")
        station = data[0]
        temp = float(data[1])

        if station not in stations.keys():
            stations[station] = [temp, 1, temp, temp]
        else:
            sums=stations[station][0]+temp
            n=stations[station][1]+1
            if temp < stations[station][2]:
                mins = temp 
            else:
                mins=stations[station][2]
            if temp > stations[station][3]:
                maxs=temp
            else:
                maxs=stations[station][3]

            stations[station] = [sums, n, mins, maxs]

        return stations



def attempt_3(file:str, N:int):
    with open (file, 'r') as f:
        stations = {}
        results = {}

        head = [next(f) for _ in range(N)]
        # for data in f:
        for data in head:
            do_work(data, stations)
        
        sorted_stations = sorted(stations.items())
        # [sum n min max]
        for s in sorted_stations:
            avg = round(s[1][0]/s[1][1],1)
            mn = s[1][2]
            ma = s[1][3]
            # print(f"{s}={mn}/{avg}/{ma}")


def batch_file(arr, n_workers):
    fl = len(arr)

    batch_size = round(fl/n_workers)
    batches = [
            arr[ix:ix+batch_size]
            for ix in tqdm(range(0, fl, batch_size))
            ]
    return batches


def do_work_batch(batch, stations):
    return[do_work(data, stations) for data in batch]


def attempt_4(file:str, N:int):

    pool = mp.Pool(16)
    with open(file) as f:
        head = [next(f) for _ in range(N)]
        stations={}
        # results={}

        
        head = [next(f) for _ in range(N)]

        # for data in f:

        # do_work_iter(head, stations)
        # 
        # sorted_stations = sorted(stations.items())
        # # [sum n min max]
        # for s in sorted_stations:
        #     avg = round(s[1][0]/s[1][1],1)
        #     mn = s[1][2]
        #     ma = s[1][3]
            # print(f"{s}={mn}/{avg}/{ma}")


        # p= mp.Process(target=do_work_iter, args=(head, stations))
        # p.start()
        # p,join()
        # func = partial(do_work_iter, head, stations)
        # results = pool.map(func, head, 16)
        # print(results)
    #     nextLineByte = f.tell()
    #     for line in iter(f.readline, ''):
    #         pool.apply_async(processWrapper, args=(nextLineByte,f) )
    #         nextLineByte = f.tell()
    # pool.close()
    # pool.join()

def attempt_5(data:list[str]):
    n_workers=16
    batches = batch_file(data,n_workers)
   
    
    db="a5.db"
    curr = init_db(db)

    global currg
    if not currg:
        currg = curr

    # for batch in batches:
    #     insert_rows(batch)

    p = mp.Pool(16)
    p.map(insert_rows, batches)


    print(batches)



def insert_rows(batch):
    for data in batch:
        name, temp = str(data.split(';')[0]), float(data.split(';')[1])
        sql = f"insert into data (name, temp) values('{name}', {temp})"
        currg.execute(sql)


def init_db(db):
    # os.remove(db)    
    conn = sqlite3.connect(db)
    curr = conn.cursor()
    sql="drop table if exists data"
    curr.execute(sql)
    sql= "create table data (name text, temp number);"
    curr.execute(sql)

    return curr


def do_agg(curr):

    sql= """
        select 
            name,
            average(temp) av,
            min(temp) mn,
            max(temp) mx 
        from data
    """


def do_work_2(line:str)->dict:
    data = line.rstrip().split(";")
    station = data[0]
    temp = float(data[1])

    if station not in stations.keys():
        stations[station] = [temp, 1, temp, temp]
    else:
        sums=stations[station][0]+temp
        n=stations[station][1]+1
        if temp < stations[station][2]:
            mins = temp 
        else:
            mins=stations[station][2]
        if temp > stations[station][3]:
            maxs=temp
        else:
            maxs=stations[station][3]

        stations[station] = [sums, n, mins, maxs]

    return stations


def do_work_batch_2(batch):
    return[do_work_2(data) for data in tqdm(batch)]


def attempt_6(data:list[str])->None:

    global stations
    if not stations:
        stations = {}

    batches = batch_file(data,16)

    p = mp.Pool(16)
    results = p.map(do_work_batch_2, batches)

    # print(results)







def get_n_rows(file:str, N:int) -> list[str]:
     with open(file) as f:
        return [next(f) for _ in range(N)]


# def write_out():
#
#     with open

        




if __name__ == "__main__":

    infile = "../../../data/measurements.txt"
    outfile = "../../../output/"
    N = 10
    data = get_n_rows(infile, N)

    start = timer()
    attempt_1(infile, N)
    end = timer()
    print(end-start)
    #
    # start = timer()
    # attempt_2(file, N)
    # end = timer()
    # print(end-start)

    # start = timer()
    # attempt_4(file, N)
    # end = timer()
    # print(end-start)
    #
    # start = timer()
    # attempt_6(data)
    # end = timer()
    # print(end-start)

