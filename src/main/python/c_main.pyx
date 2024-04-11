import numpy as np
import multiprocessing as mp
import concurrent.futures

# file = "../../../data/measurements.txt"
def main(file):
    global stations
    stations={}
    N=1000
    c=0
    with open(file, 'r') as f:
        # line=read_line(f)
        line = f.readline()
        n = 256
        with concurrent.futures.ProcessPoolExecutor(max_workers=n) as executor:
            while True:
                line=f.readline()
                # do_work(line)
                executor.submit(do_work, line)
                if c+1 ==N:
                    break
                else:
                    c = c+1
                if not line:
                    break
    print(stations)
    return dict(sorted(stations.items()))


def do_work(line):
    data = line.rstrip().split(';')
    name, temp = str(data[0]), float(data[1])
    if name in stations.keys():
        row=stations[name]
        # [n, min, sum, max, running_avg, running_output]
        n=row[0]+1
        mn=min(row[1],temp)
        mx=max(row[3],temp)
        sm=row[2]+temp
        avg=(row[2]+temp)/(row[0]+1)
        ot=f"{name}={mn}/{avg}/{mx}"
        stations[name] = [n, mn, sm, mx, avg, ot]        
    else:
        stations[name] = [1, temp, temp, temp, temp, '']
    print(stations[name])

# if __name__ =="__main__":
#     main(file)

# def read_line(fh):
#     txt_arr= np.fromfile(fh)
#     # txt_arr = txt_arr[txt_arr != b'\n']
#     return txt_arr

# print("Hello World")
