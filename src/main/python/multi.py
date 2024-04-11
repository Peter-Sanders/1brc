import sys
import multiprocessing as mp
from tqdm import tqdm
from timeit import default_timer as timer



def do_work(line:str)->dict:
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


def do_work_batch(batch):
    return[do_work(data) for data in tqdm(batch)]


def attempt_6(data:list[str])->None:

    global stations
    if not stations:
        stations = {}

    batches = batch_file(data,16)

    p = mp.Pool(16)
    results = p.map(do_work_batch, batches)


def get_n_rows(file:str, N:int) -> list[str]:
     with open(file) as f:
        return [next(f) for _ in range(N)]


def batch_file(arr, n_workers):
    fl = len(arr)

    batch_size = round(fl/n_workers)
    batches = [
            arr[ix:ix+batch_size]
            for ix in tqdm(range(0, fl, batch_size))
            ]
    return batches


if __name__ == "__main__":
    global stations
    stations = {}

    n = (mp.cpu_count() * 32)

    infile = "../../../data/measurements.txt"
    N = 10000000
    data = get_n_rows(infile, N)

    start = timer()
    batches = batch_file(data, n)
    with mp.Pool(n) as p:
        results = p.map(do_work, data)
        # results = p.map(do_work_batch, batches)
    end = timer()
    print(end-start)
