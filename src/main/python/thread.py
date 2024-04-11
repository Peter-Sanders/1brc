import sys
import multiprocessing as mp
import threading
from tqdm import tqdm
from timeit import default_timer as timer
import concurrent.futures



def do_work(line:str)->dict:
    data = line.rstrip().split(";")
    station = data[0]
    temp = float(data[1])

    if station not in stations.keys():
        stations[station] = [temp, 1, temp, temp]
    else:
        sums=stations[station][0]+temp
        n=stations[station][1]+1
        mins = min(temp,stations[station][2])
        maxs=max(temp,stations[station][3])
        stations[station] = [sums, n, mins, maxs]

    return stations


def do_work_batch(batch):
    return[do_work(data) for data in tqdm(batch)]


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
    start = timer()
    global stations
    stations = {}

    n = mp.cpu_count() * 8

    infile = "../../../data/measurements.txt"
    N = 10000000
    data = get_n_rows(infile, N)

    
    batches = batch_file(data, n)
    print(f"Batch Data: {timer()-start}")

    pstart = timer()
    with concurrent.futures.ProcessPoolExecutor(max_workers=n) as executor:
        executor.map(do_work_batch, batches)
    # with mp.Pool(n) as p:
    #     results = p.map(do_work_batch, batches)
    # for batch in tqdm(batches):
    #     t = threading.Thread(target=do_work_batch, args=(batch,))
    #     t.start()
    #     t.join()
    print(f"Parse Data: {timer()-pstart}")
    print(f"TOTAL: {timer()-start}")


