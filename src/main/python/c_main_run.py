import pyximport; pyximport.install()
import c_main
from timeit import default_timer as timer


file = "../../../data/measurements.txt"

start=timer()
res=c_main.main(file)
print(res)
print(timer()-start)
