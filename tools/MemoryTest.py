# in order to use less RAM I thought about normalizing the dictionaries used in ProcessFile.py
# instead of storing the domain and keywords repeatedly, I could use a mirrored dictionary
# to map unique domains and keywords to an integer alternative. This was a test of that RAM usage
import sys

# not an accurate representation of true memory used, but enough to prove the concept
def sum_dict(dict):
    total = 0
    for key, value in dict.items():
        total += sys.getsizeof(key)
        total += sys.getsizeof(value)
    
    total += sys.getsizeof(dict)
    return total


mirrordict = dict()
datadict = dict()

mirrordict['Purple Ipod Touch'] = 1
mirrordict[1] = "Purple Ipod Touch"

for i in range(50):
    datadict[i] = "1"

print(f"Mirror Total Size: {sum_dict(mirrordict) + sum_dict(datadict)}")

datadict2 = dict()
for i in range(50):
    datadict2[i] = f"Purple Ipod Touch"

print(f"Standard Total Size: {sum_dict(datadict2)}")
