from mapreduce.map_task import MapTask

map_f = """
for w in value.split():
    emit(w, '1')
"""

t = MapTask(map_f)
t.call("file", "v v b a c a")

print(t.results)
