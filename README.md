## Wersja pythona:
python >= 3.10

## Instalacja zależności

pip install -r ./requirements

## Uruchomienie programu
### Master

python ./communication/master_node.py

### Worker

python ./communication/worker_node.py

### Uruchomienie map reduce

Gdzie uruchomiony jest master, wpisz "mr". Uruchomi to map reduce
z funkcjami zadeklarowanymi w plikach `files/map.py` oraz `files/reduce.py`
z danymi z pliku `files/words.txt`.