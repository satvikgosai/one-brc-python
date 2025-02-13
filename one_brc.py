import concurrent.futures
import os


def read_chunk(file, start, end):
    cities = {}
    with open(file, "rb") as chunk:
        chunk.seek(start)
        while start < end:
            line = chunk.readline()
            start += len(line)
            city, temp = line.split(b";")
            temp = float(temp)
            if found := cities.get(city):
                if temp < found[0]:
                    found[0] = temp
                elif temp > found[1]:
                    found[1] = temp
                found[2] += temp
                found[3] += 1
            else:
                cities[city] = [temp, temp, temp, 1]
    return cities


def main(file):
    max_workers = os.cpu_count()
    total_size = os.path.getsize(file)
    interval = total_size // max_workers
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        with open(file, "rb") as f:
            start = 0
            for i in range(1, max_workers + 1):
                end = i * interval
                if i < max_workers:
                    f.seek(end)
                    while f.read(1) != b"\n":
                        end += 1
                else:
                    end = total_size
                futures.append(executor.submit(read_chunk, file, start, end))
                start = end + 1
        cities = {}
        for future in concurrent.futures.as_completed(futures):
            for city, (min_, max_, sum_, count) in future.result().items():
                if found := cities.get(city):
                    found[0] = min(min_, found[0])
                    found[1] = max(max_, found[1])
                    found[2] += sum_
                    found[3] += count
                else:
                    cities[city] = [min_, max_, sum_, count]
        # Abha=-23.0/18.0/59.2
        print(
            *sorted(
                [
                    f"{city.decode()}={round(min_, 1)}/{round(sum_ / count, 1)}/{round(max_, 1)}"
                    for city, (min_, max_, sum_, count) in cities.items()
                ]
            ),
            sep="\n",
        )
