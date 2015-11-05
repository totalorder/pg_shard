import csv

SHARD_RANGES = [
    (-2147483648, -1610612738),
    (-1610612737, -1073741827),
    (-1073741826, -536870916),
    (-536870915, -5),
    (-4, 536870906),
    (536870907, 1073741817),
    (1073741818, 1610612728),
    (1610612729, 2147483647)
]


def find_shard(hash_value):
    for i in xrange(0, len(SHARD_RANGES)):
        if hash_value <= SHARD_RANGES[i][1]:
            return i

    raise Exception('Oops!')


def create_csv_files():
    shard_output_files = []

    try:
        for i in xrange(0, len(SHARD_RANGES)):
            shard_output_files.append(
                open('/Users/adrian/shards/shard{0}.csv'.format(i), 'w+'))

        with open('/tmp/shards.csv', 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t')
            for row in reader:
                shard = find_shard(int(row[0]))
                shard_output_files[shard].write('\t'.join(row[1:]) + '\n')

    finally:
        for i in xrange(0, len(shard_output_files)):
            shard_output_files[i].close()


create_csv_files()
