# Compression Network Sender/Receiver

High-performance network data transfer with Intel QAT hardware-accelerated compression.


## Building
```bash
make all
```

Individual targets:
```bash
make sender    # Build sender only
make pipeline  # Build pipeline receiver only
make fast      # Build fast receiver only
```

## Usage

### Sender
```bash
sudo ./build/qat_sender -f <fraction> -l <level>
```

Options:
- `-f <fraction>`: Compression fraction (0.0 = all uncompressed, 1.0 = all compressed)
- `-l <level>`: Compression level (1-9, higher = better compression but slower)
- `-i <bencmark file>`: Which Benchmark file to use form Squash Benchmarks (1-5)
- `-d`: to enable debug mode

Squash Benchmarks: 
- Download from: https://quixdb.github.io/squash-benchmark/ (change line in `map_file_number` function to point to file donwload dir)
- Argument to Filename Mapping:
    1: geo.protodata
    2: nci
    3: ptt5
    4: sum
    5: xml

Examples:
```bash
make run-uncompressed  # 100% uncompressed
make run-compressed    # 100% compressed
make run-mixed         # 50/50 mixed
```

### Receivers
```bash
sudo ./build/qat_reciever
sudo ./build/fast_receiver
```
