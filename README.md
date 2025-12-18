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

Examples:
```bash
make run-uncompressed  # 100% uncompressed
make run-compressed    # 100% compressed
make run-mixed         # 50/50 mixed
```

### Receivers
```bash
sudo ./build/pipeline_receiver
sudo ./build/fast_receiver
```
