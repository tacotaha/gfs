# gfs

## To Build:
1. Install deps (add path to cmake prefix path)
2. `git clone https://github.com/tacotaha/gfs && cd gfs && mkdir -p build && cd build && cmake ../ && make`

## Deps
* grpc (https://github.com/grpc/grpc)
* protobufs (https://github.com/protocolbuffers/protobuf)

## Useful links
https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf

## TODO
* implement global transaction log for crash recovery
* periodically generate snapshots for checkpointing
* pipeline writes for better throughput
