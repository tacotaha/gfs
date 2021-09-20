#include <iostream>

#include "gfs.h"
#include "gfsclient.h"

file_handle_t* GFSClient::open(const std::string& file_name, fmode_t mode){
    file_handle_t *f = nullptr;
    grpc::ClientContext c;
    gfs::OpenPayload p;
    gfs::FileHandle fh;
    
    p.set_id(this->ip);
    p.set_filename(file_name);
    p.set_mode(mode);

    auto status = this->master->Open(&c, p, &fh);
    if(status.ok())
        f = new file_handle_t(fh.filename(), mode, fh.ptr());

    return f;
}

int GFSClient::write(file_handle_t* fh, const void* buff, size_t nbytes){
    chunk_handle_t *ch = nullptr;
    uint64_t start_chunk = fh->ptr / CHUNK_SIZE;
    uint64_t end_chunk = (fh->ptr + nbytes) / CHUNK_SIZE;

    std::cout << "Write(" << buff << ", " << nbytes << ")" << std::endl;

    for(uint64_t i = start_chunk; i < end_chunk + 1; ++i){
        std::cout << "Requesting Chunk: " << i << "(offset = " << i*64 << ")" << std::endl;
        ch = this->request_chunk(fh->file_name, i*CHUNK_SIZE); 
        std::cout << "Got: ";
        if(ch){
            for(const auto&x : ch->loc)
                std::cout << "<" << x.first << ", " << x.second << "> ";
            std::cout << std::endl;
        }
    }

    return 0;
}

chunk_handle_t* GFSClient::request_chunk(const std::string& file_name, uint64_t offset){
    chunk_handle_t *ch = nullptr;
    grpc::ClientContext c;
    gfs::FileHandle fh;
    gfs::RCResp r;

    fh.set_filename(file_name);
    fh.set_ptr(offset);

    if(this->master->RequestChunk(&c, fh, &r).ok()){
        ch = new(chunk_handle_t);
        ch->cid = r.chunkid();
        for(int i = 0; i < r.loc_size(); ++i)
            ch->loc[i] = std::make_pair(r.loc(i).id(), r.loc(i).primary());
    }

    return ch;
}

int main(int argc, char* argv[]){
    if(argc < 2) {
        std::cerr << "Usage: " << argv[0] << " PORT" << std::endl;
        return 1; 
    }
 
    auto ip_addr = std::string(BASE_IP) + ":" + argv[1];
    GFSClient client(ip_addr);

    file_handle_t *fh = client.open("file.txt", WRITE);
    if(fh){
        std::cout << "File handle: " << fh->file_name << ":" << fh->ptr << std::endl;
    }

    std::string buff("Hello world!");
    client.write(fh, buff.c_str(), buff.size()); 

    return 0;
}
