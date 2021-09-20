#include <chrono>
#include<fstream>
#include <openssl/sha.h>
#include <grpcpp/grpcpp.h>

#include "cserver.h"
#include "gfs.h"

CServer::CServer(const std::string& ip) : IP(ip), chunks(std::map<chunkid_t, chunkserver_t>()){
    this->master = std::move(gfs_master_connect());
    this->hb_tid = std::thread{&CServer::heartbeat, this};
}

CServer::~CServer() {
    this->hb_tid.join();
}

grpc::Status CServer::SendChunk(grpc::ServerContext *c, const gfs::Chunk *chunk, gfs::Status *r){
    std::string checksum;
    chunkid_t cid = chunk->id();

    r->set_status(0);
    checksum = sha256sum(chunk->data().data(), CHUNK_SIZE);

    if(checksum != chunk->checksum())
        return grpc::Status::CANCELLED;

    std::lock_guard<std::mutex> g(this->chunks_mutex);
    const auto found_it = this->chunks.find(cid);

    // new chunk -> write to disk
    if(found_it == this->chunks.end() || found_it->second.first != checksum){
        if(!this->write_chunk(chunk))
            return grpc::Status::CANCELLED;
        this->chunks[cid] = std::make_pair(checksum, 0);
    }

    r->set_status(1);
    return grpc::Status::OK;
}

grpc::Status CServer::NewChunk(grpc::ServerContext* c, const gfs::NCPayload* p, gfs::Status* s){
    std::lock_guard<std::mutex> g(this->chunks_mutex);
    chunkid_t id = p->id();

    if(this->chunks.find(id) != this->chunks.end())
        return grpc::Status::CANCELLED;

    this->chunks[id] = std::make_pair(std::string(""), p->primary());

    s->set_status(1);
    return grpc::Status::OK;
}

gfs::Chunk CServer::new_chunk(chunkid_t id){
    char data[CHUNK_SIZE] = {0};
    gfs::Chunk c;
    c.set_id(id);
    c.set_data(data, CHUNK_SIZE);
    c.set_checksum(sha256sum(data, CHUNK_SIZE));
    return c;
}

int CServer::write_chunk(const gfs::Chunk *c) {
    auto file_path = std::string(CHUNK_DIR) + "/" + std::to_string(c->id());
    std::ofstream chunk_file(file_path, std::ios::out | std::ios::binary);
    if(chunk_file){
        chunk_file.write(c->data().c_str(), c->data().length());
        chunk_file.close();
    }
    return chunk_file && chunk_file.good();
}

int CServer::send_heartbeat(gfs::Status *r){
    grpc::ClientContext c;
    gfs::HBPayload p;

    p.set_id(this->IP);
    this->chunks_mutex.lock();
    p.set_numchunks(this->chunks.size());
    this->chunks_mutex.unlock();

    auto status = this->master->HeartBeat(&c, p, r);

    if(!status.ok())
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;

    return status.ok();
}

void CServer::heartbeat() {
    int res;
    gfs::Status r;

    while(1) {
        res = this->send_heartbeat(&r);
        if(!res)
            std::cerr << "Heartbeat error" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(HB_INTERVAL_MS));
    }
}

int main(int argc, char* argv[]){ 
    if(argc < 2) {
        std::cerr << "Usage: " << argv[0] << " PORT" << std::endl;
        return 1; 
    }

    auto ip_addr = std::string(BASE_IP) + ":" + argv[1];
    CServer c(ip_addr);
    grpc::ServerBuilder builder;

    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();

    builder.AddListeningPort(ip_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&c);

    std::cout << "Chunk server listening on: " << ip_addr << std::endl;
    std::unique_ptr<grpc::Server> s(builder.BuildAndStart());
    s->Wait();

    return 0;
}
