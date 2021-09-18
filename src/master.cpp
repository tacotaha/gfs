#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <mutex>

#include "master.h"

GFSMaster::GFSMaster(const std::string &ip) : IP(ip){
    this->chunks = std::map<std::string, chunkset_t>();
    this->chunk_servers = std::map<std::string, std::time_t>();
    this->chunk_locs = std::map<chunkid_t, std::set<chunkserver_t>>();
    this->chunk_ctr = 0;
    
    if(mkdir(CHUNK_DIR, 0777) < 0){
        std::cerr << "Unable to create chunk dir" << std::endl;
    }
}

grpc::Status GFSMaster::HeartBeat(grpc::ServerContext *ct, const gfs::HBPayload *p, 
                                  gfs::HBResp* r){
    time_t ts = std::time(0);
    std::cout << ts << ": Got Heart Beat from: " << p->id() << std::endl;
    
    std::lock_guard<std::mutex> g(this->chunkservers_mutex);
    this->chunk_servers[p->id()] = ts; 

    r->set_success(true);
    return grpc::Status::OK;
}

grpc::Status GFSMaster::Open(grpc::ServerContext *ct, const gfs::OpenPayload *p, 
                             gfs::OpenResp* r){
    bool exists;
    chunkid_t cid = 0;
    MODE mode = (MODE) p->mode();
    std::string file_name = p->filename();

    this->chunks_mutex.lock();
    const auto c_it = this->chunks.find(file_name);
    exists = (c_it == this->chunks.end());
    this->chunks_mutex.unlock();

    if(!exists && mode == READ)
        return grpc::Status::CANCELLED;

    cid = exists ? c_it->second.at(0) : this->new_chunk(file_name);

    this->chunklocs_mutex.lock();
    const auto l_it = this->chunk_locs.find(cid);
    exists = (l_it == this->chunk_locs.end());
    this->chunklocs_mutex.unlock();
   
    return grpc::Status::OK;
}

chunkid_t GFSMaster::new_chunkid(){
    std::lock_guard<std::mutex> g(this->chunk_ctr_mutex); 
    return this->chunk_ctr++;
}

chunkid_t GFSMaster::new_chunk(const std::string& fname){
    auto cid = this->new_chunkid();
    std::lock_guard<std::mutex> g(this->chunks_mutex); 

    const auto &it = this->chunks.find(fname);
    if(it == this->chunks.end())
        this->chunks[fname] = chunkset_t();

    this->chunks[fname].push_back(cid);
    
    return cid;
}

int main(){
    auto master_ip = std::string(BASE_IP) + ":" + BASE_PORT;

    GFSMaster service(master_ip);
    grpc::ServerBuilder builder;

    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
     
    builder.AddListeningPort(master_ip, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::cout << "Master server listening on: " << master_ip << std::endl;
    std::unique_ptr<grpc::Server> s(builder.BuildAndStart());

    s->Wait();

    return 0;
}
