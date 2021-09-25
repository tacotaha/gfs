#include "cserver.h"

#include <grpcpp/grpcpp.h>
#include <openssl/sha.h>

#include <chrono>
#include <cstdio>
#include <fstream>

#include "gfs.h"

CServer::CServer(const std::string& ip)
    : IP(ip),
      tids(std::vector<std::thread>()),
      chunks(std::map<chunkid_t, std::string>()),
      leases(std::map<chunkid_t, time_t>()) {
  this->server_id = sha256sum(ip.data(), ip.size());
  this->chunk_dir = std::filesystem::path(CHUNK_DIR) / server_id;
  std::filesystem::create_directories(this->chunk_dir);
  this->master = std::move(gfs_master_connect());
  this->tids.push_back(std::thread{&CServer::heartbeat, this});
  this->tids.push_back(std::thread{&CServer::chunk_report, this});
}

CServer::~CServer() {
  for (size_t i = 0; i < this->tids.size(); ++i) this->tids[i].join();
}

grpc::Status CServer::SendChunk(grpc::ServerContext* c, const gfs::Chunk* chunk,
                                gfs::Status* r) {
  std::string checksum;
  chunkid_t cid = chunk->chunk().id();

  r->set_status(0);
  checksum = sha256sum(chunk->data().data(), CHUNK_SIZE);

  std::cout << "SendChunk(" << cid << ", " << checksum << ")" << std::endl;

  if (checksum != chunk->chunk().checksum()) {
    std::cerr << "Invalid checksum" << std::endl;
    return grpc::Status::CANCELLED;
  }

  std::lock_guard<std::mutex> g(this->chunks_mutex);
  const auto found_it = this->chunks.find(cid);

  // new chunk -> write to disk
  if (found_it == this->chunks.end() || found_it->second != checksum) {
    if (!this->write_chunk(chunk)) {
      std::cerr << "Local write failed" << std::endl;
      return grpc::Status::CANCELLED;
    }
    this->chunks[cid] = checksum;
  }

  r->set_status(1);
  return grpc::Status::OK;
}

int CServer::get_chunk(chunkid_t cid, void* buff) {
  bool ret = false;
  std::ifstream chunk;
  std::string file_path = this->_get_path(cid);

  std::lock_guard<std::mutex> g(this->chunks_mutex);
  if (this->chunks.find(cid) != this->chunks.end()) {
    chunk.open(file_path, std::ifstream::binary);
    chunk.read((char*)buff, CHUNK_SIZE);
    ret = (chunk && chunk.gcount() == CHUNK_SIZE);
    chunk.close();
  }

  return ret;
}

grpc::Status CServer::GetChunk(grpc::ServerContext* c, const gfs::ChunkID* cid,
                               gfs::Chunk* chunk) {
  char buff[CHUNK_SIZE];
  chunkid_t id = cid->id();

  std::cout << "GetChunk(" << cid << ")" << std::endl;

  if (!this->get_chunk(id, buff)) return grpc::Status::CANCELLED;

  chunk->set_data(buff, CHUNK_SIZE);
  chunk->mutable_chunk()->set_id(id);
  chunk->mutable_chunk()->set_checksum(sha256sum(buff, CHUNK_SIZE));

  return grpc::Status::OK;
}

grpc::Status CServer::RemoveChunk(grpc::ServerContext* c,
                                  const gfs::ChunkID* id, gfs::Status* chunk) {
  std::cout << "RemoveChunk(" << id->id() << ")" << std::endl;
  chunkid_t cid = id->id();

  std::lock_guard<std::mutex> g(this->chunks_mutex);
  this->chunks.erase(cid);

  if (!std::filesystem::remove(this->_get_path(cid)))
    return grpc::Status::CANCELLED;

  return grpc::Status::OK;
}

grpc::Status CServer::NewChunk(grpc::ServerContext* c, const gfs::ChunkID* p,
                               gfs::Status* s) {
  std::cout << "NewChunk(" << p->id() << ")" << std::endl;

  std::lock_guard<std::mutex> g(this->chunks_mutex);

  if (this->chunks.find(p->id()) != this->chunks.end())
    return grpc::Status::CANCELLED;

  this->chunks[p->id()] = std::string("");

  s->set_status(1);
  return grpc::Status::OK;
}

grpc::Status CServer::SetLease(grpc::ServerContext* c, const gfs::Lease* l,
                               gfs::Status* s) {
  std::cout << "SetLease(" << l->chunkid() << ", " << l->timestamp() << ")"
            << std::endl;
  std::lock_guard<std::mutex> g(this->leases_mutex);
  this->leases[l->chunkid()] = (time_t)l->timestamp();
  return grpc::Status::OK;
}

grpc::Status CServer::RevokeLease(grpc::ServerContext* c, const gfs::ChunkID* p,
                                  gfs::Status* s) {
  std::cout << "RevokeLease(" << p->id() << ")" << std::endl;
  std::lock_guard<std::mutex> g(this->leases_mutex);
  this->leases.erase(p->id());
  return grpc::Status::OK;
}

int CServer::extend_lease(chunkid_t cid) {
  grpc::ClientContext c;
  gfs::ELPayload p;
  gfs::Lease l;

  p.set_id(this->IP);
  p.set_chunkid(cid);

  auto status = this->master->ExtendLease(&c, p, &l);

  if (status.ok()) {
    std::lock_guard<std::mutex> g(this->leases_mutex);
    this->leases[l.chunkid()] = l.timestamp();
  }

  return status.ok();
}

gfs::Chunk CServer::new_chunk(chunkid_t id) {
  char data[CHUNK_SIZE] = {0};
  gfs::Chunk c;
  c.set_data(data, CHUNK_SIZE);
  c.mutable_chunk()->set_id(id);
  c.mutable_chunk()->set_checksum(sha256sum(data, CHUNK_SIZE));
  return c;
}

int CServer::write_chunk(const gfs::Chunk* c) {
  std::cout << "write_chunk(" << c->chunk().id() << ")" << std::endl;
  auto file_path = this->_get_path(c->chunk().id());
  std::ofstream chunk_file(file_path, std::ios::out | std::ios::binary);
  if (chunk_file) {
    chunk_file.write(c->data().c_str(), c->data().length());
    chunk_file.close();
  }
  return chunk_file && chunk_file.good();
}

int CServer::send_heartbeat(gfs::Status* r) {
  grpc::ClientContext c;
  gfs::HBPayload p;

  p.set_id(this->IP);
  this->chunks_mutex.lock();
  p.set_numchunks(this->chunks.size());
  this->chunks_mutex.unlock();

  auto status = this->master->HeartBeat(&c, p, r);

  if (!status.ok())
    std::cout << status.error_code() << ": " << status.error_message()
              << std::endl;

  return status.ok();
}

int CServer::send_chunk_report(gfs::Status* r) {
  grpc::ClientContext c;
  gfs::CRPayload p;

  p.set_id(this->IP);

  std::cout << "Chunk Report: ";

  this->chunks_mutex.lock();
  for (const auto& [chunk, checksum] : this->chunks) {
    std::cout << chunk << " ";
    p.add_chunks(chunk);
  }

  std::cout << std::endl;

  this->chunks_mutex.unlock();

  std::cout << "Lease: ";
  for (const auto& [chunk, ts] : this->leases)
    std::cout << chunk << "(" << ts << ")"
              << " ";
  std::cout << std::endl;

  return this->master->ChunkReport(&c, p, r).ok();
}

void CServer::heartbeat() {
  gfs::Status r;
  while (1) {
    if (!this->send_heartbeat(&r)) std::cerr << "Heartbeat error" << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(HB_INTERVAL_MS));
  }
}

void CServer::chunk_report() {
  gfs::Status r;
  while (1) {
    if (!this->send_chunk_report(&r))
      std::cerr << "Chunk report error" << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(CR_INTERVAL_MS));
  }
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
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
