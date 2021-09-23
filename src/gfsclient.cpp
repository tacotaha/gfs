#include "gfsclient.h"

#include <cassert>
#include <iostream>

#include "gfs.h"

file_handle_t* GFSClient::open(const std::string& file_name, fmode_t mode) {
  file_handle_t* f = nullptr;
  chunk_handle_t* ch = nullptr;
  std::string file_path = this->_get_file_path(file_name);

  grpc::ClientContext c;
  gfs::OpenPayload p;
  gfs::FileHandle fh;

  p.set_id(this->ip);
  p.set_filename(file_path);
  p.set_mode(mode);

  if (this->master->Open(&c, p, &fh).ok()) {
    f = new file_handle_t(file_path, mode, fh.ptr(), this->_new_fh());
    if (f) {
      std::lock_guard<std::mutex> g(this->openfiles_mutex);
      if (this->open_files.find(file_path) == this->open_files.end())
        this->open_files[file_path] = std::vector<uint64_t>();
      this->open_files[file_path].push_back(f->inode);

      std::lock_guard<std::mutex> l(this->buffmap_mutex);
      if (this->buff_map.find(f->inode) == this->buff_map.end())
        this->buff_map[f->inode] = new char[CHUNK_SIZE];
    }

    // prefetch first chunk for reading
    if (mode & READ && (ch = this->request_chunk(file_path, 0))) {
      this->get_chunk(ch, this->buff_map[f->inode]);
      delete ch;
    }
  }

  return f;
}

int GFSClient::remove(const std::string& file_name) {
  std::string file_path = this->_get_file_path(file_name);

  grpc::ClientContext c;
  gfs::FileHandle fh;
  gfs::Status s;

  fh.set_filename(file_path);

  if (this->master->Remove(&c, fh, &s).ok()) {
    std::lock_guard<std::mutex> g(this->openfiles_mutex);
    const auto of_it = this->open_files.find(file_path);
    if (of_it != this->open_files.end()) {
      std::lock_guard<std::mutex> l(this->buffmap_mutex);
      for (const auto& x : of_it->second) {
        const auto bm_it = this->buff_map.find(x);
        if (bm_it != this->buff_map.end()) {
          delete bm_it->second;
          this->buff_map.erase(bm_it);
        }
      }
      this->open_files.erase(of_it);
    }
  }

  return 0;
}

void GFSClient::close(file_handle_t* fh) {
  size_t bleft = 0;
  chunk_handle_t* ch = nullptr;

  if (fh && fh->mode & (WRITE | APPEND) && fh->ptr)
    if ((ch = this->request_chunk(fh->file_name, fh->ptr - 1))) {
      std::lock_guard<std::mutex> g(this->openfiles_mutex);
      const auto of_it = this->open_files.find(fh->file_name);
      if (of_it != this->open_files.end())
        std::remove(of_it->second.begin(), of_it->second.end(), fh->inode);

      std::lock_guard<std::mutex> l(this->buffmap_mutex);
      const auto bm_it = this->buff_map.find(fh->inode);
      if (bm_it != this->buff_map.end()) {
        bleft = (CHUNK_SIZE - (fh->ptr % CHUNK_SIZE));
        if (bleft < CHUNK_SIZE) {
          memset(bm_it->second + (fh->ptr % CHUNK_SIZE), 0, bleft);
          this->flush(ch, bm_it->second);
        }
      }
      delete ch;
    }
}

int GFSClient::write(file_handle_t* fh, const void* buff, size_t nbytes) {
  void* f_buff = nullptr;
  chunk_handle_t* ch = nullptr;
  size_t bytes_left = CHUNK_SIZE - (fh->ptr % CHUNK_SIZE), nwrite = 0;

  if (fh) {
    this->buffmap_mutex.lock();
    if (this->buff_map.find(fh->inode) != this->buff_map.end()) {
      f_buff = this->buff_map[fh->inode];

      // fill & flush remainder of current chunk
      if (nbytes >= bytes_left &&
          (ch = this->request_chunk(fh->file_name, fh->ptr))) {
        memcpy((char*)f_buff + (fh->ptr % CHUNK_SIZE), buff, bytes_left);
        this->flush(ch, f_buff);
        fh->ptr += bytes_left;
        nwrite += bytes_left;
        nbytes -= bytes_left;
        delete ch;
      }

      if (nbytes > 0) {
        // multi-chunk write -> split
        if (nbytes >= CHUNK_SIZE) {
          this->buffmap_mutex.unlock();
          return nwrite + this->write(fh, (char*)buff + nwrite, nbytes);
        }
        memcpy(f_buff, (char*)buff + nwrite, nbytes);
        fh->ptr += nbytes;
        nwrite += nbytes;
      }
    }

    this->buffmap_mutex.unlock();
  }

  return nwrite;
}

int GFSClient::read(file_handle_t* fh, void* buff, size_t nbytes) {
  void* f_buff = nullptr;
  chunk_handle_t* ch = nullptr;
  size_t bytes_left = CHUNK_SIZE - (fh->ptr % CHUNK_SIZE), nread = 0;

  if (fh) {
    this->buffmap_mutex.lock();

    if (this->buff_map.find(fh->inode) != this->buff_map.end()) {
      f_buff = this->buff_map[fh->inode];

      // read remainder of current chunk
      if (nbytes >= bytes_left) {
        memcpy(buff, (char*)f_buff + (fh->ptr % CHUNK_SIZE), bytes_left);
        fh->ptr += bytes_left;
        nread += bytes_left;
        nbytes -= bytes_left;
      }

      if (fh->ptr && fh->ptr % CHUNK_SIZE == 0) {
        ch = this->request_chunk(fh->file_name, fh->ptr);
        this->get_chunk(ch, f_buff);
        delete ch;
      }

      if (nbytes > 0) {
        // multi-chunk read -> split
        if (nbytes >= CHUNK_SIZE) {
          this->buffmap_mutex.unlock();
          return nread + this->read(fh, (char*)buff + nread, nbytes);
        }
        memcpy((char*)buff + nread, f_buff, nbytes);
        fh->ptr += nbytes;
        nread += nbytes;
      }
    }

    this->buffmap_mutex.unlock();
  }

  return nread;
}

int GFSClient::flush(const chunk_handle_t* ch, const void* buff) {
  if (ch && buff) {
    for (const auto& x : ch->loc)
      for (int i = 0; i < RETRY_SEND; ++i)
        if (this->send_chunk(x.first, ch->cid, buff))
          break;
        else if (i == RETRY_SEND - 1)
          return -1;
  }
  return 0;
}

int GFSClient::send_chunk(const std::string& ip_addr, chunkid_t cid,
                          const void* buff) {
  auto c = grpc::CreateChannel(ip_addr, grpc::InsecureChannelCredentials());
  auto client = std::move(gfs::CServer::NewStub(c));

  gfs::Status r;
  gfs::Chunk chunk;
  grpc::ClientContext ctx;

  chunk.set_data(buff, CHUNK_SIZE);
  chunk.mutable_chunk()->set_id(cid);
  chunk.mutable_chunk()->set_checksum(sha256sum(buff, CHUNK_SIZE));

  return client->SendChunk(&ctx, chunk, &r).ok();
}

int GFSClient::get_chunk(const chunk_handle_t* ch, void* buff) {
  if (!ch || !buff) return -1;

  std::string ip_addr = ch->loc[0].first;
  auto c = grpc::CreateChannel(ip_addr, grpc::InsecureChannelCredentials());
  auto client = std::move(gfs::CServer::NewStub(c));

  gfs::Chunk chunk;
  gfs::ChunkID chunk_id;
  grpc::ClientContext ctx;

  chunk_id.set_id(ch->cid);
  auto status = client->GetChunk(&ctx, chunk_id, &chunk);

  if (!status.ok() || chunk.chunk().id() != ch->cid) return -1;

  memcpy(buff, chunk.data().data(), CHUNK_SIZE);
  return sha256sum(buff, CHUNK_SIZE) == chunk.chunk().checksum();
}

chunk_handle_t* GFSClient::request_chunk(const std::string& file_name,
                                         uint64_t offset) {
  chunk_handle_t* ch = nullptr;
  grpc::ClientContext c;
  gfs::FileHandle fh;
  gfs::RCResp r;

  fh.set_filename(file_name);
  fh.set_ptr(offset);

  if (this->master->RequestChunk(&c, fh, &r).ok()) {
    ch = new chunk_handle_t;
    ch->cid = r.chunkid();
    for (int i = 0; i < r.loc_size(); ++i)
      ch->loc[i] = std::make_pair(r.loc(i).id(), r.loc(i).primary());
  }

  return ch;
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " PORT" << std::endl;
    return 1;
  }

  int nwrite, nread;
  file_handle_t* fh = nullptr;
  const int payload_size = (CHUNK_SIZE << 2) + 33;
  std::vector<char> payload(payload_size), p_in(payload_size);
  auto ip_addr = std::string(BASE_IP) + ":" + argv[1];
  GFSClient client(ip_addr);

  std::srand(std::time(0));
  for (int i = 0; i < payload_size; ++i) payload[i] = std::rand();

  if ((fh = client.open("file.txt", WRITE))) {
    nwrite = client.write(fh, payload.data(), payload_size);
    std::cout << "Wrote " << nwrite << " bytes" << std::endl;
    client.close(fh);
  }

  if ((fh = client.open("file.txt", READ))) {
    nread = client.read(fh, &p_in[0], payload_size);
    std::cout << "Read " << nread << " bytes" << std::endl;
    client.close(fh);
  }

  assert(nread == nwrite);
  for (int i = 0; i < payload_size; ++i) assert(payload[i] == p_in[i]);

  client.remove("file.txt");

  return 0;
}
