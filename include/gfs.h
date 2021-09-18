#ifndef GFS_H
#define GFS_H 

#define BASE_IP "127.0.0.1"
#define BASE_PORT "8000"
#define CHUNK_DIR ".chunks"

#define HB_INTERVAL_MS (2 * 1000)
#define CHUNK_SIZE (1 << 6)
#define RFACTOR 3

typedef enum GFS_OP{
    GFS_CREATE = 0,
    GFS_DELETE,
    GFS_OPEN,
    GFS_CLOSE,
    GFS_READ,
    GFS_WRITE
} GFS_OP;

typedef enum MODE{
   READ = 0,
   WRITE
}MODE;

typedef uint64_t chunkid_t;

struct file_handle_t {
    std::string name;
    uint64_t offset; // multiple of chunk size
};

struct chunk_handle_t {
    chunkid_t cid;
    std::string loc[RFACTOR];
};

#endif /* GFS_H */
