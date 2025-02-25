#ifndef COLUMN_STORAGE_HPP
#define COLUMN_STORAGE_HPP

#include <string>
#include <stdexcept>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <system_error>
#include <cstring>

class ColumnStorage {
public:
    ColumnStorage(const std::string& filename, size_t element_size);
    ~ColumnStorage();

    void append(const void* data);
    void read(size_t index, void* data) const;
    size_t get_count() const { return count; }

private:
    void remap();

    int fd = -1;
    size_t element_size;
    size_t count = 0;
    size_t capacity = 0;
    void* mapped_data = nullptr;
    std::string filename;
};

#endif // COLUMN_STORAGE_HPP