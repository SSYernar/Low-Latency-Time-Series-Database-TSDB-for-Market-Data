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
#include <atomic>
#include <filesystem>

class ColumnStorage {
public:
    ColumnStorage(const std::string& data_dir, const std::string& symbol, const std::string& column_name, size_t element_size);
    ~ColumnStorage();

    // Disable copy to avoid double memory mapping
    ColumnStorage(const ColumnStorage&) = delete;
    ColumnStorage& operator=(const ColumnStorage&) = delete;

    // Allow move semantics
    ColumnStorage(ColumnStorage&& other) noexcept;
    ColumnStorage& operator=(ColumnStorage&& other) noexcept;

    void append(const void* data);
    void read(size_t index, void* data) const;
    size_t get_count() const { return count.load(std::memory_order_acquire); }
    const std::string& get_filename() const { return filename; }

private:
    void remap();
    void ensure_directory_exists(const std::string& dir_path);

    int fd = -1;
    size_t element_size;
    std::atomic<size_t> count{0};
    size_t capacity = 0;
    void* mapped_data = nullptr;
    std::string filename;
    const size_t CHUNK_SIZE = 4096;  // 4KB chunks for efficient file growth
};

#endif // COLUMN_STORAGE_HPP