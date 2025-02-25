#include "column_storage.hpp"

ColumnStorage::ColumnStorage(const std::string& filename, size_t element_size)
    : filename(filename), element_size(element_size) {
    fd = open(filename.c_str(), O_RDWR | O_CREAT, 0666);
    if (fd == -1) {
        throw std::system_error(errno, std::generic_category(), "Failed to open file " + filename);
    }

    struct stat st;
    if (fstat(fd, &st) == -1) {
        close(fd);
        throw std::system_error(errno, std::generic_category(), "Failed to stat file " + filename);
    }

    size_t file_size = st.st_size;
    count = file_size / element_size;
    capacity = count;

    if (capacity == 0) {
        capacity = 1;
        if (ftruncate(fd, capacity * element_size) == -1) {
            close(fd);
            throw std::system_error(errno, std::generic_category(), "Failed to truncate file " + filename);
        }
    }

    remap();
}

ColumnStorage::~ColumnStorage() {
    if (mapped_data) {
        munmap(mapped_data, capacity * element_size);
    }
    if (fd != -1) {
        close(fd);
    }
}

void ColumnStorage::remap() {
    if (mapped_data) {
        munmap(mapped_data, capacity * element_size);
        mapped_data = nullptr;
    }

    size_t map_size = capacity * element_size;
    mapped_data = mmap(nullptr, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapped_data == MAP_FAILED) {
        throw std::system_error(errno, std::generic_category(), "mmap failed for file " + filename);
    }
}

void ColumnStorage::append(const void* data) {
    if (count >= capacity) {
        capacity = capacity == 0 ? 1 : capacity * 2;
        if (ftruncate(fd, capacity * element_size) == -1) {
            throw std::system_error(errno, std::generic_category(), "Failed to truncate file " + filename);
        }
        remap();
    }

    char* dest = static_cast<char*>(mapped_data) + (count * element_size);
    std::memcpy(dest, data, element_size);
    ++count;
    msync(dest, element_size, MS_SYNC);
}

void ColumnStorage::read(size_t index, void* data) const {
    if (index >= count) {
        throw std::out_of_range("Index " + std::to_string(index) + " out of range for count " + std::to_string(count));
    }

    const char* src = static_cast<const char*>(mapped_data) + (index * element_size);
    std::memcpy(data, src, element_size);
}