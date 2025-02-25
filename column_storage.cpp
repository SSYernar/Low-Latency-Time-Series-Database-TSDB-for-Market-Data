#include "column_storage.hpp"

ColumnStorage::ColumnStorage(const std::string& data_dir, const std::string& symbol, 
                             const std::string& column_name, size_t element_size)
    : element_size(element_size) {
    
    // Ensure data directory exists
    ensure_directory_exists(data_dir);
    
    // Create symbol directory if it doesn't exist
    std::string symbol_dir = data_dir + "/" + symbol;
    ensure_directory_exists(symbol_dir);
    
    // Set up filename
    filename = symbol_dir + "/" + column_name + ".bin";
    
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
    count.store(file_size / element_size, std::memory_order_release);
    capacity = count.load(std::memory_order_acquire);

    if (capacity == 0) {
        // Pre-allocate with initial chunk size
        capacity = CHUNK_SIZE / element_size;
        if (capacity == 0) capacity = 1; // Ensure at least one element fits
        
        if (ftruncate(fd, capacity * element_size) == -1) {
            close(fd);
            throw std::system_error(errno, std::generic_category(), "Failed to truncate file " + filename);
        }
    }

    remap();
}

ColumnStorage::~ColumnStorage() {
    if (mapped_data && mapped_data != MAP_FAILED) {
        munmap(mapped_data, capacity * element_size);
    }
    if (fd != -1) {
        close(fd);
    }
}

// Move constructor
ColumnStorage::ColumnStorage(ColumnStorage&& other) noexcept
    : fd(other.fd), 
      element_size(other.element_size), 
      capacity(other.capacity), 
      mapped_data(other.mapped_data),
      filename(std::move(other.filename)) {
    
    count.store(other.count.load(std::memory_order_acquire), std::memory_order_release);
    
    // Reset other's state
    other.fd = -1;
    other.mapped_data = nullptr;
    other.count.store(0, std::memory_order_release);
    other.capacity = 0;
}

// Move assignment
ColumnStorage& ColumnStorage::operator=(ColumnStorage&& other) noexcept {
    if (this != &other) {
        // Clean up current resources
        if (mapped_data && mapped_data != MAP_FAILED) {
            munmap(mapped_data, capacity * element_size);
        }
        if (fd != -1) {
            close(fd);
        }
        
        // Move from other
        fd = other.fd;
        element_size = other.element_size;
        count.store(other.count.load(std::memory_order_acquire), std::memory_order_release);
        capacity = other.capacity;
        mapped_data = other.mapped_data;
        filename = std::move(other.filename);
        
        // Reset other's state
        other.fd = -1;
        other.mapped_data = nullptr;
        other.count.store(0, std::memory_order_release);
        other.capacity = 0;
    }
    return *this;
}

void ColumnStorage::ensure_directory_exists(const std::string& dir_path) {
    std::filesystem::path path(dir_path);
    if (!std::filesystem::exists(path)) {
        std::filesystem::create_directories(path);
    }
}

void ColumnStorage::remap() {
    if (mapped_data && mapped_data != MAP_FAILED) {
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
    size_t current_count = count.load(std::memory_order_acquire);
    
    if (current_count >= capacity) {
        // Lock for remapping, this is a rare operation so it's OK to synchronize
        static std::mutex remap_mutex;
        std::lock_guard<std::mutex> lock(remap_mutex);
        
        // Check again after acquiring lock (another thread might have remapped)
        if (current_count >= capacity) {
            // Grow by chunks rather than doubling for better memory efficiency
            size_t new_capacity = capacity + (CHUNK_SIZE / element_size);
            if (new_capacity <= capacity) new_capacity = capacity * 2; // Fallback if element_size > CHUNK_SIZE
            
            if (ftruncate(fd, new_capacity * element_size) == -1) {
                throw std::system_error(errno, std::generic_category(), "Failed to truncate file " + filename);
            }
            capacity = new_capacity;
            remap();
        }
    }

    // Increment count atomically and get the insertion position
    size_t pos = count.fetch_add(1, std::memory_order_acq_rel);
    char* dest = static_cast<char*>(mapped_data) + (pos * element_size);
    std::memcpy(dest, data, element_size);
    
    // Ensure data is written to disk
    msync(dest, element_size, MS_ASYNC); // Use MS_ASYNC for better performance
}

void ColumnStorage::read(size_t index, void* data) const {
    size_t current_count = count.load(std::memory_order_acquire);
    if (index >= current_count) {
        throw std::out_of_range("Index " + std::to_string(index) + " out of range for count " + std::to_string(current_count));
    }

    const char* src = static_cast<const char*>(mapped_data) + (index * element_size);
    std::memcpy(data, src, element_size);
}