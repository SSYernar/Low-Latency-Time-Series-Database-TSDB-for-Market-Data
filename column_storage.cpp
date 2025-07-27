#include "column_storage.hpp"
#include <mutex>
#include <algorithm>

ColumnStorage::ColumnStorage(const std::string &data_dir, const std::string &symbol,
                             const std::string &column_name, size_t element_size)
    : element_size(element_size)
{

    // Ensure data directory exists
    ensure_directory_exists(data_dir);

    // Create symbol directory if it doesn't exist
    std::string symbol_dir = data_dir + "/" + symbol;
    ensure_directory_exists(symbol_dir);

    // Set up filename
    filename = symbol_dir + "/" + column_name + ".bin";

    fd = open(filename.c_str(), O_RDWR | O_CREAT, 0666);
    if (fd == -1)
    {
        throw std::system_error(errno, std::generic_category(), "Failed to open file " + filename);
    }

    struct stat st;
    if (fstat(fd, &st) == -1)
    {
        close(fd);
        throw std::system_error(errno, std::generic_category(), "Failed to stat file " + filename);
    }

    size_t file_size = st.st_size;

    if (file_size == 0)
    {
        // New file - initialize with header and pre-allocate space
        capacity = CHUNK_SIZE / element_size;
        if (capacity == 0)
            capacity = 1; // Ensure at least one element fits

        size_t total_size = HEADER_SIZE + (capacity * element_size);
        if (ftruncate(fd, total_size) == -1)
        {
            close(fd);
            throw std::system_error(errno, std::generic_category(), "Failed to truncate file " + filename);
        }

        count.store(0, std::memory_order_release);
        remap();
        write_header(); // Initialize header with count = 0
    }
    else
    {
        // Existing file - read count from header
        if (file_size < HEADER_SIZE)
        {
            close(fd);
            throw std::runtime_error("Invalid file format: file too small for header");
        }

        capacity = (file_size - HEADER_SIZE) / element_size;
        remap();
        read_header(); // Read actual count from header
    }
}

ColumnStorage::~ColumnStorage()
{
    if (mapped_data && mapped_data != MAP_FAILED)
    {
        // Write header before unmapping to ensure count is persisted
        write_header();
        munmap(mapped_data, HEADER_SIZE + (capacity * element_size));
    }
    if (fd != -1)
    {
        close(fd);
    }
}

// Move constructor
ColumnStorage::ColumnStorage(ColumnStorage &&other) noexcept
    : fd(other.fd),
      element_size(other.element_size),
      capacity(other.capacity),
      mapped_data(other.mapped_data),
      filename(std::move(other.filename))
{

    count.store(other.count.load(std::memory_order_acquire), std::memory_order_release);

    // Reset other's state
    other.fd = -1;
    other.mapped_data = nullptr;
    other.count.store(0, std::memory_order_release);
    other.capacity = 0;
}

// Move assignment
ColumnStorage &ColumnStorage::operator=(ColumnStorage &&other) noexcept
{
    if (this != &other)
    {
        // Clean up current resources
        if (mapped_data && mapped_data != MAP_FAILED)
        {
            write_header();
            munmap(mapped_data, HEADER_SIZE + (capacity * element_size));
        }
        if (fd != -1)
        {
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

void ColumnStorage::ensure_directory_exists(const std::string &dir_path)
{
    std::filesystem::path path(dir_path);
    if (!std::filesystem::exists(path))
    {
        std::filesystem::create_directories(path);
    }
}

void ColumnStorage::remap()
{
    if (mapped_data && mapped_data != MAP_FAILED)
    {
        munmap(mapped_data, HEADER_SIZE + (capacity * element_size));
        mapped_data = nullptr;
    }

    size_t map_size = HEADER_SIZE + (capacity * element_size);
    mapped_data = mmap(nullptr, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapped_data == MAP_FAILED)
    {
        throw std::system_error(errno, std::generic_category(), "mmap failed for file " + filename);
    }
}

void ColumnStorage::write_header()
{
    if (mapped_data && mapped_data != MAP_FAILED)
    {
        size_t current_count = count.load(std::memory_order_acquire);
        std::memcpy(mapped_data, &current_count, HEADER_SIZE);
        msync(mapped_data, HEADER_SIZE, MS_ASYNC);
    }
}

void ColumnStorage::read_header()
{
    if (mapped_data && mapped_data != MAP_FAILED)
    {
        size_t stored_count;
        std::memcpy(&stored_count, mapped_data, HEADER_SIZE);
        count.store(stored_count, std::memory_order_release);
    }
}

void ColumnStorage::append(const void *data)
{
    size_t current_count = count.load(std::memory_order_acquire);

    if (current_count >= capacity)
    {
        // Lock for remapping, this is a rare operation so it's ok to synchronize
        static std::mutex remap_mutex;
        std::lock_guard<std::mutex> lock(remap_mutex);

        // Check again after acquiring lock (another thread might have remapped)
        if (current_count >= capacity)
        {
            // Grow by chunks rather than doubling for better memory efficiency
            size_t new_capacity = capacity + (CHUNK_SIZE / element_size);
            if (new_capacity <= capacity)
                new_capacity = capacity * 2; // Fallback if element_size > CHUNK_SIZE

            size_t new_total_size = HEADER_SIZE + (new_capacity * element_size);
            if (ftruncate(fd, new_total_size) == -1)
            {
                throw std::system_error(errno, std::generic_category(), "Failed to truncate file " + filename);
            }
            capacity = new_capacity;
            remap();
        }
    }

    // Increment count atomically and get the insertion position
    size_t pos = count.fetch_add(1, std::memory_order_acq_rel);
    char *dest = static_cast<char *>(mapped_data) + HEADER_SIZE + (pos * element_size);
    std::memcpy(dest, data, element_size);

    msync(dest, element_size, MS_ASYNC); // Use MS_ASYNC for better performance
}

void ColumnStorage::append_batch(const void *data, size_t batch_count)
{
    if (batch_count == 0)
        return;

    size_t current_count = count.load(std::memory_order_acquire);

    // Check if we need to grow capacity
    if (current_count + batch_count > capacity)
    {
        // Lock for remapping
        static std::mutex remap_mutex;
        std::lock_guard<std::mutex> lock(remap_mutex);

        // Check again after acquiring lock
        current_count = count.load(std::memory_order_acquire);
        if (current_count + batch_count > capacity)
        {
            // Grow capacity to accommodate the batch
            size_t needed_capacity = current_count + batch_count;
            size_t new_capacity = capacity;
            while (new_capacity < needed_capacity)
            {
                new_capacity += (CHUNK_SIZE / element_size);
                if (new_capacity <= capacity)
                    new_capacity = capacity * 2; // Fallback
            }

            size_t new_total_size = HEADER_SIZE + (new_capacity * element_size);
            if (ftruncate(fd, new_total_size) == -1)
            {
                throw std::system_error(errno, std::generic_category(), "Failed to truncate file " + filename);
            }
            capacity = new_capacity;
            remap();
        }
    }

    // Get the starting position and increment count atomically
    size_t start_pos = count.fetch_add(batch_count, std::memory_order_acq_rel);

    // Copy all data at once
    char *dest = static_cast<char *>(mapped_data) + HEADER_SIZE + (start_pos * element_size);
    std::memcpy(dest, data, batch_count * element_size);

    // Sync the written data
    msync(dest, batch_count * element_size, MS_ASYNC);
}

void ColumnStorage::flush_header()
{
    write_header();
}

void ColumnStorage::read(size_t index, void *data) const
{
    size_t current_count = count.load(std::memory_order_acquire);
    if (index >= current_count)
    {
        throw std::out_of_range("Index " + std::to_string(index) + " out of range for count " + std::to_string(current_count));
    }

    const char *src = static_cast<const char *>(mapped_data) + HEADER_SIZE + (index * element_size);
    std::memcpy(data, src, element_size);
}