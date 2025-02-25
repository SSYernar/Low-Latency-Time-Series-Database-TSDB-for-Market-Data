#ifndef TIMESERIES_DB_HPP
#define TIMESERIES_DB_HPP

#include "column_storage.hpp"
#include "bplus_tree.hpp"
#include <vector>
#include <tuple>
#include <string>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <queue>
#include <functional>

struct Tick {
    uint64_t timestamp;
    double price;
    uint64_t volume;
};

class TimeSeriesDB {
public:
    TimeSeriesDB(const std::string& data_dir, const std::string& symbol);
    
    // Insert a new tick
    void append(uint64_t timestamp, double price, uint64_t volume);
    
    // Batch append for better performance
    void append_batch(const std::vector<Tick>& ticks);
    
    // Query by time range
    std::vector<std::tuple<uint64_t, double, uint64_t>> query_range(uint64_t start, uint64_t end) const;
    
    // Get last N ticks
    std::vector<std::tuple<uint64_t, double, uint64_t>> query_last(size_t n) const;
    
    // Get total count of ticks
    size_t get_count() const { return timestamps.get_count(); }
    
    // Wait for background tasks to complete
    void sync();
    
    ~TimeSeriesDB();
    
private:
    // Columnar storage
    ColumnStorage timestamps;
    ColumnStorage prices;
    ColumnStorage volumes;
    
    // B+ Tree index for efficient time range lookups
    BPlusTree<uint64_t, size_t> time_index;
    
    // Concurrency control
    mutable std::shared_mutex query_mutex;
    
    // Background writer thread
    std::thread writer_thread;
    std::atomic<bool> stop_writer{false};
    std::condition_variable cv;
    std::mutex write_mutex;
    std::queue<Tick> write_queue;
    
    // Worker thread function
    void writer_loop();
    
    // Rebuild index from storage
    void rebuild_index();
};

#endif // TIMESERIES_DB_HPP