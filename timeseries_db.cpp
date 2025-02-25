#include "timeseries_db.hpp"
#include <algorithm>
#include <iostream>

TimeSeriesDB::TimeSeriesDB(const std::string& data_dir, const std::string& symbol)
    : timestamps(data_dir, symbol, "timestamps", sizeof(uint64_t)),
      prices(data_dir, symbol, "prices", sizeof(double)),
      volumes(data_dir, symbol, "volumes", sizeof(uint64_t)) {
    
    // Rebuild index from storage
    rebuild_index();
    
    // Start background writer thread
    writer_thread = std::thread(&TimeSeriesDB::writer_loop, this);
}

TimeSeriesDB::~TimeSeriesDB() {
    // Signal writer thread to stop and wait for it
    stop_writer.store(true, std::memory_order_release);
    cv.notify_all();
    if (writer_thread.joinable()) {
        writer_thread.join();
    }
}

void TimeSeriesDB::append(uint64_t timestamp, double price, uint64_t volume) {
    Tick tick{timestamp, price, volume};
    
    // Queue the tick for background writing
    {
        std::lock_guard<std::mutex> lock(write_mutex);
        write_queue.push(tick);
    }
    cv.notify_one();
}

void TimeSeriesDB::append_batch(const std::vector<Tick>& ticks) {
    // Queue all ticks for background writing
    {
        std::lock_guard<std::mutex> lock(write_mutex);
        for (const auto& tick : ticks) {
            write_queue.push(tick);
        }
    }
    cv.notify_one();
}

void TimeSeriesDB::writer_loop() {
    while (!stop_writer.load(std::memory_order_acquire)) {
        std::vector<Tick> batch;
        
        // Get items from queue up to a certain batch size
        {
            std::unique_lock<std::mutex> lock(write_mutex);
            cv.wait(lock, [this] { 
                return !write_queue.empty() || stop_writer.load(std::memory_order_acquire); 
            });
            
            if (stop_writer.load(std::memory_order_acquire) && write_queue.empty()) {
                break;
            }
            
            // Process up to 1000 ticks at once for better batching
            const size_t BATCH_SIZE = 1000;
            for (size_t i = 0; i < BATCH_SIZE && !write_queue.empty(); ++i) {
                batch.push_back(write_queue.front());
                write_queue.pop();
            }
        }
        
        // Process batch
        if (!batch.empty()) {
            // Lock for writing to storage and updating index
            std::unique_lock<std::shared_mutex> lock(query_mutex);
            
            for (const auto& tick : batch) {
                size_t index = timestamps.get_count();
                
                timestamps.append(&tick.timestamp);
                prices.append(&tick.price);
                volumes.append(&tick.volume);
                
                // Update index
                time_index.insert(tick.timestamp, index);
            }
        }
    }
}

void TimeSeriesDB::rebuild_index() {
    std::unique_lock<std::shared_mutex> lock(query_mutex);
    
    size_t count = timestamps.get_count();
    for (size_t i = 0; i < count; ++i) {
        uint64_t ts;
        timestamps.read(i, &ts);
        time_index.insert(ts, i);
    }
}

std::vector<std::tuple<uint64_t, double, uint64_t>> TimeSeriesDB::query_range(uint64_t start, uint64_t end) const {
    std::shared_lock<std::shared_mutex> lock(query_mutex);
    
    // Use B+ Tree for efficient range lookup
    auto results = time_index.range_query(start, end);
    
    std::vector<std::tuple<uint64_t, double, uint64_t>> ticks;
    ticks.reserve(results.size());
    
    for (const auto& [ts, idx] : results) {
        double price;
        uint64_t volume;
        
        // Zero-copy reading from mmap'd files
        prices.read(idx, &price);
        volumes.read(idx, &volume);
        
        ticks.emplace_back(ts, price, volume);
    }
    
    return ticks;
}

std::vector<std::tuple<uint64_t, double, uint64_t>> TimeSeriesDB::query_last(size_t n) const {
    std::shared_lock<std::shared_mutex> lock(query_mutex);
    
    size_t count = timestamps.get_count();
    size_t start_idx = (count > n) ? (count - n) : 0;
    
    std::vector<std::tuple<uint64_t, double, uint64_t>> result;
    result.reserve(count - start_idx);
    
    for (size_t i = start_idx; i < count; ++i) {
        uint64_t ts;
        double price;
        uint64_t volume;
        
        timestamps.read(i, &ts);
        prices.read(i, &price);
        volumes.read(i, &volume);
        
        result.emplace_back(ts, price, volume);
    }
    
    return result;
}

void TimeSeriesDB::sync() {
    // Process all queued writes
    std::unique_lock<std::mutex> lock(write_mutex);
    while (!write_queue.empty()) {
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        lock.lock();
    }
}