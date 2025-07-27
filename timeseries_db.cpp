#include "timeseries_db.hpp"
#include <algorithm>
#include <iostream>

TimeSeriesDB::TimeSeriesDB(const std::string &data_dir, const std::string &symbol)
    : timestamps(data_dir, symbol, "timestamps", sizeof(uint64_t)),
      prices(data_dir, symbol, "prices", sizeof(double)),
      volumes(data_dir, symbol, "volumes", sizeof(uint64_t))
{

    // Rebuild index from storage
    rebuild_index();

    // Start background writer thread
    writer_thread = std::thread(&TimeSeriesDB::writer_loop, this);
}

TimeSeriesDB::~TimeSeriesDB()
{
    // Signal writer thread to stop and wait for it
    stop_writer.store(true, std::memory_order_release);
    cv.notify_all();
    if (writer_thread.joinable())
    {
        writer_thread.join();
    }
}

void TimeSeriesDB::append(uint64_t timestamp, double price, uint64_t volume)
{
    Tick tick{timestamp, price, volume};

    // Increment pending writes counter
    pending_writes.fetch_add(1, std::memory_order_acq_rel);

    // Queue the tick for background writing
    {
        std::lock_guard<std::mutex> lock(write_mutex);
        write_queue.push(tick);
    }
    cv.notify_one();
}

void TimeSeriesDB::append_batch(const std::vector<Tick> &ticks)
{
    // Increment pending writes counter by the number of ticks
    pending_writes.fetch_add(ticks.size(), std::memory_order_acq_rel);

    // Queue all ticks for background writing
    {
        std::lock_guard<std::mutex> lock(write_mutex);
        for (const auto &tick : ticks)
        {
            write_queue.push(tick);
        }
    }
    cv.notify_one();
}

void TimeSeriesDB::writer_loop()
{
    while (!stop_writer.load(std::memory_order_acquire))
    {
        std::vector<Tick> batch;

        // Get items from queue up to a certain batch size
        {
            std::unique_lock<std::mutex> lock(write_mutex);
            cv.wait(lock, [this]
                    { return !write_queue.empty() || stop_writer.load(std::memory_order_acquire); });

            if (stop_writer.load(std::memory_order_acquire) && write_queue.empty())
            {
                break;
            }

            // Process up to 1000 ticks at once for better batching
            const size_t BATCH_SIZE = 1000;
            for (size_t i = 0; i < BATCH_SIZE && !write_queue.empty(); ++i)
            {
                batch.push_back(write_queue.front());
                write_queue.pop();
            }
        }

        // Process batch - ensure all columns stay synchronized
        if (!batch.empty())
        {
            // Lock for writing to storage and updating index
            std::unique_lock<std::shared_mutex> lock(query_mutex);

            // Get the starting index before any appends to ensure consistency
            size_t start_index = timestamps.get_count();

            // Prepare data arrays for batch operations
            std::vector<uint64_t> ts_data;
            std::vector<double> price_data;
            std::vector<uint64_t> vol_data;

            ts_data.reserve(batch.size());
            price_data.reserve(batch.size());
            vol_data.reserve(batch.size());

            for (const auto &tick : batch)
            {
                ts_data.push_back(tick.timestamp);
                price_data.push_back(tick.price);
                vol_data.push_back(tick.volume);
            }

            // Batch append to all columns atomically
            if (batch.size() == 1)
            {
                // Single item - use regular append
                timestamps.append(&ts_data[0]);
                prices.append(&price_data[0]);
                volumes.append(&vol_data[0]);
            }
            else
            {
                // Multiple items - use batch append for better performance
                timestamps.append_batch(ts_data.data(), batch.size());
                prices.append_batch(price_data.data(), batch.size());
                volumes.append_batch(vol_data.data(), batch.size());
            }

            // Flush headers to ensure persistence
            timestamps.flush_header();
            prices.flush_header();
            volumes.flush_header();

            // Update time index for all items
            for (size_t i = 0; i < batch.size(); ++i)
            {
                time_index.insert(batch[i].timestamp, start_index + i);
            }

            // Verify synchronization (debug check)
            if (!verify_column_sync())
            {
                std::cerr << "ERROR: Columns became desynchronized during batch write!" << std::endl;
            }

            // Decrement pending writes counter by the number of processed ticks
            pending_writes.fetch_sub(batch.size(), std::memory_order_acq_rel);

            // Notify sync if it's waiting and no more pending writes
            if (sync_requested.load(std::memory_order_acquire) &&
                pending_writes.load(std::memory_order_acquire) == 0)
            {
                sync_cv.notify_all();
            }
        }
    }
}

void TimeSeriesDB::rebuild_index()
{
    std::unique_lock<std::shared_mutex> lock(query_mutex);

    size_t count = timestamps.get_count();
    for (size_t i = 0; i < count; ++i)
    {
        uint64_t ts;
        timestamps.read(i, &ts);
        time_index.insert(ts, i);
    }
}

std::vector<std::tuple<uint64_t, double, uint64_t>> TimeSeriesDB::query_range(uint64_t start, uint64_t end) const
{
    std::shared_lock<std::shared_mutex> lock(query_mutex);

    // Use B+ Tree for efficient range lookup
    auto results = time_index.range_query(start, end);

    std::vector<std::tuple<uint64_t, double, uint64_t>> ticks;
    ticks.reserve(results.size());

    for (const auto &[ts, idx] : results)
    {
        double price;
        uint64_t volume;

        // Zero-copy reading from mmap'd files
        prices.read(idx, &price);
        volumes.read(idx, &volume);

        ticks.emplace_back(ts, price, volume);
    }

    return ticks;
}

std::vector<std::tuple<uint64_t, double, uint64_t>> TimeSeriesDB::query_last(size_t n) const
{
    std::shared_lock<std::shared_mutex> lock(query_mutex);

    size_t count = timestamps.get_count();
    size_t start_idx = (count > n) ? (count - n) : 0;

    std::vector<std::tuple<uint64_t, double, uint64_t>> result;
    result.reserve(count - start_idx);

    for (size_t i = start_idx; i < count; ++i)
    {
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

void TimeSeriesDB::sync()
{
    // Set sync requested flag
    sync_requested.store(true, std::memory_order_release);

    // Wait for all pending writes to complete
    std::unique_lock<std::mutex> lock(write_mutex);
    sync_cv.wait(lock, [this]
                 { return pending_writes.load(std::memory_order_acquire) == 0; });

    // Reset sync requested flag
    sync_requested.store(false, std::memory_order_release);
}