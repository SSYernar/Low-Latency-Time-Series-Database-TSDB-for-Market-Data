#include "timeseries_db.hpp"
#include <algorithm>

TimeSeriesDB::TimeSeriesDB(const std::string& symbol)
    : timestamps(symbol + "_timestamps.bin", sizeof(uint64_t)),
      prices(symbol + "_prices.bin", sizeof(double)),
      volumes(symbol + "_volumes.bin", sizeof(uint64_t)) {
    size_t count = timestamps.get_count();
    timestamp_index.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        uint64_t ts;
        timestamps.read(i, &ts);
        timestamp_index.push_back(ts);
    }
}

void TimeSeriesDB::append(uint64_t timestamp, double price, uint64_t volume) {
    timestamps.append(&timestamp);
    prices.append(&price);
    volumes.append(&volume);
    timestamp_index.push_back(timestamp);
}

std::vector<std::tuple<uint64_t, double, uint64_t>> TimeSeriesDB::query_range(uint64_t start, uint64_t end) const {
    auto lower = std::lower_bound(timestamp_index.begin(), timestamp_index.end(), start);
    auto upper = std::upper_bound(timestamp_index.begin(), timestamp_index.end(), end);

    std::vector<std::tuple<uint64_t, double, uint64_t>> result;
    result.reserve(std::distance(lower, upper));

    for (auto it = lower; it != upper; ++it) {
        size_t idx = it - timestamp_index.begin();
        uint64_t ts = *it;
        double price;
        prices.read(idx, &price);
        uint64_t volume;
        volumes.read(idx, &volume);
        result.emplace_back(ts, price, volume);
    }

    return result;
}