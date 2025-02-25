#ifndef TIMESERIES_DB_HPP
#define TIMESERIES_DB_HPP

#include "column_storage.hpp"
#include <vector>
#include <tuple>
#include <string>

class TimeSeriesDB {
public:
    explicit TimeSeriesDB(const std::string& symbol);
    void append(uint64_t timestamp, double price, uint64_t volume);
    std::vector<std::tuple<uint64_t, double, uint64_t>> query_range(uint64_t start, uint64_t end) const;

private:
    ColumnStorage timestamps;
    ColumnStorage prices;
    ColumnStorage volumes;
    std::vector<uint64_t> timestamp_index;
};

#endif // TIMESERIES_DB_HPP