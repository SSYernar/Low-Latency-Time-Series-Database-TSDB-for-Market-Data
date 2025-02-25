#include "timeseries_db.hpp"
#include <iostream>
#include <cstdlib>
#include <iomanip>

void print_help() {
    std::cout << "Usage:\n"
              << "  tsdb_cli insert <symbol> <timestamp> <price> <volume>\n"
              << "  tsdb_cli query <symbol> <start_timestamp> <end_timestamp>\n";
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_help();
        return 1;
    }

    std::string command = argv[1];

    try {
        if (command == "insert") {
            if (argc != 6) {
                print_help();
                return 1;
            }
            std::string symbol = argv[2];
            uint64_t timestamp = std::stoull(argv[3]);
            double price = std::stod(argv[4]);
            uint64_t volume = std::stoull(argv[5]);

            TimeSeriesDB db(symbol);
            db.append(timestamp, price, volume);
            std::cout << "Inserted tick for " << symbol << std::endl;
        } else if (command == "query") {
            if (argc != 5) {
                print_help();
                return 1;
            }
            std::string symbol = argv[2];
            uint64_t start = std::stoull(argv[3]);
            uint64_t end = std::stoull(argv[4]);

            TimeSeriesDB db(symbol);
            auto results = db.query_range(start, end);

            std::cout << "Found " << results.size() << " results:\n";
            for (const auto& [ts, price, vol] : results) {
                std::cout << "Timestamp: " << ts
                          << " Price: " << std::fixed << std::setprecision(2) << price
                          << " Volume: " << vol << std::endl;
            }
        } else {
            print_help();
            return 1;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}