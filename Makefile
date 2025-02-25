# Compiler and flags
CXX = g++
CXXFLAGS = -std=c++20 -O3 -Wall -Wextra -march=native

# Target
TARGET = tsdb_cli

# Source files
SOURCES = cli.cpp timeseries_db.cpp column_storage.cpp

# Object files
OBJECTS = $(SOURCES:.cpp=.o)

# Header files
HEADERS = column_storage.hpp timeseries_db.hpp bplus_tree.hpp

# Main target
all: $(TARGET)

# Link
$(TARGET): $(OBJECTS)
	$(CXX) $(CXXFLAGS) -o $@ $^ -pthread

# Compile
%.o: %.cpp $(HEADERS)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean
clean:
	rm -f $(OBJECTS) $(TARGET)

# Run tests
test: $(TARGET)
	./$(TARGET) benchmark TEST 10000

# Generate benchmark data
benchmark: $(TARGET)
	./$(TARGET) benchmark TEST 100000
	./$(TARGET) benchmark TEST 1000000

.PHONY: all clean test benchmark