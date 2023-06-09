# Compiler and flags
CXX = g++
CXXFLAGS = -g -Wno-deprecated-declarations -std=c++17

# Source files
SRC = test.cpp extmem.cpp worker.cpp

# Binary output
BIN = bin/test

# Build target
.PHONY: all
all: clean $(BIN)

# Compile and link
$(BIN): $(SRC)
	$(CXX) $(CXXFLAGS) -o $(BIN) $(SRC)

# Clean target
.PHONY: clean
clean:
	rm -rf *.blk
	rm -f $(BIN)
