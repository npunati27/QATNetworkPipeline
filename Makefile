# Compiler
CC = gcc

# Compiler flags
CFLAGS = -Wall -O3 -I/opt/intel/qat/include

# Linker flags (all targets need QAT)
LDFLAGS = -L/opt/intel/qat/lib
LDLIBS = -lqat -lusdm -lpthread -lm

# Build directory
BUILD_DIR = build

# Target executables
SENDER_TARGET = $(BUILD_DIR)/qat_sender
PIPELINE_TARGET = $(BUILD_DIR)/pipeline_receiver
FAST_TARGET = $(BUILD_DIR)/fast_receiver

# Source files
SENDER_SRCS = qat_comp_sender.c
PIPELINE_SRCS = receivers/qat_pipeline_reciever.c
FAST_SRCS = receivers/fast_receiver.c

# Default target - build all
all: $(SENDER_TARGET) $(PIPELINE_TARGET) $(FAST_TARGET)

# Build the sender
$(SENDER_TARGET): $(SENDER_SRCS) | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)
	@echo "Build complete: $(SENDER_TARGET)"

# Build the pipeline receiver
$(PIPELINE_TARGET): $(PIPELINE_SRCS) | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)
	@echo "Build complete: $(PIPELINE_TARGET)"

# Build the fast receiver
$(FAST_TARGET): $(FAST_SRCS) | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)
	@echo "Build complete: $(FAST_TARGET)"

# Create build directory if it doesn't exist
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

# Clean build artifacts
clean:
	rm -f $(SENDER_TARGET) $(PIPELINE_TARGET) $(FAST_TARGET)
	rm -f *.o receivers/*.o
	@echo "Cleaned build artifacts"

# Build individual targets
sender: $(SENDER_TARGET)

pipeline: $(PIPELINE_TARGET)

fast: $(FAST_TARGET)

# Run targets
run-sender: $(SENDER_TARGET)
	sudo $(SENDER_TARGET)

run-pipeline: $(PIPELINE_TARGET)
	sudo $(PIPELINE_TARGET)

run-fast: $(FAST_TARGET)
	sudo $(FAST_TARGET)

# Run sender with specific arguments
run-uncompressed: $(SENDER_TARGET)
	sudo $(SENDER_TARGET) -f 0.0 -l 6

run-compressed: $(SENDER_TARGET)
	sudo $(SENDER_TARGET) -f 1.0 -l 6


# Rebuild (clean + build)
rebuild: clean all

# Help
help:
	@echo "Available targets:"
	@echo "  all              - Build all programs (default)"
	@echo "  sender           - Build sender only"
	@echo "  pipeline         - Build pipeline receiver only"
	@echo "  fast             - Build fast receiver only"
	@echo "  clean            - Remove build artifacts"
	@echo "  distclean        - Remove build artifacts and build directory"
	@echo "  run-sender       - Run sender with sudo"
	@echo "  run-pipeline     - Run pipeline receiver with sudo"
	@echo "  run-fast         - Run fast receiver with sudo"
	@echo "  run-uncompressed - Run sender with 100% uncompressed traffic"
	@echo "  run-compressed   - Run sender with 100% compressed traffic"
	@echo "  run-mixed        - Run sender with 50/50 mixed traffic"
	@echo "  rebuild          - Clean and rebuild all"
	@echo "  install          - Install all to /usr/local/bin/"
	@echo "  uninstall        - Remove all from /usr/local/bin/"
	@echo "  help             - Show this help message"

# Phony targets
.PHONY: all sender pipeline fast clean distclean run-sender run-pipeline run-fast \
        run-uncompressed run-compressed run-mixed install uninstall rebuild help
