# Compiler
CC = gcc

CFLAGS = -Wall -O3 -I/opt/intel/qat/include

LDFLAGS = -L/opt/intel/qat/lib
LDLIBS = -lqat -lusdm -lpthread -lm

BUILD_DIR = build

# Target executables
SENDER_TARGET = $(BUILD_DIR)/qat_sender
PIPELINE_TARGET = $(BUILD_DIR)/pipeline_receiver
FAST_TARGET = $(BUILD_DIR)/fast_receiver
QAT_TARGET = $(BUILD_DIR)/qat_receiver

# Source files
SENDER_SRCS = qat_comp_sender.c
PIPELINE_SRCS = receivers/qat_pipeline_reciever.c
FAST_SRCS = receivers/fast_receiver.c
QAT_SRCS = receivers/qat_reciever.c

# Default target - build all
all: $(SENDER_TARGET) $(PIPELINE_TARGET) $(FAST_TARGET) $(QAT_TARGET)

$(SENDER_TARGET): $(SENDER_SRCS) | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)
	@echo "Build complete: $(SENDER_TARGET)"

$(PIPELINE_TARGET): $(PIPELINE_SRCS) | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)
	@echo "Build complete: $(PIPELINE_TARGET)"

$(FAST_TARGET): $(FAST_SRCS) | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)
	@echo "Build complete: $(FAST_TARGET)"

$(QAT_TARGET): $(QAT_SRCS) | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)
	@echo "Build complete: $(QAT_TARGET)"

# Create build directory if it doesn't exist
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

clean:
	rm -f $(SENDER_TARGET) $(PIPELINE_TARGET) $(FAST_TARGET) $(QAT_TARGET)
	rm -f *.o receivers/*.o
	@echo "Cleaned build artifacts"

sender: $(SENDER_TARGET)

pipeline: $(PIPELINE_TARGET)

fast: $(FAST_TARGET)

qat: $(QAT_TARGET)

# Run targets
run-sender: $(SENDER_TARGET)
	sudo $(SENDER_TARGET)

run-pipeline: $(PIPELINE_TARGET)
	sudo $(PIPELINE_TARGET)

run-qat-receiver: $(QAT_TARGET)
	sudo $(QAT_TARGET)


run-fast: $(FAST_TARGET)
	sudo $(FAST_TARGET)

run-uncompressed: $(SENDER_TARGET)
	sudo $(SENDER_TARGET) -f 0.0 -l 6

run-compressed: $(SENDER_TARGET)
	sudo $(SENDER_TARGET) -f 1.0 -l 6


rebuild: clean all

# Phony targets
.PHONY: all sender pipeline fast clean distclean run-sender run-pipeline run-fast \
        run-uncompressed run-compressed run-mixed install uninstall rebuild help
