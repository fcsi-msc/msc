# Sources live in src/, test harnesses and LD_PRELOAD shims in tests/. Every
# build product goes to build/ except the msc binary, which stays at the top.
B := build

msc_Cfiles := findzero.c fork.c  local.c  local_single.c local_multiple.c \
              local_sockets.c recursive.c msc.c parse.c remote.c netutil.c \
              udp_transport.c udp_session.c udp_io.c udp_stats.c udp_control.c \
              port_spec.c man.c resume.c cancellation.c progress.c
msc_OBJS := $(addprefix $(B)/,$(msc_Cfiles:.c=.o))

# $(call o,a b) -> build/a.o build/b.o
o = $(patsubst %,$(B)/%.o,$(1))

UDP_ENGINE_OBJS := $(call o,udp_session udp_io udp_stats udp_control port_spec)
ALL_OBJS := $(msc_OBJS)
# gcc unless the caller chose a compiler (make CC=clang, or CC in the environment)
ifeq ($(origin CC),default)
CC = gcc
endif
CFLAGS ?= -O2 -g
CFLAGS += -Wall -Wextra
# -MMD writes each object's header dependencies to build/*.d. The test harness
# objects embed struct argdata too, so a stale one linked against fresh msc
# objects would read fields at wrong offsets after any msc.h change.
override CPPFLAGS += -Isrc -MMD -MP
override LDFLAGS += -pthread
LDLIBS=-lm

ifeq ($(LUSTRE),1)
CFLAGS += -DHAVE_LUSTRE
LDLIBS += -llustreapi
endif

TEST_BINS := recursive_test segmented_test child_status_test udp_test \
             udp_protocol_test resume_test retry_test exit_code_test \
             stall_timeout_test port_spec_test port_squat
SHIMS := $(addprefix $(B)/,wan_shim.so storage_fault_shim.so \
             listen_count_shim.so fsync_fault_shim.so udp_offload_shim.so)

all: msc

$(B)/%.o: src/%.c
	@mkdir -p $(@D)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(B)/%.o: tests/%.c
	@mkdir -p $(@D)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(B)/%.o: benchmarks/%.c
	@mkdir -p $(@D)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

-include $(wildcard $(B)/*.d)

msc: $(ALL_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o $@ $(LDLIBS)

# `make udp_test` and friends build build/udp_test.
$(TEST_BINS): %: $(B)/%

$(B)/recursive_test: $(call o,recursive_test recursive resume progress cancellation netutil) $(UDP_ENGINE_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o $@ $(LDLIBS)

$(B)/manifest_scan: $(call o,manifest_scan recursive resume progress cancellation netutil) $(UDP_ENGINE_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o $@ $(LDLIBS)

$(B)/segmented_test: $(call o,segmented_test local_sockets remote recursive fork netutil resume progress cancellation udp_transport) $(UDP_ENGINE_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o $@ $(LDLIBS)

$(B)/child_status_test: $(call o,child_status_test fork cancellation)
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o $@

$(B)/udp_test: $(call o,udp_test udp_transport recursive resume progress cancellation netutil) $(UDP_ENGINE_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o $@ $(LDLIBS)

$(B)/udp_protocol_test: $(call o,udp_protocol_test netutil) $(UDP_ENGINE_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o $@ $(LDLIBS)

$(B)/resume_test: $(call o,resume_test local_sockets remote recursive fork netutil resume progress cancellation udp_transport) $(UDP_ENGINE_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o $@ $(LDLIBS)

$(B)/retry_test: $(call o,retry_test local_single local_sockets remote recursive fork netutil resume progress cancellation udp_transport) $(UDP_ENGINE_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o $@ $(LDLIBS)

$(B)/stall_timeout_test: $(call o,stall_timeout_test netutil)
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o $@

$(B)/exit_code_test: $(call o,exit_code_test)
	$(CC) $(CFLAGS) $^ -o $@

$(B)/port_spec_test: $(call o,port_spec_test port_spec)
	$(CC) $(CFLAGS) $^ -o $@

$(B)/port_squat: $(call o,port_squat)
	$(CC) $(CFLAGS) $^ -o $@

# Test-only LD_PRELOAD shims (tests/shims/): a privilege-free WAN emulator, a
# destination-storage fault injector, an fsync/thread fault injector, a
# listen() recorder, and a no-GSO/GRO kernel. None is linked into msc.
shims: $(SHIMS)

$(B)/wan_shim.so: tests/shims/wan_shim.c
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) -shared -fPIC -pthread $< -o $@ -ldl

$(B)/storage_fault_shim.so: tests/shims/storage_fault_shim.c
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) -shared -fPIC $< -o $@ -ldl

$(B)/listen_count_shim.so: tests/shims/listen_count_shim.c
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) -shared -fPIC $< -o $@ -ldl

$(B)/fsync_fault_shim.so: tests/shims/fsync_fault_shim.c
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) -shared -fPIC -pthread $< -o $@ -ldl

$(B)/udp_offload_shim.so: tests/shims/udp_offload_shim.c
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) -shared -fPIC $< -o $@ -ldl

$(B)/wan_shim_test: tests/shims/wan_shim_test.c
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) -pthread $< -o $@

# Statistical self-test of the WAN shim itself: passthrough, delay, loss, the
# three combined, and seed determinism.
shim_check: $(B)/wan_shim.so $(B)/wan_shim_test
	@set -e; SHIM=$$PWD/$(B)/wan_shim.so; T=./$(B)/wan_shim_test; \
	echo "== passthrough =="; LD_PRELOAD=$$SHIM $$T; \
	echo "== delay =="; LD_PRELOAD=$$SHIM MSC_UDP_WANSHIM_DELAY_MS=20 $$T; \
	echo "== loss =="; LD_PRELOAD=$$SHIM MSC_UDP_WANSHIM_LOSS_PCT=10 $$T; \
	echo "== delay + jitter + loss =="; \
	LD_PRELOAD=$$SHIM MSC_UDP_WANSHIM_DELAY_MS=25 MSC_UDP_WANSHIM_JITTER_MS=5 \
	   MSC_UDP_WANSHIM_LOSS_PCT=5 $$T; \
	echo "== determinism =="; \
	a=$$(LD_PRELOAD=$$SHIM MSC_UDP_WANSHIM_LOSS_PCT=20 MSC_UDP_WANSHIM_SEED=12345 $$T | grep '^DROPPED:'); \
	b=$$(LD_PRELOAD=$$SHIM MSC_UDP_WANSHIM_LOSS_PCT=20 MSC_UDP_WANSHIM_SEED=12345 $$T | grep '^DROPPED:'); \
	test "$$a" = "$$b" && echo "  deterministic OK" || { echo "  NONDETERMINISTIC"; exit 1; }
	@set -e; SHIM=$$PWD/$(B)/wan_shim.so; T=./$(B)/wan_shim_test; \
	for mode in gso mmsg; do \
	   echo "== $$mode: per-segment MTU, loss, delay, scatter/gather and TOS =="; \
	   LD_PRELOAD=$$SHIM MSC_TEST_SEND_STYLE=$$mode MSC_UDP_WANSHIM_MTU=12 $$T; \
	   a=$$(LD_PRELOAD=$$SHIM MSC_UDP_WANSHIM_LOSS_PCT=20 MSC_UDP_WANSHIM_SEED=12345 $$T | grep '^DROPPED:'); \
	   b=$$(LD_PRELOAD=$$SHIM MSC_TEST_SEND_STYLE=$$mode MSC_UDP_WANSHIM_DELAY_MS=10 \
	      MSC_UDP_WANSHIM_MTU=12 MSC_UDP_WANSHIM_LOSS_PCT=20 MSC_UDP_WANSHIM_SEED=12345 $$T | grep '^DROPPED:'); \
	   test "$$a" = "$$b" || { echo "GSO changed the loss pattern"; exit 1; }; \
	done

udp_regressions: msc $(B)/udp_test $(B)/wan_shim.so $(B)/fsync_fault_shim.so
	python3 tests/udp_regression_test.py

udp_parity: $(B)/udp_test $(B)/udp_protocol_test $(B)/udp_offload_shim.so $(B)/port_spec_test $(B)/port_squat
	./$(B)/port_spec_test
	sh tests/udp_parity_test.sh
	sh tests/udp_recursive_ctl_test.sh

# Gate the UDP engine through the privilege-free WAN emulator (build/wan_shim.so): a 15ms/2% smoke plus a 50ms/0.1% 64 MiB profile that asserts a
# throughput floor and a retransmit ceiling (see tests/wanshim_test.sh).
wanshim: $(B)/udp_test msc $(B)/wan_shim.so
	sh tests/wanshim_test.sh smoke
	sh tests/wanshim_test.sh wan
	sh tests/wanshim_test.sh mtu

# The wan profile alone.
wanshim_wan: $(B)/udp_test $(B)/wan_shim.so
	sh tests/wanshim_test.sh wan

# --dest-stripe-count: CLI bounds, the shapes it refuses, and proof the request
# reaches whichever process creates the destination. Actual OST placement needs
# a real Lustre file system; see tests/lustre_dest_stripe_test.sh.
dest_stripe_suite: msc
	sh tests/dest_stripe_test.sh

udp_resume_suite: $(B)/udp_test $(B)/storage_fault_shim.so
	sh tests/udp_resume_test.sh
	sh tests/udp_recursive_resume_test.sh

stdio_ctl_suite: msc $(B)/wan_shim.so $(B)/listen_count_shim.so
	sh tests/stdio_ctl_test.sh
	sh tests/env_preset_test.sh

# Which transport a command line gets: the UDP default, -T, -U, and the
# stdin/-c shapes the UDP engine cannot carry.  Its own target because the C
# harnesses bypass parseargs() and so cannot cover any of it.
transport_suite: msc
	sh tests/transport_default_test.sh

# The sub-suites run one after another rather than as prerequisites: they
# share loopback UDP ports, so `make -j` must not start them concurrently.
resume_suite: msc $(addprefix $(B)/,resume_test recursive_test retry_test exit_code_test stall_timeout_test)
	$(MAKE) udp_resume_suite
	$(MAKE) stdio_ctl_suite
	$(MAKE) transport_suite
	$(MAKE) dest_stripe_suite
	test "$$(./msc --probe)" = MSC-PROBE-OK
	sh tests/resume_test.sh
	./$(B)/retry_test
	./$(B)/exit_code_test
	./$(B)/stall_timeout_test

# Every fast suite, serially (safe under make -j).
check:
	$(MAKE) udp_parity
	$(MAKE) resume_suite
	$(MAKE) udp_regressions
	$(MAKE) shim_check
	$(MAKE) wanshim

mscclean:
	/bin/rm -rf $(B) msc

clean: mscclean

PREFIX ?= /usr/local
BINDIR ?= $(PREFIX)/bin

install: msc
	install -d $(DESTDIR)$(BINDIR)
	install -m 755 msc $(DESTDIR)$(BINDIR)/msc

uninstall:
	/bin/rm -f $(DESTDIR)$(BINDIR)/msc

# --- Valgrind / Helgrind memory + race checks (see docs/valgrind.md) ---
.PHONY: all check clean mscclean install uninstall valgrind helgrind helgrind_udp udp_parity \
	udp_resume_suite resume_suite wanshim wanshim_wan shim_check shims \
	dest_stripe_suite stdio_ctl_suite transport_suite udp_regressions $(TEST_BINS)

VALGRIND ?= valgrind
VG_DIR ?= /tmp/mscvg
VG_FLAGS := --leak-check=full --show-leak-kinds=all --track-origins=yes \
            --num-callers=40 --error-exitcode=1 --suppressions=tests/valgrind.supp
VG_HARNESSES := $(addprefix $(B)/,segmented_test udp_test recursive_test resume_test retry_test child_status_test)

# Build the harnesses unoptimised (clean line numbers), generate fixtures, then
# run Memcheck over the main file-transfer paths: TCP single-file, UDP clean +
# 25% loss, recursive directory, and fork/child-exit. The legacy stdin/command
# pipe path still requires an SSH-level integration test. Run `make clean`
# first for a pristine -O0 build. Exits non-zero on any leak or error
# (--error-exitcode=1).
valgrind: CFLAGS := -O0 -g
valgrind: $(VG_HARNESSES)
	@rm -rf $(VG_DIR); mkdir -p $(VG_DIR)/rsrc/sub1 $(VG_DIR)/rsrc/sub2/deep
	@head -c 16M /dev/urandom > $(VG_DIR)/in.bin
	@head -c 5M /dev/urandom > $(VG_DIR)/rsrc/a.bin
	@head -c 1M /dev/urandom > $(VG_DIR)/rsrc/sub1/b.bin
	@head -c 3M /dev/urandom > $(VG_DIR)/rsrc/sub2/deep/c.bin
	@: > $(VG_DIR)/rsrc/empty.bin
	$(VALGRIND) $(VG_FLAGS) ./$(B)/segmented_test $(VG_DIR)/in.bin $(VG_DIR)/out.bin 8 1048576 1
	$(VALGRIND) $(VG_FLAGS) ./$(B)/resume_test $(VG_DIR)/in.bin $(VG_DIR)/resume-out.bin
	$(VALGRIND) $(VG_FLAGS) ./$(B)/retry_test
	$(VALGRIND) $(VG_FLAGS) ./$(B)/udp_test $(VG_DIR)/in.bin $(VG_DIR)/udpout.bin 8
	$(VALGRIND) $(VG_FLAGS) ./$(B)/udp_test $(VG_DIR)/in.bin $(VG_DIR)/udpout.bin 8 25
	$(VALGRIND) $(VG_FLAGS) ./$(B)/recursive_test $(VG_DIR)/rsrc $(VG_DIR)/rdst 8 1048576
	diff -r $(VG_DIR)/rsrc $(VG_DIR)/rdst
	$(VALGRIND) $(VG_FLAGS) ./$(B)/child_status_test
	@echo "valgrind: all harnesses clean"

# Thread-race pass (Helgrind) on the threaded sender/receiver workers. Expect
# benign glibc/pthreads noise; only stacks in local_sockets.c / remote.c matter.
helgrind: CFLAGS := -O0 -g
helgrind: $(B)/segmented_test
	@mkdir -p $(VG_DIR); head -c 16M /dev/urandom > $(VG_DIR)/in.bin
	$(VALGRIND) --tool=helgrind --num-callers=40 ./$(B)/segmented_test $(VG_DIR)/in.bin $(VG_DIR)/out.bin 8 1048576 1

helgrind_udp: $(B)/udp_test $(B)/udp_offload_shim.so
	VALGRIND="$(VALGRIND)" sh tests/helgrind_udp_test.sh
