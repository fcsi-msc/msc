msc_Cfiles := findzero.c fork.c  local.c  local_single.c local_multiple.c \
              local_sockets.c recursive.c msc.c parse.c remote.c netutil.c \
              udp_transport.c udp_session.c udp_io.c udp_stats.c udp_control.c \
              port_spec.c man.c resume.c cancellation.c progress.c
msc_OBJS := $(msc_Cfiles:.c=.o)

UDP_ENGINE_OBJS := udp_session.o udp_io.o udp_stats.o udp_control.o port_spec.o
ALL_OBJS := $(msc_OBJS)
# gcc unless the caller chose a compiler (make CC=clang, or CC in the environment)
ifeq ($(origin CC),default)
CC = gcc
endif
CFLAGS ?= -O2 -g
CFLAGS += -Wall -Wextra
override LDFLAGS += -pthread
LDLIBS=-lm

ifeq ($(LUSTRE),1)
CFLAGS += -DHAVE_LUSTRE
LDLIBS += -llustreapi
endif

all: msc
$(msc_OBJS): msc.h
# Test harness objects embed struct argdata too -- a stale one linked against
# fresh msc objects reads fields at wrong offsets after any msc.h change.
recursive_test.o segmented_test.o child_status_test.o udp_test.o \
	udp_protocol_test.o resume_test.o retry_test.o exit_code_test.o: msc.h

$(UDP_ENGINE_OBJS) port_spec_test.o: udp_session.h
udp_session.o udp_stats.o: udp_stats.h
udp_transport.o udp_protocol_test.o: udp_session.h

msc: $(ALL_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o $@ $(LDLIBS)

recursive_test: recursive_test.o recursive.o resume.o progress.o cancellation.o netutil.o $(UDP_ENGINE_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o recursive_test $(LDLIBS)

benchmarks/manifest_scan.o: benchmarks/manifest_scan.c msc.h
	$(CC) $(CFLAGS) -I. -c $< -o $@

benchmarks/manifest_scan: benchmarks/manifest_scan.o recursive.o resume.o progress.o cancellation.o netutil.o $(UDP_ENGINE_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o $@ $(LDLIBS)

segmented_test: segmented_test.o local_sockets.o remote.o recursive.o fork.o netutil.o resume.o progress.o cancellation.o udp_transport.o $(UDP_ENGINE_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o segmented_test $(LDLIBS)

child_status_test: child_status_test.o fork.o cancellation.o
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o child_status_test

udp_test: udp_test.o udp_transport.o recursive.o resume.o progress.o cancellation.o netutil.o $(UDP_ENGINE_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o udp_test $(LDLIBS)

udp_protocol_test: udp_protocol_test.o netutil.o $(UDP_ENGINE_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o udp_protocol_test $(LDLIBS)

resume_test: resume_test.o local_sockets.o remote.o recursive.o fork.o netutil.o resume.o progress.o cancellation.o udp_transport.o $(UDP_ENGINE_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o resume_test $(LDLIBS)

retry_test: retry_test.o local_single.o local_sockets.o remote.o recursive.o fork.o netutil.o resume.o progress.o cancellation.o udp_transport.o $(UDP_ENGINE_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o retry_test $(LDLIBS)

stall_timeout_test: stall_timeout_test.o netutil.o
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o stall_timeout_test

exit_code_test: exit_code_test.o
	$(CC) $(CFLAGS) $^ -o exit_code_test

port_spec_test: port_spec_test.o port_spec.o
	$(CC) $(CFLAGS) $^ -o port_spec_test

port_squat: port_squat.o
	$(CC) $(CFLAGS) $^ -o port_squat

udp_offload_shim.so: udp_offload_shim.c
	$(CC) $(CFLAGS) -shared -fPIC $< -o $@ -ldl

# Test-only LD_PRELOAD shims (shims/): a privilege-free WAN emulator, a
# destination-storage fault injector, and a listen() recorder. Neither is linked into msc.
shims/wan_shim.so: shims/wan_shim.c
	$(CC) $(CFLAGS) -shared -fPIC -pthread $< -o $@ -ldl

shims/storage_fault_shim.so: shims/storage_fault_shim.c
	$(CC) $(CFLAGS) -shared -fPIC $< -o $@ -ldl

shims/listen_count_shim.so: shims/listen_count_shim.c
	$(CC) $(CFLAGS) -shared -fPIC $< -o $@ -ldl

shims/fsync_fault_shim.so: shims/fsync_fault_shim.c
	$(CC) $(CFLAGS) -shared -fPIC -pthread $< -o $@ -ldl

shims/wan_shim_test: shims/wan_shim_test.c
	$(CC) $(CFLAGS) -pthread $< -o $@

# Statistical self-test of the WAN shim itself: passthrough, delay, loss, the
# three combined, and seed determinism.
shim_check: shims/wan_shim.so shims/wan_shim_test
	@set -e; SHIM=$$PWD/shims/wan_shim.so; T=./shims/wan_shim_test; \
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
	@set -e; SHIM=$$PWD/shims/wan_shim.so; T=./shims/wan_shim_test; \
	for mode in gso mmsg; do \
	   echo "== $$mode: per-segment MTU, loss, delay, scatter/gather and TOS =="; \
	   LD_PRELOAD=$$SHIM MSC_TEST_SEND_STYLE=$$mode MSC_UDP_WANSHIM_MTU=12 $$T; \
	   a=$$(LD_PRELOAD=$$SHIM MSC_UDP_WANSHIM_LOSS_PCT=20 MSC_UDP_WANSHIM_SEED=12345 $$T | grep '^DROPPED:'); \
	   b=$$(LD_PRELOAD=$$SHIM MSC_TEST_SEND_STYLE=$$mode MSC_UDP_WANSHIM_DELAY_MS=10 \
	      MSC_UDP_WANSHIM_MTU=12 MSC_UDP_WANSHIM_LOSS_PCT=20 MSC_UDP_WANSHIM_SEED=12345 $$T | grep '^DROPPED:'); \
	   test "$$a" = "$$b" || { echo "GSO changed the loss pattern"; exit 1; }; \
	done

udp_regressions: msc udp_test shims/wan_shim.so shims/fsync_fault_shim.so
	python3 ./udp_regression_test.py

udp_parity: udp_test udp_protocol_test udp_offload_shim.so port_spec_test port_squat
	./port_spec_test
	sh ./udp_parity_test.sh
	sh ./udp_recursive_ctl_test.sh

# Gate the UDP engine through the privilege-free WAN emulator (shims/wan_shim.so): a 15ms/2% smoke plus a 50ms/0.1% 64 MiB profile that asserts a
# throughput floor and a retransmit ceiling (see wanshim_test.sh).
wanshim: udp_test msc shims/wan_shim.so
	sh ./wanshim_test.sh smoke
	sh ./wanshim_test.sh wan
	sh ./wanshim_test.sh mtu

# The wan profile alone.
wanshim_wan: udp_test shims/wan_shim.so
	sh ./wanshim_test.sh wan

# --dest-stripe-count: CLI bounds, the shapes it refuses, and proof the request
# reaches whichever process creates the destination. Actual OST placement needs
# a real Lustre file system; see lustre_dest_stripe_test.sh.
dest_stripe_suite: msc
	sh ./dest_stripe_test.sh

udp_resume_suite: udp_test shims/storage_fault_shim.so
	sh ./udp_resume_test.sh
	sh ./udp_recursive_resume_test.sh

stdio_ctl_suite: msc shims/wan_shim.so shims/listen_count_shim.so
	sh ./stdio_ctl_test.sh
	sh ./env_preset_test.sh

# Which transport a command line gets: the UDP default, -T, -U, and the
# stdin/-c shapes the UDP engine cannot carry.  Its own target because the C
# harnesses bypass parseargs() and so cannot cover any of it.
transport_suite: msc
	sh ./transport_default_test.sh

# The sub-suites run one after another rather than as prerequisites: they
# share loopback UDP ports, so `make -j` must not start them concurrently.
resume_suite: msc resume_test recursive_test retry_test exit_code_test stall_timeout_test
	$(MAKE) udp_resume_suite
	$(MAKE) stdio_ctl_suite
	$(MAKE) transport_suite
	$(MAKE) dest_stripe_suite
	test "$$(./msc --probe)" = MSC-PROBE-OK
	sh ./resume_test.sh
	./retry_test
	./exit_code_test
	./stall_timeout_test

# Every fast suite, serially (safe under make -j).
check:
	$(MAKE) udp_parity
	$(MAKE) resume_suite
	$(MAKE) udp_regressions
	$(MAKE) shim_check
	$(MAKE) wanshim

mscclean:
	/bin/rm -f $(ALL_OBJS) recursive_test.o segmented_test.o child_status_test.o \
		udp_test.o udp_protocol_test.o port_spec_test.o port_squat.o \
		resume_test.o retry_test.o exit_code_test.o stall_timeout_test.o progress.o \
		msc recursive_test segmented_test child_status_test udp_test \
		udp_protocol_test resume_test retry_test exit_code_test \
		stall_timeout_test udp_offload_shim.so port_spec_test port_squat \
		shims/wan_shim.so shims/storage_fault_shim.so shims/wan_shim_test \
		shims/listen_count_shim.so
	/bin/rm -f shims/fsync_fault_shim.so
	/bin/rm -f benchmarks/manifest_scan benchmarks/manifest_scan.o

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
	udp_resume_suite resume_suite wanshim wanshim_wan shim_check \
	dest_stripe_suite stdio_ctl_suite transport_suite udp_regressions

VALGRIND ?= valgrind
VG_DIR ?= /tmp/mscvg
VG_FLAGS := --leak-check=full --show-leak-kinds=all --track-origins=yes \
            --num-callers=40 --error-exitcode=1 --suppressions=valgrind.supp
VG_HARNESSES := segmented_test udp_test recursive_test resume_test retry_test child_status_test

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
	$(VALGRIND) $(VG_FLAGS) ./segmented_test $(VG_DIR)/in.bin $(VG_DIR)/out.bin 8 1048576 1
	$(VALGRIND) $(VG_FLAGS) ./resume_test $(VG_DIR)/in.bin $(VG_DIR)/resume-out.bin
	$(VALGRIND) $(VG_FLAGS) ./retry_test
	$(VALGRIND) $(VG_FLAGS) ./udp_test $(VG_DIR)/in.bin $(VG_DIR)/udpout.bin 8
	$(VALGRIND) $(VG_FLAGS) ./udp_test $(VG_DIR)/in.bin $(VG_DIR)/udpout.bin 8 25
	$(VALGRIND) $(VG_FLAGS) ./recursive_test $(VG_DIR)/rsrc $(VG_DIR)/rdst 8 1048576
	diff -r $(VG_DIR)/rsrc $(VG_DIR)/rdst
	$(VALGRIND) $(VG_FLAGS) ./child_status_test
	@echo "valgrind: all harnesses clean"

# Thread-race pass (Helgrind) on the threaded sender/receiver workers. Expect
# benign glibc/pthreads noise; only stacks in local_sockets.c / remote.c matter.
helgrind: CFLAGS := -O0 -g
helgrind: segmented_test
	@mkdir -p $(VG_DIR); head -c 16M /dev/urandom > $(VG_DIR)/in.bin
	$(VALGRIND) --tool=helgrind --num-callers=40 ./segmented_test $(VG_DIR)/in.bin $(VG_DIR)/out.bin 8 1048576 1

helgrind_udp: udp_test udp_offload_shim.so
	VALGRIND="$(VALGRIND)" sh ./helgrind_udp_test.sh
