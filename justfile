# For testing purposes

set limit:
    echo {{limit}} | sudo tee /sys/fs/cgroup/neon-test/memory.high

start:
    sudo cgexec -g memory:neon-test ./allocate-loop 128 512

monitor:
    cargo build && sudo RUST_LOG=trace ./target/debug/vm-monitor -c neon-test
