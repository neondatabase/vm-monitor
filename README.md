# vm-monitor

The `vm-monitor` is one component of Neon's [autoscaling] system for VMs. The
monitor resides within a compute instance and manages a cgroup running Postgres.
It talks to to the `vm-informant`, which relays upscaling/downscaling decisions
from the `autoscaler-agent`, and then carries out those decisions. The monitor
can also inform the agent (via the informant) that it needs more resources.
Currently, the only metric that the monitor requests upscales based on is memory
usage.

The monitor is connected to the informant on port 10369 via a websocket connection.

# Simple usage
1. Clone this repository
2. Run `RUST_LOG=<log level> cargo run -- -c <cgroup name> -f <postgres connection string>`.

In practice, usage of the monitor involves much more integration with the rest of the
autoscaling system. However, running the monitor standalone can be useful for testing
it.

[autoscaling]: https://github.com/neondatabase/autoscaling
