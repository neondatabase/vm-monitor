# Preface

This repository was created by Felix Prasanna during his internship at Neon
during Summer 2023. The monitor now lives in
[`neondatatabse/neon/libs/vm_monitor`](https://github.com/neondatabase/neon/tree/main/libs/vm_monitor).

# vm-monitor

The `vm-monitor` is one component of Neon's [autoscaling] system for VMs. The
monitor resides within a compute instance and manages a cgroup running Postgres.
It talks to to the `autoscaler-agent`, which makes upscaling/downscaling decisions.
The monitor then carries out those decisions. The monitor can also inform the
agent that it needs more resources when under load. Currently, the only metric
that the monitor requests upscales based on is memory usage.

The monitor is connected to the agent on port 10301 via a websocket connection.

[autoscaling]: https://github.com/neondatabase/autoscaling
