# Vendored CorfuDB protocol definitions

These files are the wire protocol of the CorfuDB server, copied from
`runtime/proto/` of the CorfuDB repository for the native C++ client
(`src/db/corfu/`, `PLAN-native-corfu.md`).

| File | Source | Change |
|---|---|---|
| `rpc_common.proto` | `runtime/proto/rpc_common.proto` | none |
| `log_data.proto` | `runtime/proto/log_data.proto` | none |
| `tx_resolution.proto` | `runtime/proto/tx_resolution.proto` | none |
| `server_errors.proto` | `runtime/proto/server_errors.proto` | none |
| `service/base.proto` | `runtime/proto/service/base.proto` | none |
| `service/layout.proto` | `runtime/proto/service/layout.proto` | none |
| `service/sequencer.proto` | `runtime/proto/service/sequencer.proto` | none |
| `service/log_unit.proto` | `runtime/proto/service/log_unit.proto` | none |
| `service/corfu_message.proto` | `runtime/proto/service/corfu_message.proto` | management and log replication payload cases removed |

Source commit: `65902f3af6688a8c9020558dad72ac07f75f012c` (last change to
`runtime/proto/`, 2024-05-02) of the local CorfuDB checkout at
`05e0ba53744602467efec325733105b3949810b5`. The bench nodes pin CorfuDB at
`8f144d4c92535dfe5fad8e1a5c9ddaba5b7ad8d5` (`bench/scripts/setup.sh`). Phase 1
of the plan diffs these files against `runtime/proto/` of that commit on a
bench node before the first cluster run.

The removed payload cases keep their field numbers free. Protobuf skips an
unknown oneof member on the wire, so every server reply still parses.

Generated into `build/generated/corfu_proto/` by CMake. Sources include them as
`"service/corfu_message.pb.h"`.
