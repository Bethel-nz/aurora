#!/bin/bash
set -euo pipefail
cargo test --release \
  --test core_stress_bench \
  --test performance_comparison \
  --test stress_test \
  --test reactive_performance_test \
  --test reactive_extreme_test \
  --test memory_1m_test \
  --test pagination_stress_test \
  --test durability_stress_test \
  --test pubsub_fanout_test \
  --bench baseline_benchmark \
  --bench aurora_benchmarks \
  -- --nocapture | tee aurora_performance_release.report
