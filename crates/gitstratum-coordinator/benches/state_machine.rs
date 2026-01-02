use criterion::{criterion_group, criterion_main, Criterion};

fn bench_state_machine_apply(c: &mut Criterion) {
    // TODO: Implement benchmarks
    c.bench_function("state_machine_apply", |b| {
        b.iter(|| {
            // Placeholder
        })
    });
}

criterion_group!(benches, bench_state_machine_apply);
criterion_main!(benches);
