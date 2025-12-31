use criterion::{criterion_group, criterion_main, Criterion, Throughput};

fn bench_placeholder(c: &mut Criterion) {
    let mut group = c.benchmark_group("bitcask");
    group.throughput(Throughput::Elements(1));
    group.bench_function("placeholder", |b| {
        b.iter(|| {
            // TODO: Add real benchmarks
            std::hint::black_box(1 + 1)
        });
    });
    group.finish();
}

criterion_group!(benches, bench_placeholder);
criterion_main!(benches);
