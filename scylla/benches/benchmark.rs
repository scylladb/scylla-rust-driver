use criterion::{criterion_group, criterion_main, Criterion};

use bytes::BytesMut;
use scylla::frame::types;

fn types_benchmark(c: &mut Criterion) {
    let mut buf = BytesMut::with_capacity(64);
    c.bench_function("short", |b| {
        b.iter(|| {
            buf.clear();
            types::write_short(-1, &mut buf);
            types::read_short(&mut &buf[..]).unwrap();
        })
    });
    c.bench_function("int", |b| {
        b.iter(|| {
            buf.clear();
            types::write_int(-1, &mut buf);
            types::read_int(&mut &buf[..]).unwrap();
        })
    });
    c.bench_function("long", |b| {
        b.iter(|| {
            buf.clear();
            types::write_long(-1, &mut buf);
            types::read_long(&mut &buf[..]).unwrap();
        })
    });
    c.bench_function("string", |b| {
        b.iter(|| {
            buf.clear();
            types::write_string("hello, world", &mut buf);
            types::read_string(&mut &buf[..]).unwrap();
        })
    });
}

criterion_group!(benches, types_benchmark);
criterion_main!(benches);
