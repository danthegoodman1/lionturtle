fn main() {
    let buf = vec![7u8; 4096];
    let t = std::time::Instant::now();
    let mut x = 0u32;
    for i in 0..250_000u32 {
        x ^= crc32c::crc32c_append(i, &buf[4..]);
    }
    let s = t.elapsed().as_secs_f64();
    println!("crc32c 4KiB blocks: {:.2} GB/s ({x})", 250_000.0 * 4096.0 / s / 1e9);
}
