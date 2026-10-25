//! YCSB key generation: scrambled Zipfian choice over record ids, and a
//! bijective scramble from record id to an 8-byte key.

pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn next_u64(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    pub fn below(&mut self, n: u64) -> u64 {
        ((self.next_u64() as u128 * n as u128) >> 64) as u64
    }

    pub fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let b = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&b[..chunk.len()]);
        }
    }
}

/// splitmix64's finalizer: a bijection on u64, so distinct ids give
/// distinct keys in a scrambled order.
fn mix(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

pub fn key_of(id: u64) -> [u8; 8] {
    mix(id).to_be_bytes()
}

fn fnv64(x: u64) -> u64 {
    let mut h: u64 = 0xCBF2_9CE4_8422_2325;
    for b in x.to_le_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h
}

/// YCSB's Zipfian generator (Gray et al.), scrambled by hashing the rank so
/// popular items spread across the key space.
pub struct Zipfian {
    n: u64,
    theta: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
}

fn zeta(n: u64, theta: f64) -> f64 {
    // Summing in parallel chunks keeps setup fast for hundreds of millions
    // of items.
    let threads = std::thread::available_parallelism().map_or(8, |n| n.get()) as u64;
    let chunk = n.div_ceil(threads);
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                s.spawn(move || {
                    let lo = t * chunk + 1;
                    let hi = ((t + 1) * chunk).min(n);
                    (lo..=hi).map(|i| 1.0 / (i as f64).powf(theta)).sum::<f64>()
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).sum()
    })
}

impl Zipfian {
    pub fn new(n: u64, theta: f64) -> Self {
        let n = n.max(2);
        let zetan = zeta(n, theta);
        let zeta2 = zeta(2, theta);
        let eta = (1.0 - (2.0 / n as f64).powf(1.0 - theta)) / (1.0 - zeta2 / zetan);
        Self { n, theta, alpha: 1.0 / (1.0 - theta), zetan, eta }
    }

    fn next_rank(&self, rng: &mut Rng) -> u64 {
        let u = rng.unit();
        let uz = u * self.zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5f64.powf(self.theta) {
            return 1;
        }
        ((self.n as f64) * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as u64
    }

    pub fn next_scrambled(&self, rng: &mut Rng) -> u64 {
        fnv64(self.next_rank(rng)) % self.n
    }
}
