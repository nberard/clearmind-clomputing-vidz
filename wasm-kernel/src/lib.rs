//! Same workload as the JavaScript client (64x64 f32 matrix multiply), so a
//! phone's score in JS and in WebAssembly can be compared directly.
//! No wasm-bindgen: plain exported functions, loaded with
//! WebAssembly.instantiate in join.html.

const N: usize = 64;

static mut A: [f32; N * N] = [0.0; N * N];
static mut C: [f32; N * N] = [0.0; N * N];

/// Fill the input matrix with deterministic pseudo-random values.
#[no_mangle]
pub extern "C" fn init(seed: u32) {
    let mut x = seed.max(1);
    // SAFETY: wasm is single-threaded here; only these exported functions touch the buffers.
    unsafe {
        for v in (*core::ptr::addr_of_mut!(A)).iter_mut() {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            *v = (x % 1000) as f32 / 1000.0;
        }
    }
}

/// Run `n` matrix multiplications. Returns a checksum so the work can't be optimised away.
#[no_mangle]
pub extern "C" fn run(n: u32) -> f32 {
    // SAFETY: see init().
    let (a, c) = unsafe { (&*core::ptr::addr_of!(A), &mut *core::ptr::addr_of_mut!(C)) };
    for _ in 0..n {
        matmul(a, c);
    }
    c[0] + c[N * N - 1]
}

/// c += a * a, in i-k-j order: the inner loop runs over contiguous memory, so
/// the compiler turns it into SIMD instructions (simd128).
#[inline(always)]
fn matmul(a: &[f32; N * N], c: &mut [f32; N * N]) {
    for i in 0..N {
        let row = &mut c[i * N..(i + 1) * N];
        for k in 0..N {
            let v = a[i * N + k];
            let b = &a[k * N..(k + 1) * N];
            for j in 0..N {
                row[j] += v * b[j];
            }
        }
    }
}
