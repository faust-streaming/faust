//! Rust ports of Faust's Cython accelerators, for the evaluation in
//! `docs/proposals/rust-acceleration.md`.
//!
//! Not built by default. See that document for the `USE_RUST` build wiring
//! and for what the measurements mean.

use pyo3::prelude::*;
use pyo3::types::{PyInt, PyList, PyTuple};

/// Port of `faust._cython.functional.first_consecutive_run`.
///
/// Kept deliberately faithful to the Cython version so the comparison is
/// like-for-like: the same two-tier structure (an `i64` fast path for exact
/// ints, a Python-arithmetic slow path for everything else), and the same
/// rule that a run continues only while each number is exactly one greater
/// than the one before it.
#[pyfunction]
fn first_consecutive_run<'py>(numbers: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyList>> {
    match numbers.cast::<PyList>() {
        Ok(seq) => run_from_list(seq),
        Err(_) => run_from_iterable(numbers),
    }
}

fn run_from_list<'py>(seq: &Bound<'py, PyList>) -> PyResult<Bound<'py, PyList>> {
    let py = seq.py();
    if seq.len() == 0 {
        return Ok(PyList::empty(py));
    }

    let mut prev = seq.get_item(0)?;
    let out = PyList::empty(py);
    out.append(&prev)?;
    let mut i: usize = 1;

    // Fast path: exact ints that fit in an i64.  `is_exact_instance_of`
    // matches Cython's PyLong_CheckExact, so bool (an int subclass) falls
    // through to the slow path in both implementations.
    if prev.is_exact_instance_of::<PyInt>() {
        if let Ok(mut c_prev) = prev.extract::<i64>() {
            // The length is re-read every iteration: nothing in this loop can
            // run Python code, but the list is the caller's and staying in
            // step with the Cython version costs nothing measurable.
            while i < seq.len() {
                let cur = seq.get_item(i)?;
                if !cur.is_exact_instance_of::<PyInt>() {
                    break;
                }
                let c_cur = match cur.extract::<i64>() {
                    Ok(value) => value,
                    Err(_) => break,
                };
                if c_prev.checked_add(1) != Some(c_cur) {
                    break;
                }
                out.append(&cur)?;
                c_prev = c_cur;
                i += 1;
            }
            if i >= seq.len() {
                return Ok(out);
            }
            prev = seq.get_item(i - 1)?;
        }
    }

    // Slow path: arbitrary objects supporting `-` and comparison to 1.
    while i < seq.len() {
        let cur = seq.get_item(i)?;
        if !cur.sub(&prev)?.eq(1i64)? {
            break;
        }
        out.append(&cur)?;
        prev = cur;
        i += 1;
    }
    Ok(out)
}

fn run_from_iterable<'py>(numbers: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyList>> {
    let py = numbers.py();
    let mut it = numbers.try_iter()?;
    let out = PyList::empty(py);

    let mut prev = match it.next() {
        None => return Ok(out),
        Some(first) => first?,
    };
    out.append(&prev)?;

    // Stops consuming as soon as the run is broken, so a shared iterator is
    // left exactly where the Python and Cython versions leave it.
    for cur in it {
        let cur = cur?;
        if !cur.sub(&prev)?.eq(1i64)? {
            break;
        }
        out.append(&cur)?;
        prev = cur;
    }
    Ok(out)
}

/// Length of the first consecutive run, without materialising it.
///
/// Not a drop-in for anything Faust calls -- it exists to separate the two
/// costs the benchmark keeps conflating: scanning the input, and building the
/// Python list of results. See the proposal for why that distinction is the
/// whole answer.
#[pyfunction]
fn first_consecutive_run_length(seq: &Bound<'_, PyList>) -> PyResult<usize> {
    if seq.len() == 0 {
        return Ok(0);
    }
    let first = seq.get_item(0)?;
    let mut c_prev = match first.extract::<i64>() {
        Ok(value) => value,
        Err(_) => return Ok(1),
    };
    let mut count: usize = 1;
    let mut i: usize = 1;
    while i < seq.len() {
        let cur = seq.get_item(i)?;
        let c_cur = match cur.extract::<i64>() {
            Ok(value) => value,
            Err(_) => break,
        };
        if c_prev.checked_add(1) != Some(c_cur) {
            break;
        }
        count += 1;
        c_prev = c_cur;
        i += 1;
    }
    Ok(count)
}

/// Port of `faust._cython.windows.HoppingWindow`.
///
/// Only the float paths are implemented: enough to reproduce the numbers in
/// the proposal, not enough to stand in for the real class.
#[pyclass]
struct HoppingWindow {
    #[pyo3(get)]
    size: f64,
    #[pyo3(get)]
    step: f64,
    #[pyo3(get)]
    expires: f64,
}

impl HoppingWindow {
    fn start_initial_range(&self, timestamp: f64) -> f64 {
        let rem = (timestamp / self.step).floor() as i64;
        (rem as f64) * self.step - self.size + self.step
    }

    fn current_start(&self, timestamp: f64, start: f64) -> f64 {
        let m = ((timestamp - start) / self.step).floor();
        start + (self.step * m)
    }
}

#[pymethods]
impl HoppingWindow {
    #[new]
    #[pyo3(signature = (size, step, expires = 0.0))]
    fn new(size: f64, step: f64, expires: f64) -> Self {
        HoppingWindow {
            size,
            step,
            expires,
        }
    }

    fn ranges<'py>(&self, py: Python<'py>, timestamp: f64) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        let mut start = self.start_initial_range(timestamp) as i64;
        let stop = timestamp as i64;
        let step = self.step as i64;
        while start <= stop {
            let begin = start as f64;
            out.append(PyTuple::new(py, [begin, begin + self.size - 0.1])?)?;
            start += step;
        }
        Ok(out)
    }

    fn current<'py>(&self, py: Python<'py>, timestamp: f64) -> PyResult<Bound<'py, PyTuple>> {
        let initial = self.start_initial_range(timestamp);
        let start = self.current_start(timestamp, initial);
        PyTuple::new(py, [start, start + self.size - 0.1])
    }

    fn earliest<'py>(&self, py: Python<'py>, timestamp: f64) -> PyResult<Bound<'py, PyTuple>> {
        let start = self.start_initial_range(timestamp);
        PyTuple::new(py, [start, start + self.size - 0.1])
    }

    fn delta<'py>(&self, py: Python<'py>, timestamp: f64, d: f64) -> PyResult<Bound<'py, PyTuple>> {
        self.current(py, timestamp - d)
    }

    fn stale(&self, timestamp: f64, latest_timestamp: f64) -> bool {
        if self.expires == 0.0 {
            return false;
        }
        let ts = latest_timestamp - self.expires;
        let initial = self.start_initial_range(ts);
        timestamp <= self.current_start(ts, initial)
    }
}

/// Same as `_ffi`, but using the CPython macros that the limited API hides.
/// Only compiles without `abi3`.
#[cfg(not(feature = "abi3"))]
#[pyfunction]
fn first_consecutive_run_macro<'py>(seq: &Bound<'py, PyList>) -> PyResult<Bound<'py, PyList>> {
    use pyo3::ffi;
    let py = seq.py();
    let out = PyList::empty(py);
    let raw = seq.as_ptr();
    unsafe {
        let n = ffi::PyList_GET_SIZE(raw);
        if n == 0 {
            return Ok(out);
        }
        let mut item = ffi::PyList_GET_ITEM(raw, 0);
        ffi::PyList_Append(out.as_ptr(), item);
        let mut overflow: std::os::raw::c_int = 0;
        let mut c_prev = ffi::PyLong_AsLongLongAndOverflow(item, &mut overflow);
        if overflow != 0 {
            return Ok(out);
        }
        let mut i: ffi::Py_ssize_t = 1;
        while i < n {
            item = ffi::PyList_GET_ITEM(raw, i);
            let c_cur = ffi::PyLong_AsLongLongAndOverflow(item, &mut overflow);
            if overflow != 0 || c_cur <= c_prev || c_cur - 1 != c_prev {
                break;
            }
            ffi::PyList_Append(out.as_ptr(), item);
            c_prev = c_cur;
            i += 1;
        }
    }
    Ok(out)
}

/// The same scan over data that is already native.
///
/// Builds `0..n` as a `Vec<i64>` and finds the first consecutive run in it.
/// Nothing here touches a Python object, so this is the ceiling: what the
/// scan costs once the per-element Python object work is removed.
#[pyfunction]
fn scan_native(n: usize) -> usize {
    let data: Vec<i64> = (0..n as i64).collect();
    if data.is_empty() {
        return 0;
    }
    let mut count = 1usize;
    let mut prev = data[0];
    for &cur in &data[1..] {
        if cur != prev + 1 {
            break;
        }
        count += 1;
        prev = cur;
    }
    count
}

#[pymodule]
fn _accel(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(first_consecutive_run, m)?)?;
    m.add_function(wrap_pyfunction!(first_consecutive_run_length, m)?)?;
    m.add_function(wrap_pyfunction!(first_consecutive_run_ffi, m)?)?;
    #[cfg(not(feature = "abi3"))]
    m.add_function(wrap_pyfunction!(first_consecutive_run_macro, m)?)?;
    m.add_function(wrap_pyfunction!(scan_native, m)?)?;
    m.add_class::<HoppingWindow>()?;
    Ok(())
}

/// Raw-FFI variant of `first_consecutive_run`, list fast path only.
///
/// The idiomatic version above pays for a bounds-checked `get_item` that
/// returns an owned `Bound` (a refcount round-trip per element) and for
/// PyO3's `extract` machinery. This one calls the same C API the Cython
/// version compiles down to, to find out how much of the gap is PyO3 and how
/// much is the language.
#[pyfunction]
fn first_consecutive_run_ffi<'py>(seq: &Bound<'py, PyList>) -> PyResult<Bound<'py, PyList>> {
    use pyo3::ffi;
    let py = seq.py();
    let out = PyList::empty(py);
    let raw = seq.as_ptr();
    unsafe {
        let n = ffi::PyList_Size(raw);
        if n == 0 {
            return Ok(out);
        }
        let mut item = ffi::PyList_GetItem(raw, 0); // borrowed
        ffi::PyList_Append(out.as_ptr(), item);
        let mut overflow: std::os::raw::c_int = 0;
        let mut c_prev = ffi::PyLong_AsLongLongAndOverflow(item, &mut overflow);
        if overflow != 0 {
            return Ok(out);
        }
        let mut i: ffi::Py_ssize_t = 1;
        while i < n {
            item = ffi::PyList_GetItem(raw, i);
            let c_cur = ffi::PyLong_AsLongLongAndOverflow(item, &mut overflow);
            if overflow != 0 || c_cur <= c_prev || c_cur - 1 != c_prev {
                break;
            }
            ffi::PyList_Append(out.as_ptr(), item);
            c_prev = c_cur;
            i += 1;
        }
    }
    Ok(out)
}
