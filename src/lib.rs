use std::path::PathBuf;

use pyo3::{
    create_exception,
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
    types::PyBytes,
};
use sled::{Db, IVec, Tree};

create_exception!(pysled, PySledError, PyRuntimeError);

fn to_pyerr(err: sled::Error) -> PyErr {
    PySledError::new_err(format!("{err:?}"))
}

fn to_bytes(vec: IVec) -> Py<PyBytes> {
    Python::with_gil(|py| PyBytes::new(py, &vec).into())
}

fn to_maybe_bytes(maybe_vec: Option<IVec>) -> Option<Py<PyBytes>> {
    Python::with_gil(|py| maybe_vec.map(|v| PyBytes::new(py, &v).into()))
}

fn to_maybe_bytes_result(res: Result<Option<IVec>, sled::Error>) -> PyResult<Option<Py<PyBytes>>> {
    match res {
        Ok(maybe_vec) => Ok(to_maybe_bytes(maybe_vec)),
        Err(err) => Err(to_pyerr(err)),
    }
}

#[pyclass]
pub struct CompareAndSwapError {
    #[pyo3(get, set)]
    pub current: Option<Py<PyBytes>>,
    #[pyo3(get, set)]
    pub proposed: Option<Py<PyBytes>>,
}

#[pyclass]
pub struct SledDb {
    inner: Db,
}

#[pymethods]
impl SledDb {
    #[new]
    pub fn new(path: PathBuf) -> PyResult<Self> {
        let inner = sled::open(&path)
            .map_err(|e| PyValueError::new_err(format!("Failed to open db: {}", e)))?;
        Ok(Self { inner })
    }

    pub fn insert(&self, key: &[u8], value: Vec<u8>) -> PyResult<Option<Py<PyBytes>>> {
        to_maybe_bytes_result(self.inner.insert(key, value))
    }

    pub fn get(&self, key: &[u8]) -> PyResult<Option<Py<PyBytes>>> {
        to_maybe_bytes_result(self.inner.get(key))
    }

    pub fn remove(&self, key: &[u8]) -> PyResult<Option<Py<PyBytes>>> {
        to_maybe_bytes_result(self.inner.remove(key))
    }

    pub fn clear(&self) -> PyResult<()> {
        self.inner.clear().map_err(to_pyerr)
    }

    pub fn all(&self) -> PyResult<Vec<(Py<PyBytes>, Py<PyBytes>)>> {
        let mut out = Vec::new();
        let iter = self.inner.iter();
        out.reserve(iter.size_hint().0);
        for e in iter {
            let (a, b) = e.map_err(to_pyerr)?;
            out.push((to_bytes(a), to_bytes(b)));
        }
        Ok(out)
    }

    pub fn compare_and_swamp(
        &self,
        key: &[u8],
        old: Option<&[u8]>,
        new: Option<Vec<u8>>,
    ) -> PyResult<Option<CompareAndSwapError>> {
        match self.inner.compare_and_swap(key, old, new) {
            Ok(e) => { match e {
                Ok(_) => Ok(None),
                Err(i) => Ok(Some(CompareAndSwapError{
                    current: i.current.map(to_bytes),
                    proposed: i.proposed.map(to_bytes),
                }))
            }},
            Err(err) => Err(to_pyerr(err))
        }
    }

    pub fn checksum(&self) -> PyResult<u32> {
        self.inner.checksum().map_err(to_pyerr)
    }

    pub fn flush(&self) -> PyResult<usize> {
        self.inner.flush().map_err(to_pyerr)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn __len__(&self) -> usize {
        self.inner.len()
    }

    pub fn __contains__(&self, key: &[u8]) -> PyResult<bool> {
        self.inner.contains_key(key).map_err(to_pyerr)
    }

    pub fn __getitem__(&self, key: &[u8]) -> PyResult<Option<Py<PyBytes>>> {
        self.get(key)
    }

    pub fn __setitem__(&self, key: &[u8], value: Vec<u8>) -> PyResult<()> {
        self.insert(key, value).map(|_| ())
    }

    pub fn __delitem__(&self, key: &[u8]) -> PyResult<()> {
        self.remove(key).map(|_| ())
    }

    #[getter]
    pub fn name(&self) -> Py<PyBytes> {
        to_bytes(self.inner.name())
    }

    pub fn contains_key(&self, key: &[u8]) -> PyResult<bool> {
        self.inner.contains_key(key).map_err(to_pyerr)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn open_tree(&self, name: &[u8]) -> PyResult<SledTree> {
        match self.inner.open_tree(name) {
            Ok(tree) => Ok(SledTree { inner: tree }),
            Err(err) => Err(to_pyerr(err))
        }
    }

    pub fn drop_tree(&self, name: &[u8]) -> PyResult<bool> {
        self.inner.drop_tree(name).map_err(to_pyerr)
    }

    pub fn size_on_disk(&self) -> PyResult<u64> {
        self.inner.size_on_disk().map_err(to_pyerr)
    }
}

#[pyclass(mapping)]
pub struct SledTree {
    inner: Tree,
}

#[pymethods]
impl SledTree {
     pub fn insert(&self, key: &[u8], value: Vec<u8>) -> PyResult<Option<Py<PyBytes>>> {
        to_maybe_bytes_result(self.inner.insert(key, value))
    }

    pub fn get(&self, key: &[u8]) -> PyResult<Option<Py<PyBytes>>> {
        to_maybe_bytes_result(self.inner.get(key))
    }

    pub fn remove(&self, key: &[u8]) -> PyResult<Option<Py<PyBytes>>> {
        to_maybe_bytes_result(self.inner.remove(key))
    }

    pub fn clear(&self) -> PyResult<()> {
        self.inner.clear().map_err(to_pyerr)
    }

    pub fn all(&self) -> PyResult<Vec<(Py<PyBytes>, Py<PyBytes>)>> {
        let mut out = Vec::new();
        let iter = self.inner.iter();
        out.reserve(iter.size_hint().0);
        for e in iter {
            let (a, b) = e.map_err(to_pyerr)?;
            out.push((to_bytes(a), to_bytes(b)));
        }
        Ok(out)
    }

    pub fn compare_and_swamp(
        &self,
        key: &[u8],
        old: Option<&[u8]>,
        new: Option<Vec<u8>>,
    ) -> PyResult<Option<CompareAndSwapError>> {
        match self.inner.compare_and_swap(key, old, new) {
            Ok(e) => { match e {
                Ok(_) => Ok(None),
                Err(i) => Ok(Some(CompareAndSwapError{
                    current: i.current.map(to_bytes),
                    proposed: i.proposed.map(to_bytes),
                }))
            }},
            Err(err) => Err(to_pyerr(err))
        }
    }

    pub fn checksum(&self) -> PyResult<u32> {
        self.inner.checksum().map_err(to_pyerr)
    }

    pub fn flush(&self) -> PyResult<usize> {
        self.inner.flush().map_err(to_pyerr)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn __len__(&self) -> usize {
        self.inner.len()
    }

    pub fn __contains__(&self, key: &[u8]) -> PyResult<bool> {
        self.inner.contains_key(key).map_err(to_pyerr)
    }

    pub fn __getitem__(&self, key: &[u8]) -> PyResult<Option<Py<PyBytes>>> {
        self.get(key)
    }

    pub fn __setitem__(&self, key: &[u8], value: Vec<u8>) -> PyResult<()> {
        self.insert(key, value).map(|_| ())
    }

    pub fn __delitem__(&self, key: &[u8]) -> PyResult<()> {
        self.remove(key).map(|_| ())
    }

    #[getter]
    pub fn name(&self) -> Py<PyBytes> {
        to_bytes(self.inner.name())
    }
}

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn pysled(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<SledDb>()?;
    m.add_class::<SledTree>()?;
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add("PySledError", _py.get_type::<PySledError>())?;
    Ok(())
}
