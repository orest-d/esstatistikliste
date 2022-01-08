use pyo3::prelude::*;

#[pyclass]
struct Setup {
    /// File to import from
    #[pyo3(get, set)]
    input: String,

    /// Input format
    #[pyo3(get, set)]
    format: String,
    
    /// Number of records in an import chunk
    #[pyo3(get, set)]
    chunksize: usize,
}

#[pymethods]
impl Setup {
    #[new]
    pub fn new(input: &str, format:&str, chunksize:usize) -> Self {
        Setup {
          input: input.to_owned(),
          format: format.to_owned(),
          chunksize: chunksize
        }
    }
}

#[pymodule]
fn esstatistikliste(_: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Setup>()?;

    Ok(())
}
