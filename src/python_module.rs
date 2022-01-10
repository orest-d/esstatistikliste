use pyo3::prelude::*;
use crate::*;
use anyhow;
use pyo3::exceptions::PyValueError;
use pyo3::PyIterProtocol;

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

#[pyclass]
struct XmlJsonIterator {
    /// File to import from
    #[pyo3(get)]
    input: String,
    it:PlainRecordIterator<BufReader<File>>
}

#[pymethods]
impl XmlJsonIterator {
    #[new]
    pub fn new(input: &str) -> PyResult<Self> {
        match record_iterator_from_xml_file(input){
            Ok(it) => Ok(
                XmlJsonIterator {
                  input: input.to_owned(),
                  it:it,
                }),
            Err(e) => Err(PyValueError::new_err(format!("ERROR {}",e)))
        } 
    }
}

#[pyproto]
impl PyIterProtocol for XmlJsonIterator {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<String> {
        slf.it.next().map(|x| x.to_json().to_string())
    }
}


#[pyclass]
struct XmlJsonBatchIterator {
    /// File to import from
    #[pyo3(get)]
    input: String,
    #[pyo3(get)]
    chunksize: usize,
    it:PlainRecordIterator<BufReader<File>>
}

#[pymethods]
impl XmlJsonBatchIterator {
    #[new]
    pub fn new(input: &str, chunksize: usize) -> PyResult<Self> {
        match record_iterator_from_xml_file(input){
            Ok(it) => Ok(
                XmlJsonBatchIterator {
                  input: input.to_owned(),
                  chunksize: chunksize,
                  it:it,
                }),
            Err(e) => Err(PyValueError::new_err(format!("ERROR {}",e)))
        } 
    }
}

#[pyproto]
impl PyIterProtocol for XmlJsonBatchIterator {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<String> {
        let n = slf.chunksize;
        Batch::from_iter(&mut slf.it, n).map(|x| x.to_json().to_string())
    }
}

#[pyclass]
struct XmlRegisteredJsonBatchIterator {
    /// File to import from
    #[pyo3(get)]
    input: String,
    #[pyo3(get)]
    chunksize: usize,
    it:PlainRecordIterator<BufReader<File>>
}

#[pymethods]
impl XmlRegisteredJsonBatchIterator {
    #[new]
    pub fn new(input: &str, chunksize: usize) -> PyResult<Self> {
        match record_iterator_from_xml_file(input){
            Ok(it) => Ok(
                XmlRegisteredJsonBatchIterator {
                  input: input.to_owned(),
                  chunksize: chunksize,
                  it:it,
                }),
            Err(e) => Err(PyValueError::new_err(format!("ERROR {}",e)))
        } 
    }
}

#[pyproto]
impl PyIterProtocol for XmlRegisteredJsonBatchIterator {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<String> {
        let n = slf.chunksize;
        Batch::from_iter_registered(&mut slf.it, n).map(|x| x.to_json().to_string())
    }
}

#[pyclass]
struct XmlRegisteredDictOfListsJsonBatchIterator {
    /// File to import from
    #[pyo3(get)]
    input: String,
    #[pyo3(get)]
    chunksize: usize,
    it:PlainRecordIterator<BufReader<File>>,
}

#[pymethods]
impl XmlRegisteredDictOfListsJsonBatchIterator {
    #[new]
    pub fn new(input: &str, chunksize: usize) -> PyResult<Self> {
        println!("Note, the XmlRegisteredDictOfListsJsonBatchIterator iterator is experimental and it does not work as expected");
        match record_iterator_from_xml_file(input){
            Ok(it) => Ok(
                XmlRegisteredDictOfListsJsonBatchIterator {
                  input: input.to_owned(),
                  chunksize: chunksize,
                  it:it,
                }),
            Err(e) => Err(PyValueError::new_err(format!("ERROR {}",e)))
        } 
    }
}

#[pyproto]
impl PyIterProtocol for XmlRegisteredDictOfListsJsonBatchIterator {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<String> {
        let n = slf.chunksize;
        let mut failures:HashMap<String,usize> = HashMap::new();
        let result = Batch::from_iter_registered(&mut slf.it, n).map(|x| {
            let json = x.to_json();
            let flat = flatten_json_array(&json, &mut failures); 
            match flat{
                Value::Array(a)=>{
                  to_dict_of_lists(&a)
                },
                _ => {flat}
            }.to_string()
        });
        if !failures.is_empty() {
            print!("Failures:");
            for (key, value) in failures.iter() {
              println!("  {}: {}",key, value);
            }
            print!("");
        }
        result
    }
}

#[pymodule]
fn esstatistikliste(_: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Setup>()?;
    m.add_class::<XmlJsonIterator>()?;
    m.add_class::<XmlJsonBatchIterator>()?;
    m.add_class::<XmlRegisteredJsonBatchIterator>()?;
    m.add_class::<XmlRegisteredDictOfListsJsonBatchIterator>()?;
    Ok(())
}
