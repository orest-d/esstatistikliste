use anyhow::Result;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::str;
use xml::reader::{EventReader, XmlEvent};
use serde_json::{Value};
use serde_json::map::{Map};
//use std::cell::RefCell;
//use std::rc::Rc;
use std::collections::{BTreeSet, HashMap};
use polars::prelude::*;

// src/lib.rs

enum ColType {
    String,
    Json,
    Int,
}

static COLUMNS: [(&str, ColType); 18] = [
    ("KoeretoejIdent", ColType::String),
    ("KoeretoejArtNummer", ColType::Int),
    ("KoeretoejArtNavn", ColType::String),
    ("KoeretoejAnvendelseStruktur", ColType::Json),
    ("RegistreringNummerNummer", ColType::String),
    ("RegistreringNummerUdloebDato", ColType::String),
    ("KoeretoejOplysningGrundStruktur", ColType::String),
    ("EjerBrugerSamling", ColType::Json),
    ("KoeretoejRegistreringStatus", ColType::String),
    ("KoeretoejRegistreringStatusDato", ColType::String),
    ("TilladelseSamling", ColType::Json),
    ("SynResultatStruktur", ColType::Json),
    ("AdressePostNummer", ColType::String),
    ("LeasingGyldigFra", ColType::String),
    ("LeasingGyldigTil", ColType::String),
    ("RegistreringNummerRettighedGyldigFra", ColType::String),
    ("RegistreringNummerRettighedGyldigTil", ColType::String),
    ("KoeretoejAnvendelseSamlingStruktur", ColType::Json),
];

fn is_struct(name: &str) -> bool {
    match name {
        "Statistik" => true,
        "KoeretoejAnvendelseStruktur" => true,
        "KoeretoejOplysningGrundStruktur" => true,
        "KoeretoejBetegnelseStruktur" => true,
        "Model" => true,
        "Variant" => true,
        "Type" => true,
        "KoeretoejFarveStruktur" => true,
        "FarveTypeStruktur" => true,
        "KarrosseriTypeStruktur" => true,
        "KoeretoejNormStruktur" => true,
        "NormTypeStruktur" => true,
        "KoeretoejMiljoeOplysningStruktur" => true,
        "KoeretoejMotorStruktur" => true,
        "DrivkraftTypeStruktur" => true,
        "EjerBrugerSamling" => true,
        "EjerBruger" => true,
        "EjerBrugerForholdGrundStruktur" => true,
        "TilladelseSamling" => true,
        "Tilladelse" => true,
        "TilladelseStruktur" => true,
        "TilladelseTypeStruktur" => true,
        "KoeretoejSupplerendeKarrosseriSamlingStruktur" => true,
        "KoeretoejSupplerendeKarrosseriSamling" => true,
        "KoeretoejSupplerendeKarrosseriTypeStruktur" => true,
        "SynResultatStruktur" => true,
        "KoeretoejBlokeringAarsagListeStruktur" => true,
        "KoeretoejBlokeringAarsagListe" => true,
        "KoeretoejBlokeringAarsag" => true,
        "KoeretoejUdstyrSamlingStruktur" => true,
        "KoeretoejUdstyrSamling" => true,
        "KoeretoejUdstyrStruktur" => true,
        "KoeretoejUdstyrTypeStruktur" => true,
        "DispensationTypeSamlingStruktur" => true,
        "DispensationTypeSamling" => true,
        "DispensationTypeStruktur" => true,
        "TilladelseTypeDetaljeValg" => true,
        "KunGodkendtForJuridiskEnhed" => true,
        "JuridiskEnhedIdentifikatorStruktur" => true,
        "JuridiskEnhedValg" => true,
        "KoeretoejAnvendelseSamlingStruktur" => true,
        "KoeretoejAnvendelseSamling" => true,
        "KoeretoejFastKombination" => true,
        "FastTilkobling" => true,
        "VariabelKombination" => true,
        "KoeretoejGenerelIdentifikatorStruktur" => true,
        "KoeretoejGenerelIdentifikatorValg" => true,
        "PENummerCVR" => true,
        _ => false,
    }
}

fn is_array(name: &str) -> bool {
    match name {
        "DispensationTypeSamling" => true,
        "EjerBrugerSamling" => true,
        "KoeretoejAnvendelseSamling" => true,
        "KoeretoejBlokeringAarsagListe" => true,
        "KoeretoejSupplerendeKarrosseriSamling" => true,
        "KoeretoejUdstyrSamling" => true,
        "TilladelseSamling" => true,
        _ => false,
    }
}

#[derive(Debug)]
struct Record {
    element: String,
    is_struct: bool,
    text: String,
    structure: Vec<Record>,
}

impl Record {
    fn new(element: &str) -> Record {
        Record {
            element: String::from(element),
            is_struct: is_struct(element),
            text: String::new(),
            structure: Vec::new(),
        }
    }
    fn add_text(&mut self, text: &str) {
        self.text.push_str(text);
    }
    fn add_child(&mut self, rec: Record) {
        self.structure.push(rec);
    }
    fn get_record(&self,name: &str) -> Option<&Record> {
        for x in &self.structure{
            if x.element == name{
                return Some(x);
            }
        }
        return None;
    }
    fn get(&self,name: &str) -> Option<&String>{
        self.get_record(name).map(
          |x| &x.text,
        )
    }
    fn to_json(&self) -> Value{
        if self.is_struct {
            if is_array(&self.element) {
                let mut array= Vec::new();
                for r in &self.structure {
                    array.push(r.to_json());
                }
                Value::Array(array)
            } else {
                let mut obj = Map::new();
                for r in &self.structure {
                    if obj.contains_key(&r.element) {
                        println!("ERROR: Multiple {} inside {}", r.element, self.element);
                    } else {
                        obj.insert(r.element.clone(), r.to_json());
                    }
                }
                Value::Object(obj)
            }
        } else {
            Value::String(self.text.clone())
        }
    }
}

fn registered(r:&Record)->bool{
    if let Some(status) = r.get("KoeretoejRegistreringStatus"){
        status == "Registreret"
    }
    else{
        false
    }
}

fn flatten_json(json: &Value, failures:&mut HashMap<String,usize>) -> Value{
    let mut flat = Map::new();
    match json{
        Value::Object(m) =>{
            for (key, value) in m.iter(){
                match flatten_json(value, failures){
                    Value::Object(mm) => {                        
                        for (key, value) in m.iter(){
                            if flat.contains_key(key){
                                *failures.entry(key.to_owned()).or_insert(0) += 1;
                            }
                            flat.insert(key.to_owned(),value.clone());
                        }
                    },
                    _ =>{
                        if flat.contains_key(key){
                            *failures.entry(key.to_owned()).or_insert(0) += 1;
                        }
                        flat.insert(key.to_owned(),value.clone());
                    }
                }                
            }
            Value::Object(flat)
        }
        _ => json.clone()    
    }
}

fn flatten_json_array(value: &Value, failures:&mut HashMap<String,usize>) -> Value{
    match value{
        Value::Array(a) =>{
            Value::Array(a.iter().map(|x| flatten_json(x,failures)).collect())
        },
        _ =>{
            flatten_json(&value, failures)
        }
    }
}

fn to_dict_of_lists(it:&[Value]) -> Value{
    let mut arrays:HashMap<String, Vec<Value>> = HashMap::new();
    for column in list_of_dicts_columns(it){
        let mut v:Vec<Value> = Vec::new();
        for row in it{
            match row{
                Value::Object(m)=>{
                    v.push(m.get(&column).unwrap_or(&Value::Null).clone());
                },
                _ => {
                    v.push(Value::Null);
                }
            }    
        }
        arrays.insert(column,v);
    }

    let mut m = Map::new();
    for (k,v) in arrays.into_iter() {
        m.insert(k,Value::Array(v));
    }
    Value::Object(m)
}

fn list_of_dicts_columns(it:&[Value]) -> BTreeSet<String>{
    let mut set = BTreeSet::new();
    for r in it{
        match r{
            Value::Object(m)=>{
                for key in m.keys(){
                    set.insert(key.to_string());
                }
            },
            _ => {
                println!("WARNING: Json object expected, {} obrained",r);
            }
        }
    }
    return set;
}

struct Batch(Vec<Record>);

impl Batch{
    fn new() -> Self{
        Batch(vec![])
    }

    fn column_series(&self, name:&str) -> Series{
        Series::new(name, self.0.iter().map(
            |r| {
                if let Some(r) = r.get_record(name){
                    if r.is_struct{
                        r.to_json().to_string()
                    }
                    else{
                        r.text.to_string()
                    }    
                }
                else{
                    "".to_owned()
                } 
            }
        ).collect::<Vec<String>>())
    } 

    fn columns(&self)->BTreeSet<String>{
        let mut set = BTreeSet::new();
        for r in &self.0{
            for e in &r.structure{
                set.insert(e.element.to_string());
            }
        }
        return set;
    }

    fn dataframe(&self)->Result<DataFrame, PolarsError>{
        DataFrame::new(
          self.columns().iter().map(
              |x| self.column_series(x)
          ).collect::<Vec<_>>()
        )
    }

    fn from_iter(it:&mut impl Iterator<Item=Record>, elements:usize)->Option<Self>{
        let mut batch = Batch::new();
        batch.fill(it, elements);
        if batch.is_empty(){
            None
        }
        else{
            Some(batch)
        }
    }
    fn from_iter_registered(it:&mut impl Iterator<Item=Record>, elements:usize)->Option<Self>{
        let mut batch = Batch::new();
        batch.fill_registered(it, elements);
        if batch.is_empty(){
            None
        }
        else{
            Some(batch)
        }
    }

    fn fill(&mut self, it:&mut impl Iterator<Item=Record>, elements:usize){
        for i in 0..elements{
            if let Some(r) = it.next(){
                self.0.push(r);
            }
            else{
                break;
            }
        }
    }
    fn fill_registered(&mut self, it:&mut impl Iterator<Item=Record>, elements:usize){
        let mut i=0;
        while i<elements{
            if let Some(r) = it.next(){
                if registered(&r){
                    self.0.push(r);
                    i+=1;
                }                
            }
            else{
                break;
            }
        }
    }
    fn len(&self) -> usize{
        self.0.len()
    }
    fn is_empty(&self)->bool{
        self.0.is_empty()
    }
    fn to_json(&self)-> Value{
        Value::Array(self.0.iter().map(|x| x.to_json()).collect())
    }
}
struct PlainRecordIterator<R: Read> {
    event_reader: EventReader<R>,
    stack: Vec<Record>,
}

impl<R: Read> PlainRecordIterator<R> {
    fn from_reader(reader: R) -> Self {
        PlainRecordIterator {
            event_reader: EventReader::new(reader),
            stack: Vec::new(),
        }
    }
}

impl<R: Read> Iterator for PlainRecordIterator<R> {
    type Item = Record;
    fn next(&mut self) -> Option<Self::Item> {
        loop{
            match self.event_reader.next() {
                Ok(XmlEvent::StartElement { name, .. }) => {
                    if !self.stack.is_empty() || name.local_name == "Statistik" {
                        self.stack.push(Record::new(&name.local_name));
                    }
                }
                Ok(XmlEvent::EndElement { name:_ }) => {
                    if let Some(rec) = self.stack.pop() {
                        if self.stack.is_empty() {
                            return Some(rec);
                        } else {
                            if let Some(mut parent) = self.stack.pop() {
                                parent.add_child(rec);
                                self.stack.push(parent);
                            }
                        }
                    }
                }
                Ok(XmlEvent::Characters(text)) => {
                    if let Some(mut rec) = self.stack.pop() {
                        rec.add_text(&text);
                        self.stack.push(rec);
                    }
                }
                Ok(XmlEvent::EndDocument) =>{
                    println!("End");
                    return None;
    
                },
                Err(e) => {
                    println!("Error: {}", e);
                    return None;
                },
                _ => {}
            }
        }
    }
}

fn record_iterator_from_xml_file(path: &str) -> Result<PlainRecordIterator<BufReader<File>>> {
    let file = File::open(path)?;
    Ok(PlainRecordIterator::from_reader(BufReader::new(file)))
}
/*
struct ZipRecordIterator<'a, I: Read> {
    archive: Rc<RefCell<zip::ZipArchive<I>>>,
    file_inside_zip: zip::read::ZipFile<'a>,
    //it:PlainRecordIterator<R>,
}

fn record_iterator_from_zip_file(path: &str) -> Result<ZipRecordIterator<'_,impl Read>>{
    let mut f = File::open(path)?;
    let mut archive = Rc::new(RefCell::new(zip::ZipArchive::new(f)?));
    let mut barchive = archive.clone();
    let mut bb = barchive.borrow_mut();
    let mut file_inside_zip = bb.by_index(0)?;

    Ok(ZipRecordIterator{
        archive: archive,
        file_inside_zip: file_inside_zip,
        //it:PlainRecordIterator::from_reader(BufReader::new(file_inside_zip))
    })
}
*/
/*
fn record_iterator_from_zip_file(path: &str) -> Result<PlainRecordIterator<impl Read>> {
    let file = File::open(path)?;
    let mut archive = Box::new(zip::ZipArchive::new(file)?);
    let mut file_in_archive = Box::new(archive.by_index(0)?); 
    Ok(PlainRecordIterator::from_reader(BufReader::new(&mut file_in_archive)))
}
*/
mod python_module;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        let mut failures = HashMap::new();
        flatten_json(json!())
    }
}