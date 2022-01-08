use anyhow::Result;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::str;
use xml::reader::{EventReader, XmlEvent};
use serde_json::{Value};
use serde_json::map::Map;

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

struct RecordIterator<R: Read> {
    event_reader: EventReader<R>,
    stack: Vec<Record>,
}

impl<R: Read> RecordIterator<R> {
    fn from_reader(reader: R) -> Self {
        RecordIterator {
            event_reader: EventReader::new(reader),
            stack: Vec::new(),
        }
    }
}

impl<R: Read> Iterator for RecordIterator<R> {
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

fn record_iterator_from_xml_file(path: &str) -> Result<RecordIterator<impl Read>> {
    let file = File::open(path)?;
    Ok(RecordIterator::from_reader(BufReader::new(file)))
}

fn record_iterator_from_zip_file(path: &str) -> Result<RecordIterator<impl Read>> {
    let file = File::open(path)?;
    Ok(RecordIterator::from_reader(BufReader::new(file)))
}

mod python_module;
