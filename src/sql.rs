use std::collections::HashMap;

use crate::io;
#[derive(Debug)]
pub enum Sql {
    Selection(Select),
    Creation(CreateTable),
}
pub fn parse(data: impl IntoIterator<Item = u8>) -> io::Result<Sql> {
    let v: Vec<u8> = data.into_iter().collect();

    match String::from_utf8(v).map(|s| s.to_lowercase()) {
        Ok(s) if s.starts_with("select") => select(s).map(Sql::Selection),
        Ok(s) if s.starts_with("create table") => create_table(s).map(Sql::Creation),
        Ok(s) => Err(io::Error::new(std::io::ErrorKind::Unsupported, format!("Unsupported SQL: {s}"))),
        Err(e) => Err(io::Error::new(io::ErrorKind::InvalidInput, e))

    }
}
#[derive(Debug)]
pub struct Select {
    pub query: String,
    pub source: String,
}
pub fn select(s: impl AsRef<str>) -> io::Result<Select> {
    let remainder = s.as_ref()
        .strip_prefix("select")
        .map(str::trim)
        .ok_or(io::Error::new(
            io::ErrorKind::InvalidData,
            "Failed to strip select prefix from select query",
        ))?;
    let (query, source) = remainder.split_once("from").ok_or(io::Error::new(
        io::ErrorKind::InvalidData,
        "Failed to find keyword from in select query",
    ))?;
    Ok(Select {
        query: query.trim().to_string(),
        source: source.trim().to_string(),
    })
}
#[allow(dead_code)]
unsafe fn unwrap_select(sql: Sql) -> Select {
    match sql {
        Sql::Selection(elt) => elt,
        _ => panic!("unwrapped select"),
    }
}
pub fn lift_select(sql: Sql) -> Option<Select> {
    match sql {
        Sql::Selection(elt) => Some(elt),
        _ => None
    }
}
#[derive(Debug)]
pub struct CreateTable {
    pub name: String,
    pub signature: HashMap<String, String>,
}
fn create_table(s: impl AsRef<str>) -> io::Result<CreateTable> {
    let remainder = s.as_ref()
        .strip_prefix("create table")
        .map(str::trim)
        .ok_or(io::Error::new(
            io::ErrorKind::InvalidData,
            "Expected more SQL string segments",
        ))?;
    let (name, signature_str) = remainder.split_once(' ').ok_or(io::Error::new(
        io::ErrorKind::InvalidData,
        "Failed to split to name and signature group",
    ))?;
    let name = name.to_string();
    let signature_str = signature_str.trim_start_matches('(').trim_end_matches(')');
    let signature_pieces = signature_str.split(',').map(str::trim);
    let signature_pieces = signature_pieces.map_while(|elt| {
        elt.split_once(' ')
            .map(|(fst, snd)| (fst.to_string(), snd.to_string()))
    });
    let signature = HashMap::from_iter(signature_pieces);
    Ok(CreateTable { name, signature })
}
#[allow(dead_code)]
unsafe fn unwrap_create_table(sql: Sql) -> CreateTable {
    match sql {
        Sql::Creation(elt) => elt,
        _ => panic!("unwrapped create table")
    }
}
pub fn lift_create_table(sql: Sql) -> Option<CreateTable> {
    match sql {
        Sql::Creation(elt) => Some(elt),
        _ => None
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    const CREATE_TABLE: &[u8] = 
b"CREATE TABLE tablename (id integer primary key, butterscotch text,strawberry text,chocolate text,pistachio text,coffee text)";
    #[test]
    fn create_table_is_ok() {
        let table = parse(CREATE_TABLE.iter().copied()).map(|elt| unsafe {unwrap_create_table(elt)});
        assert!(table.is_ok());
    }
    #[test]
    fn create_table_name_matches() {
        let table = parse(CREATE_TABLE.iter().copied()).map(|elt| unsafe {unwrap_create_table(elt)});
        assert!(table.is_ok_and(|CreateTable { name, .. }| name == "tablename"))
    }
    #[test]
    fn create_table_signature_matches() {
        let table = parse(CREATE_TABLE.iter().copied()).map(|elt| unsafe {unwrap_create_table(elt)});
        assert!(table.is_ok_and(|CreateTable {signature, ..}|
            signature.get("id").is_some_and(|id| id == "integer primary key")
            &&
            signature.get("butterscotch").is_some_and(|elt| elt == "text")
            &&
            signature.get("strawberry").is_some_and(|elt| elt == "text")
            &&
            signature.get("chocolate").is_some_and(|elt| elt == "text")
            &&
            signature.get("pistachio").is_some_and(|elt| elt == "text")
            &&
            signature.get("coffee").is_some_and(|elt| elt == "text")
        ))
        
    }
    const SELECT: &[u8] = b"SELECT butterscotch FROM pistachio";
    #[test]
    fn select_is_ok() {
        let select = parse(SELECT.iter().copied()).map(|elt| unsafe {unwrap_select(elt)});
        assert!(select.is_ok())
    }
    #[test]
    fn select_query_matches() {
        let select = parse(SELECT.iter().copied()).map(|elt| unsafe {unwrap_select(elt)});
        assert!(select.is_ok_and(|Select {query, ..}| query == "butterscotch"))        
    }
    #[test]
    fn select_source_matches() {
        let select = parse(SELECT.iter().copied()).map(|elt| unsafe {unwrap_select(elt)});
        assert!(select.is_ok_and(|Select {source, ..}| source == "pistachio"))        
    }
}
