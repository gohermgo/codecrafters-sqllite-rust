use std::collections::HashMap;

use crate::io;
#[derive(Debug)]
pub enum Sql {
    Select(SqlSelect),
    CreateTable(SqlCreateTable),
}
pub fn parse(data: impl IntoIterator<Item = u8>) -> io::Result<Sql> {
    let v: Vec<u8> = data.into_iter().collect();

    match String::from_utf8(v).map(|s| s.to_lowercase()) {
        Ok(s) if s.starts_with("select") => select(s).map(Sql::Select),
        Ok(s) if s.starts_with("create table") => create_table(s).map(Sql::CreateTable),
        Ok(s) => Err(io::Error::new(std::io::ErrorKind::Unsupported, format!("Unsupported SQL: {s}"))),
        Err(e) => Err(io::Error::new(io::ErrorKind::InvalidInput, e))

    }
}
#[derive(Debug)]
pub struct SqlSelect {
    pub query: String,
    pub source: String,
}
pub fn select(s: impl AsRef<str>) -> io::Result<SqlSelect> {
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
    Ok(SqlSelect {
        query: query.trim().to_string(),
        source: source.trim().to_string(),
    })
}
#[allow(dead_code)]
unsafe fn unwrap_select(sql: Sql) -> SqlSelect {
    match sql {
        Sql::Select(elt) => elt,
        _ => panic!("unwrapped select"),
    }
}
pub fn lift_select(sql: Sql) -> Option<SqlSelect> {
    match sql {
        Sql::Select(elt) => Some(elt),
        _ => None
    }
}
#[derive(Debug)]
pub struct SqlCreateTable {
    pub name: String,
    pub signature: HashMap<String, (usize, String)>,
}
fn create_table(s: impl AsRef<str>) -> io::Result<SqlCreateTable> {
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
    let signature_pieces = signature_pieces.enumerate().map_while(|(term_idx, elt)| {
        elt.split_once(' ')
            .map(|(fst, snd)| (fst.to_string(), ( term_idx, snd.to_string())))
    });
    let signature = HashMap::from_iter(signature_pieces);
    Ok(SqlCreateTable { name, signature })
}
#[allow(dead_code)]
unsafe fn unwrap_create_table(sql: Sql) -> SqlCreateTable {
    match sql {
        Sql::CreateTable(elt) => elt,
        _ => panic!("unwrapped create table")
    }
}
pub fn lift_create_table(sql: Sql) -> Option<SqlCreateTable> {
    match sql {
        Sql::CreateTable(elt) => Some(elt),
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
        assert!(table.is_ok_and(|SqlCreateTable { name, .. }| name == "tablename"))
    }
    #[test]
    fn create_table_signature_matches() {
        let table = parse(CREATE_TABLE.iter().copied()).map(|elt| unsafe {unwrap_create_table(elt)});
        assert!(table.is_ok_and(|SqlCreateTable {signature, ..}|
            signature.get("id").is_some_and(|(_, id)| id == "integer primary key")
            &&
            signature.get("butterscotch").is_some_and(|(_, elt)| elt == "text")
            &&
            signature.get("strawberry").is_some_and(|(_, elt)| elt == "text")
            &&
            signature.get("chocolate").is_some_and(|(_, elt)| elt == "text")
            &&
            signature.get("pistachio").is_some_and(|(_,elt)| elt == "text")
            &&
            signature.get("coffee").is_some_and(|(_,elt)| elt == "text")
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
        assert!(select.is_ok_and(|SqlSelect {query, ..}| query == "butterscotch"))        
    }
    #[test]
    fn select_source_matches() {
        let select = parse(SELECT.iter().copied()).map(|elt| unsafe {unwrap_select(elt)});
        assert!(select.is_ok_and(|SqlSelect {source, ..}| source == "pistachio"))        
    }
}
