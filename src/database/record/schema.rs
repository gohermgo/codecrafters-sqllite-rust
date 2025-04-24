use crate::database::record::{self, FromRawColumn, RawColumn, RecordHeader};
use crate::io;
use crate::sql;

#[derive(Debug)]
pub struct SchemaColumn {
    pub r#type: Vec<u8>,
    pub name: Vec<u8>,
    pub table_name: Vec<u8>,
    pub rootpage: u8,
    pub sql: sql::CreateTable,
}
impl FromRawColumn for SchemaColumn {
    fn from_raw_column(column: RawColumn) -> io::Result<Self>
    where
        Self: Sized,
    {
        let RawColumn { cells } = column;
        let mut cells = cells.into_iter();
        let mut next = || {
            cells.next().ok_or(io::Error::new(
                io::ErrorKind::InvalidData,
                "cells ran out when iterating for schema-column",
            ))
        };
        let r#type = next().and_then(record::lift_encoded_string)?;
        let name = next().and_then(record::lift_encoded_string)?;
        let table_name = next().and_then(record::lift_encoded_string)?;
        let rootpage = next().and_then(record::lift_twos_complement_8)?;
        let sql = next()
            .and_then(record::lift_encoded_string)
            .and_then(sql::parse)
            .and_then(|elt| {
                sql::lift_create_table(elt).ok_or(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected create-table sql for schema column",
                ))
            })?;
        Ok(SchemaColumn {
            r#type,
            name,
            table_name,
            rootpage,
            sql,
        })
    }
}
#[derive(Debug)]
pub struct SchemaRecord {
    pub header: RecordHeader,
    pub column: SchemaColumn,
}
pub fn pretty_print_schema_column(
    SchemaColumn {
        r#type,
        name,
        table_name,
        rootpage,
        sql,
    }: &SchemaColumn,
) {
    eprintln!("TYPE={}", String::from_utf8_lossy(r#type));
    eprintln!("NAME={}", String::from_utf8_lossy(name));
    eprintln!("TABLE_NAME={}", String::from_utf8_lossy(table_name));
    eprintln!("ROOTPAGE={}", rootpage);
    eprintln!("SQL={:?}", sql);
}
