use anyhow::{bail, Result};

use std::env;
use std::fs;
// use std::io;
use std::path::Path;

/// A dot-command has the structure:
///  - It must begin with its "." at the left margin with no preceding whitespace.
///  - It must be entirely contained on a single input line.
///  - It cannot occur in the middle of an ordinary SQL statement, thus it cannot occur at a continuation prompt
///  - There is no comment syntax for dot-commands
pub struct Command<'a> {
    pub name: &'a str,
}

pub const COMMAND_COUNT: usize = 1;
pub const COMMANDS: [Command<'_>; COMMAND_COUNT] = [Command { name: "dbinfo" }];

pub mod btree;
pub mod database;
pub mod io;
pub mod record;
pub mod varint;

pub use record::{Record, RecordElement, RecordHeader, RecordValue};
pub use varint::Varint;

use crate::btree::BTreeCell;

fn main() -> Result<()> {
    let SqliteArgs {
        database_path,
        command,
        remainder: _,
    } = args();
    match command.as_str() {
        ".dbinfo" => db_info_command(database_path)?,
        ".tables" => tables_command(database_path)?,
        otherwise => sql_query_command(database_path, otherwise)?,
        // _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
fn db_info_command(database_path: impl AsRef<Path>) -> io::Result<()> {
    let database = fs::File::open(database_path).and_then(database::read)?;
    println!("database page size: {}", database.header.page_size);
    let number_of_tables = database.content.count();
    println!("number of tables: {number_of_tables}");
    Ok(())
}
fn read_cells(database_path: impl AsRef<Path>) -> io::Result<impl Iterator<Item = BTreeCell>> {
    fs::File::open(database_path).and_then(database::read).map(
        |database::DatabaseFileContent { header, content }| {
            let header_size = core::mem::size_of_val(&header);
            let page_size = header.page_size as usize;
            content
                .filter_map(|database::DatabaseTable(content)| {
                    btree::read_page(&mut content.as_slice()).ok()
                })
                .flat_map(move |page| {
                    let v: Vec<BTreeCell> =
                        btree::read_cells(&page, header_size, page_size).collect();
                    v.into_iter()
                })
        },
    )
}
fn read_records<C: record::FromRawColumn>(
    database_path: impl AsRef<Path>,
) -> io::Result<impl Iterator<Item = record::Record<C>>> {
    read_cells(database_path).map(|cells| cells.filter_map(|cell| btree::parse_cell(cell).ok()))
}
fn tables_command(database_path: impl AsRef<Path>) -> io::Result<()> {
    let database::DatabaseFileContent { header, content } =
        fs::File::open(database_path).and_then(database::read)?;
    let btree_pages = content.filter_map(|database::DatabaseTable(content)| {
        btree::read_page(&mut content.as_slice()).ok()
    });

    for page in btree_pages {
        // eprintln!("Read btree-page {page:?}");
        for cell in btree::read_cells(
            &page,
            core::mem::size_of_val(&header),
            header.page_size as usize,
        ) {
            let rec = btree::parse_cell::<record::SchemaColumn>(cell);
            if let Ok(Record { columns, .. }) = rec {
                columns
                    .iter()
                    .for_each(|record::SchemaColumn { table_name, .. }| {
                        println!("{}", String::from_utf8_lossy(table_name))
                    });
            }
            // let res = rec.and_then(btree::read_schema);
            // eprintln!("SCHEMA {res:?}");
        }
    }

    Ok(())
}
fn sql_query_command(database_path: impl AsRef<Path>, query: impl AsRef<str>) -> io::Result<()> {
    // TODO: Proper query parsing
    let split_query = query.as_ref().split_whitespace();
    eprintln!("SPLIT={split_query:?}");
    let table_name = split_query.last().expect("Empty SQL query!");
    eprintln!("TABLE_NAME={table_name}");

    for record in read_records::<record::RawColumn>(database_path)? {
        eprintln!("RECORD={record:?}");
    }

    Ok(())
}
struct SqliteArgs {
    database_path: String,
    command: String,
    #[allow(dead_code)]
    remainder: env::Args,
}
fn args() -> SqliteArgs {
    let mut args = env::args();
    let Some(_program_name) = args.next() else {
        eprintln!("Shit args, no program name {args:?}");
        panic!("Missing <database path> and <command>");
    };
    let Some(database_path) = args.next() else {
        eprintln!("Shit args, no database path {args:?}");
        panic!("Missing <database path> and <command>");
    };
    let Some(command) = args.next() else {
        eprintln!("Shit args, no command {args:?}");
        panic!("Missing <command>");
    };
    SqliteArgs {
        database_path,
        command,
        remainder: args,
    }
}
