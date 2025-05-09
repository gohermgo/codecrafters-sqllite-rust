use anyhow::Result;

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

mod database;
mod io;
mod sql;
mod varint;

pub use varint::Varint;

fn main() -> Result<()> {
    let dir = std::env::current_dir().and_then(fs::read_dir)?;
    for elt in dir {
        eprintln!("ENTRY={elt:?}");
    }
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
    if let Ok(database::Database {
        header,
        record_cells,
        ..
    }) = database::open(database_path)
    {
        println!("database page size: {}", header.page_size);
        let number_of_tables = record_cells.len();
        println!("number of tables: {number_of_tables}");
    }
    // let database = fs::File::open(database_path).and_then(database::read)?;
    // println!("database page size: {}", database.header.page_size);
    // let number_of_tables = database.content.count();
    // println!("number of tables: {number_of_tables}");
    Ok(())
}
fn tables_command(database_path: impl AsRef<Path>) -> io::Result<()> {
    if let Ok(database::Database { schema_cells, .. }) = database::open(database_path) {
        for schema in schema_cells {
            println!("{}", String::from_utf8_lossy(&schema.column.table_name));
        }
    }
    Ok(())
}
fn sql_query_command(database_path: impl AsRef<Path>, query: impl AsRef<str>) -> io::Result<()> {
    // TODO: Proper query parsing
    let query = sql::parse(query.as_ref().bytes())?;
    if let Ok(database::Database {
        schema_cells,
        record_cells,
        ..
    }) = database::open(database_path)
    {
        match query {
            sql::Sql::Select(sql::SqlSelect { query, source }) => {
                let xs: Vec<_> = schema_cells.iter().zip(record_cells).collect();
                eprintln!("LENGTH={}", xs.len());
                for (schema, data) in xs {
                    eprintln!("DATA_LENGTH={}", data.len());
                    match schema.column.sql.signature.get(&query) {
                        Some((term_idx, x)) => {
                            for record in data.as_slice() {
                                eprintln!(
                                    "found data type {x} at index {term_idx} for signature {query}"
                                );
                                let Some(value) = record.column.cells.get(*term_idx).cloned()
                                else {
                                    eprintln!("No term at {term_idx}");
                                    continue;
                                };
                                let Ok(value) = database::lift_encoded_string(value) else {
                                    eprintln!("could not lift encoded string");
                                    continue;
                                };
                                let query_result = String::from_utf8_lossy(&value);
                                println!("{query_result}");
                            }
                        }
                        None => eprintln!("source {source} missing signature {query}"),
                    }
                }
            }
            sql::Sql::CreateTable(_) => todo!("creating tables is not yet supported"),
        }
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
