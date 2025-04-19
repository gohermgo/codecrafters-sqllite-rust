use anyhow::{bail, Result};

use std::env;
use std::fs;
use std::io;
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

pub mod database;

fn db_info_command(database_path: impl AsRef<Path>) -> io::Result<()> {
    let database = fs::File::open(database_path).and_then(database::read)?;
    println!("database page size: {}", database.header.page_size);
    let number_of_tables = database.content.count();
    println!("number of tables: {number_of_tables}");
    Ok(())
}
struct SqliteArgs {
    database_path: String,
    command: String,
    #[expect(dead_code)]
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
fn main() -> Result<()> {
    let SqliteArgs {
        database_path,
        command,
        remainder: _,
    } = args();
    match command.as_str() {
        ".dbinfo" => db_info_command(database_path)?,
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
