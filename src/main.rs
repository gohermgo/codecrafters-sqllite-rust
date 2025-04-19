use core::ffi::c_char;

use anyhow::{bail, Result};

use std::fs;
use std::io;

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

const HEADER_STRING_SIZE: usize = 16;
const HEADER_RESERVED_SIZE: usize = 20;
#[derive(Debug)]
#[repr(C)]
pub struct DatabaseHeader {
    /// The header string: "SQLite format 3\000"
    pub header_string: [c_char; HEADER_STRING_SIZE],
    /// The database page size in bytes.
    ///
    /// Must be a power of two between 512 and 32768 inclusive,
    /// or the value 1 representing a page size of 65536.
    pub page_size: u16,
    /// File format write version.
    ///
    /// 1 for Legacy; 2 for WAL.
    pub file_format_write_version: u8,
    /// File format read version.
    ///
    /// 1 for Legacy; 2 for WAL.
    pub file_format_read_version: u8,
    /// Bytes of "unused" reserved space at the end of each page.
    ///
    /// Usually 0
    pub reserved_page_tail_bytes: u8,
    /// Must be 64
    pub maximum_embedded_payload_fraction: u8,
    /// Must be 32
    pub minimum_embedded_payload_fraction: u8,
    /// Must be 32
    pub leaf_payload_fraction: u8,
    pub file_change_counter: u32,
    /// Size of database file in pages.
    pub in_header_database_size: u32,
    /// Page number of the first freelist trunk page
    pub freelist_page_idx: u32,
    /// Total number of freelist pages
    pub freelist_page_count: u32,
    /// The schema cookie
    pub cookie: u32,
    /// The schema format number.
    ///
    /// Supported schema formats are 1, 2, 3, and 4.
    pub format_number: u32,
    /// The default page cache-size
    pub page_cache_size: u32,
    /// The page number of the largest root b-tree page when in
    /// auto-vacuum or incremental-vacuum modes, zero otherwise.
    pub largest_root_page_idx: u32,
    /// The database text encoding.
    ///
    /// A value of 1 means UTF-8.
    /// A value of 2 means UTF-16LE.
    /// A value of 3 means UTF-16BE.
    pub text_encoding: u32,
    /// The "user version" as read and set by the user_version pragma
    pub user_version: u32,
    /// True (non-zero) for incremental-vacuum mode.
    /// Fales (zero) otherwise.
    pub incremental_vacuum_enabled: u32,
    /// The "Application ID" set by the application_id pragma
    pub application_id: u32,
    /// Reserved for expansion. Must be zero.
    pub _reserved: [u8; HEADER_RESERVED_SIZE],
    pub version_valid_for: u32,
    pub sqlite_version_number: u32,
}
impl DatabaseHeader {
    pub const fn to_be(self) -> DatabaseHeader {
        let DatabaseHeader {
            page_size,
            file_change_counter,
            in_header_database_size,
            freelist_page_idx,
            freelist_page_count,
            cookie,
            format_number,
            page_cache_size,
            largest_root_page_idx,
            text_encoding,
            user_version,
            incremental_vacuum_enabled,
            application_id,
            version_valid_for,
            sqlite_version_number,
            ..
        } = self;
        DatabaseHeader {
            page_size: page_size.to_be(),
            file_change_counter: file_change_counter.to_be(),
            in_header_database_size: in_header_database_size.to_be(),
            freelist_page_idx: freelist_page_idx.to_be(),
            freelist_page_count: freelist_page_count.to_be(),
            cookie: cookie.to_be(),
            format_number: format_number.to_be(),
            page_cache_size: page_cache_size.to_be(),
            largest_root_page_idx: largest_root_page_idx.to_be(),
            text_encoding: text_encoding.to_be(),
            user_version: user_version.to_be(),
            incremental_vacuum_enabled: incremental_vacuum_enabled.to_be(),
            application_id: application_id.to_be(),
            version_valid_for: version_valid_for.to_be(),
            sqlite_version_number: sqlite_version_number.to_be(),
            ..self
        }
    }
}
fn read_database_header<R: io::Read>(r: &mut R) -> io::Result<DatabaseHeader> {
    let mut buf = [0; 100];
    io::Read::read_exact(r, &mut buf)?;
    let header: DatabaseHeader = unsafe { core::mem::transmute(buf) };
    Ok(header.to_be())
}
#[derive(Debug)]
pub struct DatabaseTable {
    data: Vec<u8>,
}
fn read_database_table<R: io::Read>(r: &mut R, page_size: usize) -> io::Result<DatabaseTable> {
    let mut data = vec![0; page_size];
    io::Read::read_exact(r, data.as_mut_slice())?;
    Ok(DatabaseTable { data })
}
#[derive(Debug)]
pub struct DatabaseFileContent<D> {
    pub header: DatabaseHeader,
    pub content: D,
}
fn read_database<'r, R: io::Read>(
    r: &'r mut R,
) -> io::Result<DatabaseFileContent<impl Iterator<Item = DatabaseTable> + 'r>> {
    read_database_header(r).map(|header| {
        let page_size = header.page_size as usize;
        DatabaseFileContent {
            header,
            content: core::iter::from_fn(move || read_database_table(r, page_size).ok()),
        }
    })
}

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = fs::File::open(&args[1])?;
            let database = read_database(&mut file)?;
            // let header = read_database_header(&mut file)?;
            eprintln!("Read table with header {:#?}", database.header);

            // You can use print statements as follows for debugging, they'll be visible when running tests.
            println!("Logs from your program will appear here!");

            // Uncomment this block to pass the first stage
            // println!("database page size: {}", database.header.page_size);
            let number_of_tables = database.content.count();
            println!("number of tables: {number_of_tables}");
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
