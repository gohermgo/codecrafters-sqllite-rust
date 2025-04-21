use core::ffi::c_char;

use crate::io;

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
fn read_header<R: io::Read>(r: &mut R) -> io::Result<DatabaseHeader> {
    let mut buf = [0; 100];
    io::Read::read_exact(r, &mut buf)?;
    let header: DatabaseHeader = unsafe { core::mem::transmute(buf) };
    Ok(header.to_be())
}
#[derive(Debug)]
pub struct RawPage(pub Vec<u8>);
fn read_raw_page<R: io::Read>(r: &mut R, page_size: usize) -> io::Result<RawPage> {
    eprintln!("READING RAWPAGE WITH PAGE_SIZE={page_size}");
    io::read_exact_vec(r, page_size).map(RawPage)
}
#[derive(Debug)]
pub struct RootPage {
    pub database_header: DatabaseHeader,
    pub tail: Vec<u8>,
}
fn read_root_page<R: io::Read>(r: &mut R) -> io::Result<RootPage> {
    let database_header = read_header(r)?;
    let tail_size = database_header.page_size as usize - core::mem::size_of_val(&database_header);
    eprintln!("READING ROOTPAGE WITH TAIL_SIZE={tail_size}");
    io::read_exact_vec(r, tail_size).map(|tail| RootPage {
        database_header,
        tail,
    })
}
#[derive(Debug)]
pub struct DatabaseContent {
    pub root_page: RootPage,
    pub tail: Vec<RawPage>,
}
pub fn read_database<R: io::Read>(r: &mut R) -> io::Result<DatabaseContent> {
    read_root_page(r).map(|root_page| {
        let page_size = root_page.database_header.page_size as usize;
        DatabaseContent {
            root_page,
            tail: core::iter::from_fn(|| read_raw_page(r, page_size).ok()).collect(),
        }
    })
}
#[derive(Debug)]
pub struct DatabaseTable(pub Vec<u8>);
fn read_table<R: io::Read>(r: &mut R, page_size: usize) -> io::Result<DatabaseTable> {
    eprintln!("READING TABLE={page_size}");
    let mut data = vec![0; page_size];
    io::Read::read_exact(r, data.as_mut_slice())?;
    eprintln!("READ TABLE SUCCESS");
    Ok(DatabaseTable(data))
}
#[derive(Debug)]
pub struct DatabaseFileContent<D> {
    pub header: DatabaseHeader,
    pub content: D,
}
pub fn read<R: io::Read + 'static>(
    r: R,
) -> io::Result<DatabaseFileContent<impl Iterator<Item = DatabaseTable>>> {
    read_with(r, move |r, page_size| read_table(r, page_size).ok())
}
pub fn read_with<T, R: io::Read + 'static>(
    mut r: R,
    mut f: impl FnMut(&mut R, usize) -> Option<T> + 'static,
) -> io::Result<DatabaseFileContent<impl Iterator<Item = T>>> {
    read_header(&mut r).map(move |header| {
        eprintln!("Read header {header:?}");
        let page_size = header.page_size as usize;
        let mut read = 0;
        let content = core::iter::from_fn(move || {
            read += 1;
            eprintln!("READING PAGE {read}");
            f(&mut r, page_size)
        });
        DatabaseFileContent { header, content }
    })
}
