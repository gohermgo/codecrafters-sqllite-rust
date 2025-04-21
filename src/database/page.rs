use crate::database;
use crate::database::{btree, DatabaseHeader};
use crate::io;
use crate::record;

pub trait FromRawPage {
    fn from_raw_page(raw_page: RawPage) -> io::Result<Self>
    where
        Self: Sized;
}

#[derive(Debug)]
pub struct RawPage(pub Vec<u8>);
fn read_raw_page<R: io::Read>(r: &mut R, page_size: usize) -> io::Result<RawPage> {
    let raw = io::read_exact_vec(r, page_size).map(RawPage)?;
    eprintln!("READ RAWPAGE WITH PAGE_SIZE={page_size}");
    Ok(raw)
}
#[derive(Debug)]
pub struct RootPage<T> {
    pub database_header: DatabaseHeader,
    pub tail: T,
}
fn read_root_page<R: io::Read>(r: &mut R) -> io::Result<RootPage<RawPage>> {
    let database_header = database::read_header(r)?;
    let tail_size = database_header.page_size as usize - core::mem::size_of_val(&database_header);
    eprintln!("READING ROOTPAGE");
    read_raw_page(r, tail_size).map(|tail| RootPage {
        database_header,
        tail,
    })
}
fn convert_root_page<T: FromRawPage>(
    RootPage {
        database_header,
        tail,
    }: RootPage<RawPage>,
) -> io::Result<RootPage<T>> {
    T::from_raw_page(tail).map(|tail| RootPage {
        database_header,
        tail,
    })
}
#[derive(Debug)]
pub struct Pages<T> {
    pub root_page: RootPage<T>,
    pub tail: Vec<T>,
}
pub type RawPages = Pages<RawPage>;
pub fn read<R: io::Read>(r: &mut R) -> io::Result<RawPages> {
    read_root_page(r).map(|root_page| {
        let page_size = root_page.database_header.page_size as usize;
        Pages {
            root_page,
            tail: core::iter::from_fn(|| read_raw_page(r, page_size).ok()).collect(),
        }
    })
}
pub fn convert<T: FromRawPage>(Pages { root_page, tail }: RawPages) -> io::Result<Pages<T>> {
    convert_root_page(root_page).map(|root_page| Pages {
        root_page,
        tail: tail
            .into_iter()
            .filter_map(|elt| T::from_raw_page(elt).ok())
            .collect(),
    })
}
#[derive(Debug)]
pub struct DatabaseCell<T> {
    pub page_index: usize,
    pub content: T,
}
fn root_cells<'p>(
    RootPage {
        database_header,
        tail,
    }: &'p RootPage<btree::BTreePage>,
) -> impl Iterator<Item = btree::BTreeCell> + 'p {
    btree::read_cells(tail, core::mem::size_of_val(database_header))
}
fn parse_root_cells(
    xs: impl Iterator<Item = btree::BTreeCell>,
) -> impl Iterator<Item = btree::RecordCell<record::SchemaColumn>> {
    xs.map_while(|cell| btree::parse_cell(cell).ok())
}
pub struct Database {
    pub schema: Vec<SchemaCell>,
    pub cells: Vec<DatabaseCell<btree::BTreeCell>>,
}
pub type SchemaCell = DatabaseCell<btree::SchemaRecordCell>;
pub struct PageCells {
    pub schema_cells: Vec<btree::RecordCell<record::SchemaColumn>>,
    pub btree_cells: Vec<Vec<btree::BTreeCell>>,
}
pub fn cells(Pages { root_page, tail }: &Pages<btree::BTreePage>) -> PageCells {
    PageCells {
        schema_cells: parse_root_cells(root_cells(root_page)).collect(),
        btree_cells: tail
            .iter()
            .map(|page| {
                btree::read_cells(page, 0)
                    .inspect(btree::print_cell_rowid)
                    .collect()
            })
            .collect(),
    }
}
impl IntoIterator for PageCells {
    type IntoIter = core::iter::Zip<
        std::vec::IntoIter<btree::RecordCell<record::SchemaColumn>>,
        std::vec::IntoIter<Vec<btree::BTreeCell>>,
    >;
    type Item = (
        btree::RecordCell<record::SchemaColumn>,
        Vec<btree::BTreeCell>,
    );
    fn into_iter(self) -> Self::IntoIter {
        self.schema_cells.into_iter().zip(self.btree_cells)
    }
}
