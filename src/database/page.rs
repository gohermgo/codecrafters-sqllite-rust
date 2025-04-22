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
fn read_root_page<R: io::Read>(r: &mut R) -> io::Result<RootPage<btree::BTreePage>> {
    let database_header = database::read_header(r)?;
    let tail_size = database_header.page_size as usize - core::mem::size_of_val(&database_header);
    eprintln!("READING ROOTPAGE");
    io::read_exact_vec(r, tail_size)
        .and_then(|tail| {
            btree::read_page(
                &mut tail.as_slice(),
                core::mem::size_of_val(&database_header),
            )
        })
        .map(|tail| RootPage {
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
pub fn read<R: io::Read>(r: &mut R) -> io::Result<Pages<btree::BTreePage>> {
    read_root_page(r).map(|root_page| {
        let page_size = root_page.database_header.page_size as usize;
        Pages {
            root_page,
            tail: core::iter::from_fn(|| {
                io::read_exact_vec(r, page_size)
                    .and_then(|tail| btree::read_page(&mut tail.as_slice(), 0))
                    .ok()
            })
            .collect(),
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
// fn parse_root_page(
//     RootPage {
//         database_header,
//         tail,
//     }: RootPage<btree::BTreePageBytes>,
// ) -> RootPage<btree::BTreePage> {
//     let tail = btree::parse_page_bytes(tail, core::mem::size_of_val(&database_header));
//     RootPage {
//         database_header,
//         tail,
//     }
// }
// fn _page_cells(
//     xs: impl IntoIterator<Item = btree::BTreePageBytes>,
// ) -> impl Iterator<Item = btree::BTreePage> {
//     let parse = |tail| btree::parse_page_bytes(tail, 0);
//     xs.into_iter().map(parse)
// }
fn root_cells<'p>(
    RootPage {
        database_header,
        tail,
    }: &'p RootPage<btree::BTreePageBytes>,
) -> impl Iterator<Item = btree::BTreeCell> + 'p {
    btree::read_cells(tail, core::mem::size_of_val(database_header))
}
fn parse_record_cells(
    xs: impl Iterator<Item = btree::BTreeCell>,
) -> impl Iterator<Item = record::SchemaRecord> {
    xs.map_while(|cell| btree::parse_cell::<record::SchemaColumn>(cell).ok())
        .map_while(|mut record| {
            record
                .record
                .columns
                .pop()
                .map(|column| record::SchemaRecord {
                    header: record.record.header,
                    column,
                })
        })
}
#[derive(Debug)]
pub struct PageCells {
    pub database_header: DatabaseHeader,
    pub schema_cells: Vec<record::SchemaRecord>,
    pub btree_cells: Vec<Vec<btree::BTreeCell>>,
}
pub fn cells(Pages { root_page, tail }: Pages<btree::BTreePage>) -> PageCells {
    let RootPage {
        database_header,
        tail: root_tail,
    } = root_page;
    let schema_cells = root_tail
        .content
        .into_iter()
        .map_while(|cell| btree::parse_cell::<record::SchemaColumn>(cell).ok())
        .map_while(|mut record| {
            record
                .record
                .columns
                .pop()
                .map(|column| record::SchemaRecord {
                    header: record.record.header,
                    column,
                })
        })
        .collect();
    let btree_cells = tail.into_iter().map(|page| page.content).collect();
    PageCells {
        database_header,
        schema_cells,
        btree_cells,
    }
}
impl IntoIterator for PageCells {
    type IntoIter = core::iter::Zip<
        std::vec::IntoIter<record::SchemaRecord>,
        std::vec::IntoIter<Vec<btree::BTreeCell>>,
    >;
    type Item = (record::SchemaRecord, Vec<btree::BTreeCell>);
    fn into_iter(self) -> Self::IntoIter {
        self.schema_cells.into_iter().zip(self.btree_cells)
    }
}
