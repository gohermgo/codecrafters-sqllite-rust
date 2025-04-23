use core::iter::{from_fn as iter_fn, Zip};

use std::vec::IntoIter;

use crate::database;
use crate::database::btree::{self, BTreeCell, BTreePage};
use crate::database::record::{SchemaColumn, SchemaRecord};
use crate::database::DatabaseHeader;
use crate::io;

#[derive(Debug)]
pub struct RootPage<T> {
    pub database_header: DatabaseHeader,
    pub tail: T,
}
fn read_root_page<R: io::Read>(r: &mut R) -> io::Result<RootPage<BTreePage>> {
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
#[derive(Debug)]
pub struct Pages<T> {
    pub root_page: RootPage<T>,
    pub tail: Vec<T>,
}
pub fn read<R: io::Read>(r: &mut R) -> io::Result<Pages<BTreePage>> {
    read_root_page(r).map(|root_page| {
        let page_size = root_page.database_header.page_size as usize;
        Pages {
            root_page,
            tail: iter_fn(|| {
                io::read_exact_vec(r, page_size)
                    .and_then(|tail| btree::read_page(&mut tail.as_slice(), 0))
                    .ok()
            })
            .collect(),
        }
    })
}
#[derive(Debug)]
pub struct PageCells {
    pub database_header: DatabaseHeader,
    pub schema_cells: Vec<SchemaRecord>,
    pub btree_cells: Vec<Vec<btree::BTreeCell>>,
}
pub fn cells(Pages { root_page, tail }: Pages<BTreePage>) -> PageCells {
    use btree::{parse_cell, RecordCell};
    // use record::{SchemaColumn, SchemaRecord};
    let RootPage {
        database_header,
        tail: root_tail,
    } = root_page;
    let schema_cells = root_tail
        .content
        .into_iter()
        .map_while(|cell| parse_cell::<SchemaColumn>(cell).ok())
        .map_while(|RecordCell { rowid, mut record }| {
            eprintln!("ROWID={rowid:?}");
            record.columns.pop().map(|column| SchemaRecord {
                header: record.header,
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
    type IntoIter = Zip<IntoIter<SchemaRecord>, IntoIter<Vec<BTreeCell>>>;
    type Item = (SchemaRecord, Vec<BTreeCell>);
    fn into_iter(self) -> Self::IntoIter {
        self.schema_cells.into_iter().zip(self.btree_cells)
    }
}
