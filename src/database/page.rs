use crate::database;
use crate::database::{btree, DatabaseHeader};
use crate::io;

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
pub struct PageContent<T> {
    pub page_index: usize,
    pub content: T,
}
fn root_cells<'p>(
    RootPage {
        database_header,
        tail,
    }: &'p RootPage<btree::BTreePage>,
) -> impl Iterator<Item = PageContent<btree::BTreeCell>> + 'p {
    // eprintln!("READING ROOT CELLS");
    // eprintln!("ROOTPAGE_DATA={:?}", tail.content);
    // let nzi = tail
    //     .content
    //     .iter()
    //     .enumerate()
    //     .find_map(|(idx, elt)| elt.ne(&0).then_some(idx));
    // eprintln!("ROOTPAGE_STR={:?}", String::from_utf8_lossy(&tail.content));
    // eprintln!("FIRST NONZERO={nzi:?}");
    btree::read_cells(tail, core::mem::size_of_val(database_header)).map(|cell| PageContent {
        page_index: 0,
        content: cell,
    })
}
pub fn cells<'p>(
    Pages { root_page, tail }: &'p Pages<btree::BTreePage>,
) -> impl Iterator<Item = PageContent<btree::BTreeCell>> + 'p {
    root_cells(root_page).chain(tail.iter().enumerate().flat_map(|(idx, page)| {
        btree::read_cells(page, 0).map(move |cell| {
            PageContent {
                // Since it is not the root-page, we add one
                page_index: idx + 1,
                content: cell,
            }
        })
    }))
}
