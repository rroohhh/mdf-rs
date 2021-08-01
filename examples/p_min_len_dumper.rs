use log::LevelFilter;
use mdf::{PagePointer, PageProvider, PageType, DB};
use mtf::mdf::MTFPageProvider;
use mtf::MTFParser;
use std::collections::HashMap;

fn main() {
    env_logger::init();

    let old_level = log::max_level();
    log::set_max_level(LevelFilter::Off);
    let file = &std::env::args().collect::<Vec<_>>()[1];
    let mut f = MTFParser::new(&file);
    let mut db_stream = None;
    for dblk in f.dblks() {
        for stream in dblk.streams {
            if stream.stream.header.id == "MQDA" {
                db_stream = Some(stream);
            }
        }
    }

    let stream = db_stream.unwrap();
    let page_provider = MTFPageProvider::from_stream(stream);

    let db = DB::new(page_provider);

    log::set_max_level(old_level);

    let mut p_min_info = HashMap::new();

    for tbl in db.tables() {
        if !tbl.partition_pointer.is_empty() {
            let first_page = tbl.partition_pointer[0];
            if let Some(first_page) = tbl.page_provider.get(first_page) {
                println!("########## {}\n{:?}", tbl.name, first_page.header);
                p_min_info.insert(first_page.header.p_min_len, tbl.name);
            } else {
                println!("######### {}\nNOTHING!!!", tbl.name);
            }
        }
    }

    for j in db.page_provider.file_ids() {
        for i in 0..db.page_provider.num_pages(j) {
            if let Some(page) = db.page_provider.get(PagePointer {
                page_id: i,
                file_id: j,
            }) {
                if matches!(page.header.ty, PageType::Data | PageType::Index) {
                    println!(
                        "######### {:?}\n{:?}",
                        p_min_info.get(&page.header.p_min_len),
                        page.header
                    );
                }
            }
        }
    }
}
