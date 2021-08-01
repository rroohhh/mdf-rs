use failure::Error;
use log::LevelFilter;
use mdf::{
    LobData, LobDataBlocks, LobEntry, LobSmallRoot, PagePointer, PageProvider, PageType, DB,
};
use mtf::mdf::MTFPageProvider;
use mtf::MTFParser;
use std::collections::{HashMap, HashSet};
use std::path::Path;

fn main() -> Result<(), Error> {
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

    let idx_file = "large_root_yukon.idx";

    let roots = if Path::new(idx_file).exists() {
        let file = std::fs::File::open(idx_file)?;
        bincode::deserialize_from(file)?
    } else {
        let mut roots = HashMap::new();

        for j in db.page_provider.file_ids() {
            for i in 0..db.page_provider.num_pages(j) {
                if let Some(page) = db.page_provider.get(PagePointer {
                    page_id: i,
                    file_id: j,
                }) {
                    if matches!(page.header.ty, PageType::TextTree | PageType::TextMix) {
                        for (k, record) in page.local_records().enumerate() {
                            let entry = LobEntry::parse(record);
                            match entry {
                                Some(LobEntry::LargeRootYukon(root)) => {
                                    println!("{:?}", root);
                                    let mut ptrs = Vec::new();
                                    let mut idx = 0;

                                    while let Some(ptr) = root.read_idx(idx) {
                                        ptrs.push(ptr);
                                        idx += 1;
                                    }

                                    roots.insert(
                                        (j, i, k),
                                        (
                                            root.blob_id,
                                            root.max_links,
                                            root.level,
                                            root.cur_links,
                                            ptrs,
                                        ),
                                    );
                                }
                                Some(LobEntry::Internal(root)) => {
                                    println!("{:?}", root);
                                    let mut ptrs = Vec::new();
                                    let mut idx = 0;

                                    while let Some(ptr) = root.read_idx(idx) {
                                        ptrs.push(ptr);
                                        idx += 1;
                                    }

                                    roots.insert(
                                        (j, i, k),
                                        (
                                            root.blob_id,
                                            root.max_links,
                                            root.level,
                                            root.cur_links,
                                            ptrs,
                                        ),
                                    );
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        let file = std::fs::File::create(idx_file)?;
        bincode::serialize_into(file, &roots)?;

        roots
    };

    let mut real_roots = HashSet::new();

    // first fill all the roots into the real roots
    for (file_id, page_id, record_id) in roots.keys() {
        real_roots.insert((*file_id, *page_id, *record_id));
    }

    let mut old_len = 0;
    while old_len != real_roots.len() {
        old_len = real_roots.len();
        for entry in real_roots.clone() {
            for ptr in &roots[&entry].4 {
                if real_roots.contains(&(
                    ptr.page_ptr.file_id,
                    ptr.page_ptr.page_id,
                    ptr.slot_id as usize,
                )) {
                    real_roots.remove(&(
                        ptr.page_ptr.file_id,
                        ptr.page_ptr.page_id,
                        ptr.slot_id as usize,
                    ));
                }
            }
        }
    }

    let base = "../mnt/lob_dump";
    for (file_num, real_root) in real_roots.iter().enumerate() {
        let r = &roots[real_root];

        let mut entries = vec![];
        for sub_entry in &r.4 {
            if let Some(e) = db
                .page_provider
                .get_record(*sub_entry)
                .and_then(LobEntry::parse)
            {
                entries.push(e);
            }
        }

        let mut data_blocks = vec![];

        'outer: while !entries.is_empty() {
            let mut new_entries = vec![];
            for entry in entries {
                match &entry {
                    LobEntry::SmallRoot(LobSmallRoot { data, .. })
                    | LobEntry::Data(LobData { data, .. }) => {
                        // this can basically only happen at the first entry
                        data_blocks.push((data.len() as u64, *data));
                    }
                    _ => {
                        for (offs, entry) in entry.sub_entries(&db.page_provider) {
                            match entry {
                                Some(entry) => match &entry {
                                    LobEntry::SmallRoot(LobSmallRoot { data, .. })
                                    | LobEntry::Data(LobData { data, .. }) => {
                                        data_blocks.push((offs, *data));
                                    }
                                    _ => new_entries.push(entry),
                                },
                                None => break 'outer,
                            }
                        }
                    }
                }
            }
            entries = new_entries;
        }

        let data_block = LobDataBlocks { data_blocks };

        let file_name = format!("{}/{}", base, file_num);
        println!("dumping {}", file_name);
        std::fs::create_dir_all(Path::new(&file_name).parent().unwrap())?;
        data_block.write_to_file(&file_name)?;
    }

    Ok(())
}
