use failure::Error;
use log::LevelFilter;
use mdf::{Row, SqlValue, DB};
use mtf::{mdf::MTFPageProvider, MTFParser};
use std::collections::HashMap;
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

    let all_docs_index = if !Path::new("all_docs_index.bincode").exists() {
        // Read index
        let idx_file = std::fs::File::open("all_docs_index.bincode").unwrap();
        bincode::deserialize_from(idx_file).unwrap()
    } else {
        // Generate index
        let mut all_docs_index = HashMap::new();

        for tbl in db.system_tables.tables() {
            if tbl.name == "AllDocs" {
                println!("####################### {}", tbl.name);
                let tbl = db.table(&tbl.name).unwrap();
                println!("{:#?}", tbl.schema);

                for row in tbl.scan_db() {
                    let Row { mut values, .. } = row;
                    let _id = values[0].take().unwrap().unwrap_unique_identifier();
                    let site_id = values[1].take().unwrap().unwrap_unique_identifier();
                    let web_id = values[4].take().unwrap().unwrap_unique_identifier();

                    // something is extremely broken, for some reason there is a (fixed?) 0x01 byte
                    // between the id and the site_id, so we need to grab our most significant byte
                    // (because little endian) from the least significant byte of the next value
                    let actual_id = (site_id >> 8) | (web_id << (8 * 15));

                    // Something is broken, the first var length column is zero long
                    let dir_name = values[3].take().unwrap().unwrap_nvar_char_in_row();
                    let leaf_name = match values[18].take() {
                        Some(v) => v.unwrap_nvar_char_in_row(),
                        None => "empty_leaf_name".to_owned(),
                    };

                    println!("{}, {}, {}", actual_id, dir_name, leaf_name);
                    if all_docs_index.contains_key(&actual_id) {
                        let (other_dir_name, other_leaf_name) = &all_docs_index[&actual_id];
                        if &dir_name != other_dir_name || &leaf_name != other_leaf_name {
                            panic!(
                                "dupe key {}, {:?} vs ({}, {})",
                                actual_id, all_docs_index[&actual_id], dir_name, leaf_name
                            )
                        }
                    } else {
                        all_docs_index.insert(actual_id, (dir_name, leaf_name));
                    }
                }

                let outfile = std::fs::File::create("all_docs_index.bincode").unwrap();
                bincode::serialize_into(outfile, &all_docs_index)?;
            }
        }

        all_docs_index
    };

    let base = "./mnt/sharepoint_dump";

    for tbl in db.system_tables.tables() {
        if tbl.name == "AllDocStreams" {
            println!("####################### {}", tbl.name);
            let tbl = db.table(&tbl.name).unwrap();

            println!("{:#?}", tbl.schema);

            for row in tbl.scan_db() {
                let Row { mut values, .. } = row;
                let _id = values[0].take().unwrap().unwrap_unique_identifier();
                let _site_id = values[1].take().unwrap().unwrap_unique_identifier();
                let parent_id = values[3].take().unwrap().unwrap_unique_identifier();
                let _size = values[4].take().unwrap().unwrap_int();
                let ptr = values[6].take();
                let doc_info = all_docs_index.get(&parent_id);

                if let Some(SqlValue::Image(Some(ptr))) = ptr {
                    let d = ptr.read(&db.page_provider);
                    let file_name = if let Some((dir_name, leaf_name)) = doc_info {
                        format!("{}/{}", dir_name, leaf_name)
                    } else {
                        format!("unknown_name/{:032x}.dat", parent_id)
                    };

                    if let Some(d) = d {
                        let mut file_name = format!("{}/{}", base, file_name);
                        println!("dumping {}", file_name);
                        std::fs::create_dir_all(Path::new(&file_name).parent().unwrap())?;
                        while Path::new(&file_name).exists() {
                            println!("dupe file_name: {:?}", file_name);
                            file_name += "_";
                        }
                        d.write_to_file(&file_name)?;
                    }
                }
            }
        }
    }

    Ok(())
}
