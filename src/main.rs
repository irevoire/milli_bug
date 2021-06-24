use std::fs::{create_dir_all, remove_dir_all};
use std::io::Cursor;

use heed::EnvOpenOptions;
use milli::update::{IndexDocumentsMethod, UpdateBuilder, UpdateFormat};
use milli::Index;

fn main() {
    let db_name = "bug.mmdb";
    match remove_dir_all(db_name) {
        Ok(_) => (),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
        Err(e) => panic!("{}", e),
    }
    create_dir_all(db_name).unwrap();

    let mut options = EnvOpenOptions::new();
    options.map_size(100 * 1024 * 1024 * 1024); // 100 GB
    options.max_readers(10);
    let index = Index::new(options, "bug.mmdb").unwrap();

    let update_builder = UpdateBuilder::new(0);
    let mut wtxn = index.write_txn().unwrap();
    let mut builder = update_builder.settings(&mut wtxn, &index);

    /* here I can set the settings */
    builder.reset_filterable_fields();

    builder.execute(|_, _| ()).unwrap();
    wtxn.commit().unwrap();

    let update_builder = UpdateBuilder::new(0);
    let mut wtxn = index.write_txn().unwrap();
    let mut builder = update_builder.index_documents(&mut wtxn, &index);
    builder.update_format(UpdateFormat::Json);
    builder.index_documents_method(IndexDocumentsMethod::ReplaceDocuments);

    let documents = r#"
[
  { "id": 2,    "title": "Pride and Prejudice",                    "author": "Jane Austin",              "genre": "romance",    "price": 3.5 },
  { "id": 456,  "title": "Le Petit Prince",                        "author": "Antoine de Saint-Exupéry", "genre": "adventure" , "price": 10.0 },
  { "id": 1,    "title": "Alice In Wonderland",                    "author": "Lewis Carroll",            "genre": "fantasy",    "price": 25.99 },
  { "id": 4,    "title": "Harry Potter and the Half-Blood Prince", "author": "J. K. Rowling",            "genre": "fantasy" }
]
        "#;

    let reader = Cursor::new(documents);
    builder.execute(reader, |_, _| ()).unwrap();
    wtxn.commit().unwrap();

    println!("first batch of document inserted");

    let update_builder = UpdateBuilder::new(0);
    let mut wtxn = index.write_txn().unwrap();
    let mut builder = update_builder.index_documents(&mut wtxn, &index);
    builder.update_format(UpdateFormat::Json);
    builder.index_documents_method(IndexDocumentsMethod::UpdateDocuments);
    let documents = r#"
[
  {
	"id": 2,
	"author": "au",
	"date": "1813"
  }
]
        "#;

    let reader = Cursor::new(documents);
    builder.execute(reader, |_, _| ()).unwrap();
    wtxn.commit().unwrap();

    println!("second batch of document inserted");
}
