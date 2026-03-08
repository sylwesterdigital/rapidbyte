use rapidbyte_sdk::build::ManifestBuilder;
use rapidbyte_sdk::wire::{Feature, WriteMode};

fn main() {
    ManifestBuilder::destination("rapidbyte/dest-postgres")
        .name("PostgreSQL Destination")
        .description("Writes data to PostgreSQL using INSERT or COPY")
        .write_modes(&[
            WriteMode::Append,
            WriteMode::Replace,
            WriteMode::Upsert {
                primary_key: vec![],
            },
        ])
        .dest_features(vec![Feature::BulkLoad])
        .allow_runtime_network()
        .env_vars(&["PGSSLROOTCERT"])
        .emit();
}
