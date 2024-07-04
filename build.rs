use std::io::Result;

use vergen::{vergen, Config};

fn main() -> Result<()> {
    #[allow(clippy::expect_used)]
    vergen(Config::default()).expect("Could not generate vergen");

    prost_build::compile_protos(&["flow.proto"], &["."])?;

    Ok(())
}
