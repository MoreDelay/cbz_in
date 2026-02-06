# Description
Use this tool to convert image files within Zip archives from one format to another. The goal of this tools is to convert a whole comic collection in archives to new image formats such as AVIF and JXL, which have a smaller memory footprint while keeping greater detail. Internally, this program spawns new processes using ImageMagick (`magick`), cavif-rs (`cavif`) and libavif (`avifdec`), JPEG XL (`cjxl` and `djxl`), or WebM (`cwebp` and `dwebp`). If any of these programs are not installed, conversions relying on these tools can not be performed.

You can convert an archive or all zip files in a directory. By default, only JPEG and PNG files found within archives are converted, or you can force to convert all found images with `--force`. Original archives are not deleted.

# Usage
You can build the binary with `cargo build --release` and move the binary at `target/release/cbz_in` wherever you like.

Typical usage is of the form `cbz_in <format> <path/to/files>`. See `cbz_in --help` for more information.

