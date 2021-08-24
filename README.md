# warc-ip-corrector

A tool designed to run one time to correct warc.gz files produced at The National Library of Norway that contains a specific error where a header called "WARC-IP-Address" contains port numbers which makes the IP not spec compliant.  

# How to build
Clone the repository `git clone --recurse-submodules git@github.com:Avokadoen/warc-ip-corrector.git`

This project requires the latest zig master branch, make sure that `zig version` reports `0.9.0....`

Simply run `zig build` for a debug build, executable will be located in `zig-out/bin`

# How to use 
run `warc-ip-correcter -h` for usage 
