const std = @import("std");
const testing = std.testing;

const c = @import("c.zig");

// TODO: define errors, print msg in case of error
// Source: https://stackoverflow.com/a/57699371/11768869
/// Utility wrapper for deflate with gzip specific configuartion
pub fn compressGzip(source: []u8, buffer: []u8) !u64 {
    var z_stream = c.z_stream{
        .next_in = source.ptr,
        .avail_in = @intCast(u32, source.len),
        .total_in = 0,

        .next_out = buffer.ptr,
        .avail_out = @intCast(u32, buffer.len),
        .total_out = 0,

        .msg = null,
        .state = null,

        .zalloc = null,
        .zfree = null,
        .@"opaque" = null,
        
        .data_type = 0,
        .adler = 0,
        .reserved = 0,
    };

    {
        const z_result = c.deflateInit2(&z_stream, c.Z_DEFAULT_COMPRESSION, c.Z_DEFLATED, 15 | 16, 8, c.Z_DEFAULT_STRATEGY);
        switch (z_result) {
            c.Z_OK => {},
            c.Z_MEM_ERROR => return error.OutOfMemory,
            c.Z_STREAM_ERROR => return error.InvalidParameters,
            c.Z_VERSION_ERROR => return error.IncompatibleVersion,
            else => unreachable,
        }
    }

    {
        const z_result = c.deflate(&z_stream, c.Z_FINISH);
        switch (z_result) {
            c.Z_STREAM_END => {},
            c.Z_OK, c.Z_BUF_ERROR => return error.InsufficentBuffer, // TODO: not a error
            else => return error.UnknownError,
        }
    }

    {
        const z_result = c.deflateEnd(&z_stream);
        switch (z_result) {
            c.Z_OK => {},
            c.Z_STREAM_ERROR => return error.InconsistentStream,
            c.Z_DATA_ERROR => return error.EarlyMemoryFree,
            else => return error.UnknownError,
        }
    }

    return z_stream.total_out;
}

test "basic add functionality" {
    try testing.expect(add(3, 7) == 10);
}
