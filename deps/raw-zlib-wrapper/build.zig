const std = @import("std");

/// Link a step with zlib statically
pub fn linkStep(b: *std.build.Builder, step: *std.build.LibExeObjStep) void {
    step.linkLibC();

    const this_dir = std.fs.path.dirname(@src().file) orelse ".";
    var include_dir = std.fs.path.join(b.allocator, &.{ this_dir, "deps/zlib-1.2.11" }) catch unreachable;
    step.addIncludeDir(include_dir);

    var sources = std.ArrayList([]const u8).init(b.allocator);
    for([_][]const u8{
        "deps/zlib-1.2.11/inftrees.c",
        "deps/zlib-1.2.11/inflate.c",
        "deps/zlib-1.2.11/adler32.c",
        "deps/zlib-1.2.11/zutil.c",
        "deps/zlib-1.2.11/trees.c",
        "deps/zlib-1.2.11/gzclose.c",
        "deps/zlib-1.2.11/gzwrite.c",
        "deps/zlib-1.2.11/gzread.c",
        "deps/zlib-1.2.11/deflate.c",
        "deps/zlib-1.2.11/compress.c",
        "deps/zlib-1.2.11/crc32.c",
        "deps/zlib-1.2.11/infback.c",
        "deps/zlib-1.2.11/gzlib.c",
        "deps/zlib-1.2.11/uncompr.c",
        "deps/zlib-1.2.11/inffast.c",
    }) |path| {
        var abs_path = std.fs.path.join(b.allocator, &.{ this_dir, path }) catch unreachable;
        sources.append(abs_path) catch unreachable;
    }
    step.addCSourceFiles(sources.items, &.{ });
}
