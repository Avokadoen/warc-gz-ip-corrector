const std = @import("std");
const clap = @import("clap");
const zlib = @import("zlib");

const Allocator = std.mem.Allocator;

pub fn main() anyerror!void {
    const std_out = std.io.getStdOut().writer();
    const std_err = std.io.getStdErr().writer();

    // First we specify what parameters our program can take.
    // We can use `parseParam` to parse a string to a `Param(Help)`
    const params = comptime [_]clap.Param(clap.Help){
        clap.parseParam("-h, --help                     Display this help and exit.              ") catch unreachable,
        clap.parseParam("-p, --path    <STR>...         Paths to a files that should be corrected") catch unreachable,
        clap.parseParam("-w, --workers <INT>            How many threads that should run.        ") catch unreachable,
        clap.parseParam("-f, --fix-destination <STR>    Folder where fixed files should be stored") catch unreachable,
    };

    // Initalize our diagnostics, which can be used for reporting useful errors.
    // This is optional. You can also pass `.{}` to `clap.parse` if you don't
    // care about the extra information `Diagnostics` provides.
    var diag = clap.Diagnostic{};
    var args = clap.parse(clap.Help, &params, .{ .diagnostic = &diag }) catch |err| {
        // Report useful error and exit
        diag.report(std_err, err) catch {};
        return err;
    };
    defer args.deinit();

    // Print help message if user requested it
    if (args.flag("--help")) {
        try clap.help(
            std_out,
            params[0..],
        );
        return;
    }

    // Parse path variable
    const paths = args.options("--path");
    if (paths.len <= 0) {
        diag.arg = "path";
        diag.report(std_err, error.MissingValue) catch {};
        return;
    }

    // Parse workers variable
    var worker_count: usize = undefined;
    if (args.option("--workers")) |w| {
        const workers = try std.fmt.parseInt(usize, w, 10);
        worker_count = std.math.min(paths.len, workers);
        worker_count = std.math.max(1, worker_count);
    } else {
        worker_count = 1;
    }

    const destination = args.option("--fix-destination") orelse { 
        diag.arg = "fix-destination";
        diag.report(std_err, error.MissingValue) catch {};
        return;
    };

    // TODO: GPA is not really suited for terminal applications
    // create a gpa with default configuration
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const leak = gpa.deinit();
        if (leak) {
            std_err.print("leak detected in gpa!", .{}) catch unreachable;
        }
    }

    var worker_frames = try std.ArrayList(@Frame(fixWarcIP)).initCapacity(&gpa.allocator, worker_count);
    defer worker_frames.deinit();

    var destination_dir = blk: {
        if (std.fs.path.isAbsolute(destination)) {
            break :blk std.fs.openDirAbsolute(destination, .{}) catch |err| {
                std_err.print("failed to open directory at {s}, err: {any}", .{destination, err}) catch {};
                return;
            };
        }
        break :blk std.fs.cwd().openDir(destination, .{}) catch |err| {
            std_err.print("failed to open directory at {s}, err: {any}", .{destination, err}) catch {};
            return;
        };
    };
    defer destination_dir.close();

    var worker_id: usize = 0;
    while (worker_id < worker_count) : (worker_id += 1) {
        const worker = WorkContext{
            .allocator = &gpa.allocator,
            .std_out = std_out,
            .std_err = std_err,
            .paths = getPathSlice(paths[0..], worker_count, worker_id),
            .destination_dir = destination_dir,
        };

        var frame = async fixWarcIP(worker);
        worker_frames.appendAssumeCapacity(frame);
    }

    for (worker_frames.items) |*frame| {
        await frame;
    }
}

// TODO: worker design was Thread oriented, but threads are bugged in master branch
//       so we use async instead. We don't really need to slice paths at that point
//       as we can trivially start one async frame for each path ...
const WorkContext = struct {
    allocator: *Allocator,
    std_out: std.fs.File.Writer,
    std_err: std.fs.File.Writer,
    paths: []const[]const u8,
    destination_dir: std.fs.Dir,
};

/// Opens a warc.gz file and removes port number from any IP in the WARC-IP-Address header
fn fixWarcIP(ctx: WorkContext) void {
    const pre_alloc_size = 4096 * 2 * 2 * 2 * 2;

    var file_read_buffer = std.ArrayList(u8).initCapacity(ctx.allocator, pre_alloc_size) catch |err| {
        ctx.std_err.print("failed to allocate read buffer, err: {any}", .{err}) catch {};
        return;
    };
    defer file_read_buffer.deinit();
    var file_write_buffer = std.ArrayList(u8).initCapacity(ctx.allocator, pre_alloc_size) catch |err| {
        ctx.std_err.print("failed to allocate write buffer, err: {any}", .{err}) catch {};
        return;
    };
    defer file_write_buffer.deinit();
    
    // TODO: diag.report(std_err, ...)
    for (ctx.paths) |path| {
        var destination_file = blk: {
            const file_name = std.fs.path.basename(path);
            break :blk ctx.destination_dir.createFile(file_name, .{}) catch |err| {
                ctx.std_err.print("failed to create destination file, err: {any}", .{err}) catch {};
                return;
            };
        };
        defer destination_file.close();
        var desintation_file_pos: u64 = 0;

        const source_file: std.fs.File = blk: {
            if (std.fs.path.isAbsolute(path)) {
                break :blk std.fs.openFileAbsolute(path, .{}) catch |err| {
                    ctx.std_err.print("failed to open file at {s}, err: {any}", .{path, err}) catch {};
                    return;
                };
            }
            break :blk std.fs.cwd().openFile(path, .{}) catch |err| {
                ctx.std_err.print("failed to open file at {s}, err: {any}", .{path, err}) catch {};
                return;
            };
        };
        defer source_file.close();
        var file_end_pos = source_file.getEndPos() catch |err| {
            ctx.std_err.print("failed to get file end pos {s}, err: {any}", .{path, err}) catch {};
            return;
        };
        var file_pos = source_file.getPos() catch |err| {
            ctx.std_err.print("failed to get file pos {s}, err: {any}", .{path, err}) catch {};
            return;
        };
        
        while (file_pos < file_end_pos) {
            var gzip_stream = std.compress.gzip.gzipStream(ctx.allocator, source_file.reader()) catch |err| {
                ctx.std_err.print("failed to init gzip stream at {s}, err: {any}", .{path, err}) catch {};
                return;
            };
            defer gzip_stream.deinit();

            // read gzip_stream and move data into an array list
            var bytes_read: usize = 0;
            read_gzip: while (true) {
                file_read_buffer.items.len = file_read_buffer.capacity;
                const new_bytes_read = gzip_stream.read(file_read_buffer.items[bytes_read..]) catch |err| {
                    ctx.std_err.print("failed to read gzip stream at {d}, err: {any}", .{gzip_stream.read_amt, err}) catch {};
                    return;
                };
                bytes_read += new_bytes_read;
                file_read_buffer.items.len = bytes_read;
                if (new_bytes_read <= 0) {
                    break :read_gzip;
                }
                if (bytes_read >= file_read_buffer.capacity) {
                    // increase buffer size
                    file_read_buffer.ensureTotalCapacity(file_read_buffer.capacity * 2) catch |err| {
                        ctx.std_err.print("failed to increase read buffer size to {d}, err: {any}", .{file_read_buffer.capacity * 2, err}) catch {};
                        return;
                    };
                    file_write_buffer.ensureTotalCapacity(file_write_buffer.capacity * 2) catch |err| {
                        ctx.std_err.print("failed to increase write buffer size to {d}, err: {any}", .{file_write_buffer.capacity * 2, err}) catch {};
                        return;
                    };
                }
            }

            // Search record for WARC-IP-Address and replace port with whitespace if it exist
            const needle = "WARC-IP-Address: ";
            var needle_search_start: usize = 0;
            header_search: while (std.mem.indexOf(u8, file_read_buffer.items[needle_search_start..bytes_read], needle)) |index| {
                if (index == 0) {
                    break :header_search;
                }
                const SearchState = enum {
                    IP,
                    Port
                };
                var state = SearchState.IP;

                // skip "WARC-IP-Address: "
                const port_start_index = index + needle.len;
                var i: usize = 0;
                port_search: while(file_read_buffer.items[port_start_index+i] != '\n' and file_read_buffer.items[port_start_index+i] != '\r') : (i += 1) {
                    if (file_read_buffer.items[port_start_index+i] == ':') {
                        state = SearchState.Port;
                        break :port_search;
                    }
                }
                // replace port in buffer to whitespace if it exist
                if (state == SearchState.Port) {
                    while(file_read_buffer.items[port_start_index+i] != '\n' and file_read_buffer.items[port_start_index+i] != '\r') : (i += 1) {
                        file_read_buffer.items[port_start_index+i] = ' ';
                    } 
                } 
                needle_search_start = port_start_index + i + 1;
            }

            file_write_buffer.items.len = file_write_buffer.capacity;
            const bytes_compressed = zlib.compressGzip(file_read_buffer.items, file_write_buffer.items) catch |err| {
                ctx.std_err.print("failed to compress, err: {any}", .{err}) catch {};
                return;
            };
            file_write_buffer.items.len = bytes_compressed;

            // we allocate as much in the compressed buffer as uncompressed buffer,
            // if this assert fails it means that the compressed data is larger than the original data
            std.debug.assert(bytes_compressed < file_write_buffer.capacity);

            // Write to the new file with the corrected buffer
            const bytes_written = destination_file.pwrite(file_write_buffer.items, desintation_file_pos) catch |err| {
                ctx.std_err.print("failed to write to destination file at {d}, err: {any}", .{desintation_file_pos, err}) catch {};
                return;
            };
            desintation_file_pos += bytes_written;
            
            file_pos = source_file.getPos() catch |err| {
                ctx.std_err.print("failed to get file pos {s}, err: {any}", .{path, err}) catch {};
                return;
            };
        }
    }
}

fn getPathSlice(all_paths: []const[]const u8, worker_count: usize, subset: usize) []const[]const u8 {
    std.debug.assert(worker_count <= all_paths.len);

    const slice_len = all_paths.len / worker_count;
    const start = subset * slice_len;
    const end = std.math.min(all_paths.len, (start + 1) * slice_len);
    return all_paths[start..end];
}

// TODO: improve readability of test
test "getPathSlice produce correct path slices" {
    const test_paths1 = [_][]const u8{
        "hello",
    };
    try std.testing.expectEqual(
        @intCast(usize, 1), 
        getPathSlice(test_paths1[0..], 1, 0).len
    );

    const test_paths2 = test_paths1 ++ [_][]const u8{
        "world,",
    };
    try std.testing.expectEqual(
        @intCast(usize, 2), 
        getPathSlice(test_paths2[0..], 1, 0).len
    );
    try std.testing.expectEqual(
        @intCast(usize, 1), 
        getPathSlice(test_paths2[0..], 2, 0).len
    );

    const test_paths3 = test_paths2 ++ [_][]const u8{
        "this",
    };
    try std.testing.expectEqual(
        @intCast(usize, 3), 
        getPathSlice(test_paths3[0..], 1, 0).len
    );
    try std.testing.expectEqual(
        @intCast(usize, 1), 
        getPathSlice(test_paths3[0..], 2, 1).len
    );

    const test_paths4 = test_paths3 ++ [_][]const u8{
        "is",
        "a",
        "test",
        ":)"
    };
    try std.testing.expectEqual(
        @intCast(usize, 3), 
        getPathSlice(test_paths4[0..], 2, 0).len
    );
    try std.testing.expectEqual(
        @intCast(usize, 4), 
        getPathSlice(test_paths4[0..], 2, 1).len
    );
}
