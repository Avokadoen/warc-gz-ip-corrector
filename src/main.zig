const std = @import("std");
const clap = @import("clap");
const zlib = @import("zlib");

const Allocator = std.mem.Allocator;
// It would be preferable to use .evented here, but std does not support evented file handling yet
pub const io_mode = .blocking;

pub fn main() anyerror!void {
    const std_out = std.io.getStdOut().writer();
    const std_err = std.io.getStdErr().writer();

    // First we specify what parameters our program can take.
    // We can use `parseParam` to parse a string to a `Param(Help)`
    const params = comptime [_]clap.Param(clap.Help){
        clap.parseParam("-h, --help                     Display this help and exit.                     ") catch unreachable,
        clap.parseParam("-f, --file        <STR>...     Paths to a file that should be corrected        ") catch unreachable,
        clap.parseParam("-d, --directory   <STR>...     Paths to a directory that should be corrected   ") catch unreachable,
        clap.parseParam("-o, --output      <STR>        Folder where fixed files should be stored       ") catch unreachable,
    };

    // Initalize our diagnostics, which can be used for reporting useful errors.
    // This is optional. You can also pass `.{}` to `clap.parse` if you don't
    // care about the extra information `Diagnostics` provides.
    var diag = clap.Diagnostic{};
    var args = clap.parse(clap.Help, &params, .{ .diagnostic = &diag }) catch |err| {
        // Report useful error and exit
        diag.report(std_err, err) catch {};
        return;
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

    // Parse path variables
    const file_paths = args.options("--file");
    const directory_paths = args.options("--directory");
    if (file_paths.len <= 0 and directory_paths.len <= 0) {
        diag.arg = "file or directory";
        diag.report(std_err, error.MissingValue) catch {};
        return;
    }

    const destination = args.option("--output") orelse { 
        diag.arg = "output";
        diag.report(std_err, error.MissingValue) catch {};
        return;
    };

    // Use allocator capable of detecting memory leaks in debug
    const is_debug = std.builtin.mode == .Debug;
    var alloc_api = if(is_debug) std.heap.GeneralPurposeAllocator(.{}){} else std.heap.c_allocator;
    defer {
        if(is_debug) {
            const leak = alloc_api.deinit();
            if (leak) {
                std_err.print("leak detected in gpa!", .{}) catch unreachable;
            }
        }
    }
    var allocator = if(is_debug) &alloc_api.allocator else alloc_api;

    var source_files = std.ArrayList([]const u8).initCapacity(allocator, file_paths.len) catch |err| {
        std_err.print("failed to initialize source path list, err: {any}", .{err}) catch {};
        return;
    };
    defer source_files.deinit();

    try source_files.insertSlice(0, file_paths);
    
    for (directory_paths) |directory_path| {
        var directory = blk: {
            if (std.fs.path.isAbsolute(directory_path)) {
                break :blk std.fs.openDirAbsolute(directory_path, .{ .iterate = true, }) catch |err| {
                    std_err.print("failed to open directory at {s}, err: {any}", .{directory_path, err}) catch {};
                    return;
                };
            }
            break :blk std.fs.cwd().openDir(directory_path, .{ .iterate = true, }) catch |err| {
                std_err.print("failed to open directory at {s}, err: {any}", .{directory_path, err}) catch {};
                return;
            };
        };
        defer directory.close();

        var iter = directory.iterate();
        while ((try iter.next())) |some| {
            if (some.kind != std.fs.File.Kind.File) {
                continue;
            }
            // Append all compressed warc files
            if (std.mem.indexOf(u8, some.name, ".warc.gz")) |_| {
                // TODO: MEMORY LEAK: this leaks memory, but does not really matter since lifetime should be static anyways
                const file_path = try std.fs.path.join(allocator, &[_][]const u8{directory_path, some.name});
                try source_files.append(file_path);
            }
        }
    }

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

    var fix_frames = try std.ArrayList(@Frame(fixWarcIP)).initCapacity(allocator, source_files.items.len);
    defer fix_frames.deinit();

    for (source_files.items) |path| {
        const ctx = AsyncContext{
            .allocator = allocator,
            .std_out = std_out,
            .std_err = std_err,
            .path = path,
            .destination_dir = destination_dir,
        };
        var frame = async fixWarcIP(ctx);
        fix_frames.appendAssumeCapacity(frame);
    }

    for(fix_frames.items) |*frame| {
        await frame;
    }
}

const AsyncContext = struct {
    allocator: *Allocator,
    std_out: std.fs.File.Writer,
    std_err: std.fs.File.Writer,
    path: []const u8,
    destination_dir: std.fs.Dir,
};

/// Opens a warc.gz file and removes port number from any IP in the WARC-IP-Address header
fn fixWarcIP(ctx: AsyncContext) void {
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
    
    var destination_file = blk: {
        const file_name = std.fs.path.basename(ctx.path);
        break :blk ctx.destination_dir.createFile(file_name, .{}) catch |err| {
            ctx.std_err.print("failed to create destination file, err: {any}", .{err}) catch {};
            return;
        };
    };
    defer destination_file.close();
    var desintation_file_pos: u64 = 0;

    const source_file: std.fs.File = blk: {
        if (std.fs.path.isAbsolute(ctx.path)) {
            break :blk std.fs.openFileAbsolute(ctx.path, .{}) catch |err| {
                ctx.std_err.print("failed to open file at {s}, err: {any}", .{ctx.path, err}) catch {};
                return;
            };
        }
        break :blk std.fs.cwd().openFile(ctx.path, .{}) catch |err| {
            ctx.std_err.print("failed to open file at {s}, err: {any}", .{ctx.path, err}) catch {};
            return;
        };
    };
    defer source_file.close();
    var file_end_pos = source_file.getEndPos() catch |err| {
        ctx.std_err.print("failed to get file end pos {s}, err: {any}", .{ctx.path, err}) catch {};
        return;
    };
    var file_pos = source_file.getPos() catch |err| {
        ctx.std_err.print("failed to get file pos {s}, err: {any}", .{ctx.path, err}) catch {};
        return;
    };
    
    while (file_pos < file_end_pos) {
        var gzip_stream = std.compress.gzip.gzipStream(ctx.allocator, source_file.reader()) catch |err| {
            ctx.std_err.print("failed to init gzip stream at {s}, err: {any}", .{ctx.path, err}) catch {};
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
        var bytes_in_use = bytes_read;
        var needle_search_start: usize = 0;
        header_search: while (std.mem.indexOf(u8, file_read_buffer.items[needle_search_start..bytes_in_use], needle)) |index| {
            if (index == 0) {
                break :header_search;
            }
            const SearchState = enum {
                IP,
                Port
            };
            var state = SearchState.IP;

            // skip "WARC-IP-Address: " from port search
            const ip_start_index = index + needle.len;
            var i: usize = 0;
            port_search: while(file_read_buffer.items[ip_start_index+i] != '\n' and file_read_buffer.items[ip_start_index+i] != '\r') : (i += 1) {
                if (file_read_buffer.items[ip_start_index+i] == ':') {
                    state = SearchState.Port;
                    break :port_search;
                }
            }
            // replace port in buffer to whitespace if it exist
            if (state == SearchState.Port) {
                const port_start_index = ip_start_index + i;
                // find range between port begining and newline character
                const new_line_offset = std.mem.indexOf(u8, file_read_buffer.items[port_start_index..], "\n") orelse file_read_buffer.items.len;
                const carriage_return_offset = std.mem.indexOf(u8, file_read_buffer.items[port_start_index..], "\r") orelse file_read_buffer.items.len;
                const min_offset = std.math.min(new_line_offset, carriage_return_offset);

                // shift the whole buffer to replace port characters, a slow operation :(
                std.mem.copy(u8, file_read_buffer.items[port_start_index..], file_read_buffer.items[port_start_index+min_offset..]);

                // remove discared bytes from the program state
                file_read_buffer.items.len -= min_offset;
                bytes_in_use -= min_offset; 
            } 
            needle_search_start = ip_start_index + i + 1;
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
            ctx.std_err.print("failed to get file pos {s}, err: {any}", .{ctx.path, err}) catch {};
            return;
        };
    }
}
