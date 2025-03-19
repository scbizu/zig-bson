//!
//! All RawBson types provide implementions of [Canonical Extended JSON formats](https://github.com/mongodb/specifications/blob/master/source/extended-json.md). When
//! using std.json, these implementation will go into effect
//!
const std = @import("std");
const Owned = @import("root.zig").Owned;

/// represents a unique identifier.
///
/// consists of 12 bytes
/// * A 4-byte timestamp, representing the ObjectId's creation, measured in seconds since the Unix epoch, accessible via the `timestamp()` method
/// * A 5-byte random value generated once per process. This random value is unique to the machine and process.
/// * A 3-byte incrementing counter, initialized to a random value.
/// https://www.mongodb.com/docs/manual/reference/bson-types/#objectid
pub const objectId = struct {
    bytes: [12]u8,

    fn dupe(self: @This()) @This() {
        var copy: [12]u8 = undefined;
        @memcpy(&copy, &self.bytes);
        return .{ .bytes = copy };
    }

    /// Gets the timestamp (number of seconds since the Unix epoch).
    pub fn timestamp(self: @This()) i32 {
        return std.mem.readInt(i32, self.bytes[0..4], .little);
    }

    // todo add timestamp based generator fn

    pub fn fromBytes(bytes: [12]u8) @This() {
        return .{ .bytes = bytes };
    }

    /// this may return an error if encoded is not actually hex formatted
    pub fn fromHex(encoded: []const u8) !@This() {
        var bytes: [12]u8 = undefined;
        _ = try std.fmt.hexToBytes(&bytes, encoded);
        return fromBytes(bytes);
    }

    pub fn jsonStringify(self: @This(), out: anytype) !void {
        try out.print(
            \\{{"$oid":"{s}"}}
        ,
            .{std.fmt.bytesToHex(self.bytes, .lower)},
        );
    }
};

test "objectId" {
    const allocator = std.testing.allocator;
    const objId = try objectId.fromHex("507f191e810c19729de860ea");
    try std.testing.expectEqual(504987472, objId.timestamp());
    const json = try std.json.stringifyAlloc(
        allocator,
        objId,
        .{},
    );
    defer allocator.free(json);
    try std.testing.expectEqualStrings(
        \\{"$oid":"507f191e810c19729de860ea"}
    , json);
}

pub const datetime = struct {
    millis: i64,

    pub fn dupe(self: @This()) @This() {
        return fromMillis(self.millis);
    }

    pub fn fromMillis(millis: i64) @This() {
        return .{ .millis = millis };
    }
    pub fn jsonStringify(self: @This(), out: anytype) !void {
        try out.print(
            \\{{"$date":{{"$numberLong":"{d}"}}}}
        ,
            .{self.millis},
        );
    }
};

test datetime {
    const allocator = std.testing.allocator;
    const json = try std.json.stringifyAlloc(
        allocator,
        datetime.fromMillis(1716919531350804),
        .{},
    );
    defer allocator.free(json);
    try std.testing.expectEqualStrings(
        \\{"$date":{"$numberLong":"1716919531350804"}}
    , json);
}

pub const regex = struct {
    pattern: []const u8,
    options: []const u8,

    fn dupe(self: @This(), allocator: std.mem.Allocator) !@This() {
        return .{
            .pattern = try allocator.dupe(u8, self.pattern),
            .options = try allocator.dupe(u8, self.options),
        };
    }

    pub fn jsonStringify(self: @This(), out: anytype) !void {
        try out.beginObject();
        try out.objectField("$regularExpression");

        try out.beginObject();
        try out.objectField("pattern");
        try out.write(self.pattern);
        try out.objectField("options");
        try out.write(self.options);
        try out.endObject();

        try out.endObject();
    }
};

pub const timestamp = struct {
    increment: u32,
    timestamp: u32,

    fn dupe(self: @This()) @This() {
        return .{ .increment = self.increment, .timestamp = self.timestamp };
    }

    pub fn init(increment: u32, ts: u32) @This() {
        return .{ .increment = increment, .timestamp = ts };
    }

    pub fn jsonStringify(self: @This(), out: anytype) !void {
        try out.print(
            \\{{"$timestamp":{{"t":{d},"i":{d}}}}}
        ,
            .{ self.timestamp, self.increment },
        );
    }
};

pub const minKey = struct {
    pub fn jsonStringify(_: @This(), out: anytype) !void {
        try out.print(
            \\{{"$minKey":1}}
        , .{});
    }
};

test "minKey.jsonStringify" {
    const allocator = std.testing.allocator;
    const json = try std.json.stringifyAlloc(
        allocator,
        minKey{},
        .{},
    );
    defer allocator.free(json);
    try std.testing.expectEqualStrings(
        \\{"$minKey":1}
    , json);
}

pub const maxKey = struct {
    pub fn jsonStringify(_: @This(), out: anytype) !void {
        try out.print(
            \\{{"$maxKey":1}}
        , .{});
    }
};

test "maxKey.jsonStringify" {
    const allocator = std.testing.allocator;
    const json = try std.json.stringifyAlloc(
        allocator,
        maxKey{},
        .{},
    );
    defer allocator.free(json);
    try std.testing.expectEqualStrings(
        \\{"$maxKey":1}
    , json);
}

pub const document = struct {
    pub const Element = struct { name: []const u8, value: RawBson };
    elements: []const Element,

    pub fn init(elements: []const Element) @This() {
        return .{ .elements = elements };
    }

    pub fn get(self: @This(), name: []const u8) ?RawBson {
        for (self.elements) |e| {
            if (std.mem.eql(u8, name, e.name)) {
                return e.value;
            }
        }
        return null;
    }

    pub fn jsonStringify(self: @This(), out: anytype) !void {
        try out.beginObject();

        for (self.elements) |elem| {
            try out.objectField(elem.name);
            try out.write(elem.value);
        }

        try out.endObject();
    }

    // return a copy of this instance
    pub fn dupe(self: @This(), allocator: std.mem.Allocator) !@This() {
        var elems = try allocator.alloc(Element, self.elements.len);
        for (self.elements, 0..) |e, i| elems[i] = .{ .name = try allocator.dupe(u8, e.name), .value = try e.value.dupe(allocator) };
        return document.init(elems);
    }
};

pub const int64 = struct {
    value: i64,

    fn dupe(self: @This()) @This() {
        return .{ .value = self.value };
    }

    pub fn jsonStringify(self: @This(), out: anytype) !void {
        try out.print(
            \\{{"$numberLong":"{d}"}}
        , .{self.value});
    }
};

/// https://github.com/mongodb/specifications/blob/master/source/bson-decimal128/decimal128.md
pub const decimal128 = struct {
    value: [16]u8,

    fn dupe(self: @This()) @This() {
        var copy: [16]u8 = undefined;
        @memcpy(&copy, &self.value);
        return .{ .value = copy };
    }

    fn toF128(self: @This()) f128 {
        return @bitCast(self.value);
    }
    pub fn jsonStringify(self: @This(), out: anytype) !void {
        // todo: handle NaN
        try out.print(
            \\{{"$numberDecimal":"{d}"}}
        , .{self.toF128()});
    }
};

pub const int32 = struct {
    value: i32,

    fn dupe(self: @This()) @This() {
        return .{ .value = self.value };
    }

    pub fn jsonStringify(self: @This(), out: anytype) !void {
        try out.print(
            \\{{"$numberInt":"{d}"}}
        , .{self.value});
    }
};

pub const double = struct {
    value: f64,
    pub fn init(value: f64) @This() {
        return .{ .value = value };
    }
    pub fn jsonStringify(self: @This(), out: anytype) !void {
        if (std.math.isNan(self.value)) {
            try out.print(
                \\{{"$numberDouble":"NaN"}}
            , .{});
        } else if (std.math.isPositiveInf(self.value)) {
            try out.print(
                \\{{"$numberDouble":"Infinity"}}
            , .{});
        } else if (std.math.isNegativeInf(self.value)) {
            try out.print(
                \\{{"$numberDouble":"-Infinity"}}
            , .{});
            // fixme: is there a better way to detect no decimal places?
        } else if (@round(self.value) == self.value) {
            try out.print(
                \\{{"$numberDouble":"{d:.1}"}}
            , .{self.value});
        } else {
            try out.print(
                \\{{"$numberDouble":"{d}"}}
            , .{self.value});
        }
    }
};

pub const javaScript = struct {
    value: []const u8,

    pub fn init(value: []const u8) @This() {
        return .{ .value = value };
    }

    fn dupe(self: @This(), allocator: std.mem.Allocator) !@This() {
        return javaScript.init(try allocator.dupe(u8, self.value));
    }

    pub fn jsonStringify(self: @This(), out: anytype) !void {
        try out.beginObject();
        try out.objectField("$code");
        try out.write(self.value);
        try out.endObject();
    }
};

pub const javaScriptWithScope = struct {
    value: []const u8,
    scope: document,

    fn dupe(self: @This(), allocator: std.mem.Allocator) !@This() {
        return .{
            .value = try allocator.dupe(u8, self.value),
            .scope = try self.scope.dupe(allocator),
        };
    }

    pub fn jsonStringify(self: @This(), out: anytype) !void {
        try out.beginObject();
        try out.objectField("$code");
        try out.write(self.value);
        try out.objectField("$scope");
        try out.write(self.scope);
        try out.endObject();
    }
};

pub const DBPointer = struct {
    ref: []const u8,
    id: objectId,

    pub fn init(ref: []const u8, id: objectId) @This() {
        return .{ .ref = ref, .id = id };
    }

    fn dupe(self: @This(), allocator: std.mem.Allocator) !@This() {
        return DBPointer.init(try allocator.dupe(u8, self.ref), self.id.dupe());
    }

    pub fn jsonStringify(self: @This(), out: anytype) !void {
        try out.beginObject();

        try out.objectField("$dbPointer");

        try out.beginObject();
        try out.objectField("$ref");
        try out.write(self.ref);
        try out.objectField("$id");
        try out.write(self.id);
        try out.endObject();

        try out.endObject();
    }
};

pub const symbol = struct {
    value: []const u8,

    fn dupe(self: @This(), allocator: std.mem.Allocator) !@This() {
        return .{ .value = try allocator.dupe(u8, self.value) };
    }

    pub fn jsonStringify(self: @This(), out: anytype) !void {
        try out.beginObject();
        try out.objectField("$symbol");
        try out.write(self.value);
        try out.endObject();
    }
};

pub const binary = struct {
    value: []const u8,
    subtype: SubType,

    pub fn init(value: []const u8, subtype: SubType) @This() {
        return .{ .value = value, .subtype = subtype };
    }

    fn dupe(self: @This(), allocator: std.mem.Allocator) !@This() {
        return .{ .value = try allocator.dupe(u8, self.value), .subtype = self.subtype };
    }

    pub fn jsonStringify(self: @This(), out: anytype) !void {
        try out.beginObject();
        try out.objectField("$binary");

        try out.beginObject();
        try out.objectField("base64");

        // note: because we only know the len of value at runtime, we can't statically allocate
        // an array and because we're in a place we don't have an allocator, we create one on demand
        var gpa = std.heap.GeneralPurposeAllocator(.{}){};
        defer _ = gpa.deinit();
        const allocator = gpa.allocator();
        const encoder = std.base64.standard.Encoder;
        const dest = try allocator.alloc(u8, encoder.calcSize(self.value.len));
        defer allocator.free(dest);

        try out.write(encoder.encode(dest, self.value));
        try out.objectField("subType");
        try out.write(self.subtype.hex());
        try out.endObject();

        try out.endObject();
    }
};

/// An enumeration of Bson types
pub const RawBson = union(enum) {
    double: double,
    string: []const u8,
    document: document,
    array: []const RawBson,
    boolean: bool,
    null: void,
    regex: regex,
    dbPointer: DBPointer,
    javascript: javaScript,
    javascript_with_scope: javaScriptWithScope,
    int32: int32,
    int64: int64,
    decimal128: decimal128,
    timestamp: timestamp,
    binary: binary,
    object_id: objectId,
    datetime: datetime,
    symbol: symbol,
    undefined: void,
    max_key: maxKey,
    min_key: minKey,

    pub fn dupe(self: @This(), allocator: std.mem.Allocator) error{OutOfMemory}!@This() {
        return switch (self) {
            .double => |v| .{ .double = double.init(v.value) },
            .string => |v| String(try allocator.dupe(u8, v)),
            .document => |v| .{ .document = try v.dupe(allocator) },
            .array => |v| blk: {
                var copy = try allocator.alloc(RawBson, v.len);
                for (v, 0..) |elem, i| copy[i] = try elem.dupe(allocator);
                break :blk Array(copy);
            },
            .boolean => |v| Boolean(v),
            .null => Null(),
            .regex => |v| .{ .regex = try v.dupe(allocator) },
            .dbPointer => |v| .{ .dbPointer = try v.dupe(allocator) },
            .javascript => |v| .{ .javascript = try v.dupe(allocator) },
            .javascript_with_scope => |v| .{ .javascript_with_scope = try v.dupe(allocator) },
            .int32 => |v| .{ .int32 = v.dupe() },
            .int64 => |v| .{ .int64 = v.dupe() },
            .decimal128 => |v| .{ .decimal128 = v.dupe() },
            .timestamp => |v| .{ .timestamp = v.dupe() },
            .binary => |v| .{ .binary = try v.dupe(allocator) },
            .object_id => |v| .{ .object_id = v.dupe() },
            .datetime => |v| .{ .datetime = v.dupe() },
            .symbol => |v| .{ .symbol = try v.dupe(allocator) },
            .undefined => RawBson.Undefined(),
            .max_key => RawBson.MaxKey(),
            .min_key => RawBson.MinKey(),
        };
    }

    /// convenience method for creating a new RawBson string
    pub fn String(value: []const u8) @This() {
        return .{ .string = value };
    }

    /// convenience method for creating a new RawBson double
    pub fn Double(value: f64) @This() {
        return .{ .double = double.init(value) };
    }

    /// convenience method for creating a new RawBson decimal 128
    pub fn Decimal128(bytes: [16]u8) @This() {
        return .{ .decimal128 = .{ .value = bytes } };
    }

    /// convenience method for creating a new RawBson boolean
    pub fn Boolean(value: bool) @This() {
        return .{ .boolean = value };
    }

    /// convenience method for creating a new RawBson document
    pub fn Document(elements: []const document.Element) @This() {
        return .{ .document = document.init(elements) };
    }

    /// convenience method for creating a new RawBson array
    pub fn Array(elements: []const RawBson) @This() {
        return .{ .array = elements };
    }

    /// convenience method for creating a new RawBson null
    pub fn Null() @This() {
        return .{ .null = {} };
    }

    /// convenience method for creating a new RawBson undefined
    pub fn Undefined() @This() {
        return .{ .undefined = {} };
    }

    /// convenience method for creating a new RawBson min key
    pub fn MinKey() @This() {
        return .{ .min_key = .{} };
    }

    /// convenience method for creating a new RawBson max key
    pub fn MaxKey() @This() {
        return .{ .max_key = .{} };
    }

    /// convenience method for creating a new RawBson int64
    pub fn Int64(value: i64) @This() {
        return .{ .int64 = .{ .value = value } };
    }

    /// convenience method for creating a new RawBson int32
    pub fn Int32(value: i32) @This() {
        return .{ .int32 = .{ .value = value } };
    }

    /// convenience method for creating a new RawBson symbol
    pub fn Symbol(value: []const u8) @This() {
        return .{ .symbol = .{ .value = value } };
    }

    /// convenience method for creating a new RawBson regex
    pub fn Regex(pattern: []const u8, options: []const u8) @This() {
        return .{ .regex = .{ .pattern = pattern, .options = options } };
    }

    /// convenience method for creating a new RawBson timestamp
    pub fn Timestamp(increment: u32, ts: u32) @This() {
        return .{ .timestamp = timestamp.init(increment, ts) };
    }

    /// convenience method for creating a new RawBson javaScript
    pub fn JavaScript(value: []const u8) @This() {
        return .{ .javascript = javaScript.init(value) };
    }

    /// convenience method for creating a new RawBson JavaScript (with scope)
    pub fn JavaScriptWithScope(value: []const u8, scope: document) @This() {
        return .{ .javascript_with_scope = .{ .value = value, .scope = scope } };
    }

    /// convenience method for creating a new RawBson ObjectId
    pub fn ObjectId(bytes: [12]u8) @This() {
        return .{ .object_id = objectId.fromBytes(bytes) };
    }

    /// convenience method for creating a new RawBson object id
    pub fn ObjectIdHex(encoded: []const u8) !@This() {
        return .{ .object_id = try objectId.fromHex(encoded) };
    }

    /// convenience method for creating a new RawBson datetime from millis since the epoch
    pub fn Datetime(millis: i64) @This() {
        return .{ .datetime = datetime.fromMillis(millis) };
    }

    /// convenience method for creating a new RawBson binary
    pub fn Binary(bytes: []const u8, st: SubType) @This() {
        return .{ .binary = binary.init(bytes, st) };
    }

    /// Derives a RawBson type from native zig types. Typically callers pass in a struct
    /// and get back a RawBson document.
    ///
    /// When provided those types embed a RawBson
    /// values, i.e `ObjectIds` they will be returned as is in the derived value
    pub fn from(allocator: std.mem.Allocator, data: anytype) !Owned(@This()) {
        var owned = Owned(@This()){
            .arena = try allocator.create(std.heap.ArenaAllocator),
            .value = undefined,
        };
        owned.arena.* = std.heap.ArenaAllocator.init(allocator);
        // tidy up if an error happens below
        errdefer {
            owned.arena.deinit();
            allocator.destroy(owned.arena);
        }
        const dataType = @TypeOf(data);

        // if the provided value already is a raw bson or similar native bson type defined above, simply return it
        switch (dataType) {
            RawBson => {
                owned.value = data;
                return owned;
            },
            document => {
                owned.value = RawBson{ .document = data };
                return owned;
            },
            regex => {
                owned.value = RawBson{ .regex = data };
                return owned;
            },
            decimal128 => {
                owned.value = RawBson{ .decimal128 = data };
                return owned;
            },
            timestamp => {
                owned.value = RawBson{ .timestamp = data };
                return owned;
            },
            binary => {
                owned.value = RawBson{ .binary = data };
                return owned;
            },
            objectId => {
                owned.value = RawBson{ .object_id = data };
                return owned;
            },
            datetime => {
                owned.value = RawBson{ .datetime = data };
                return owned;
            },
            else => {},
        }

        const info = @typeInfo(dataType);
        owned.value = switch (info) {
            .@"struct" => |v| blk: {
                var fields = try owned.arena.allocator().alloc(document.Element, v.fields.len);
                inline for (v.fields, 0..) |field, i| {
                    // pass along this arena's allocator
                    fields[i] = .{
                        .name = field.name,
                        .value = (try from(owned.arena.allocator(), @field(data, field.name))).value,
                    };
                }
                break :blk RawBson.Document(fields);
            },
            .optional => blk: {
                if (data) |d| {
                    break :blk (try from(owned.arena.allocator(), d)).value;
                } else {
                    break :blk RawBson.Null();
                }
            },
            .bool => RawBson.Boolean(data),
            .@"enum" => RawBson.String(@tagName(data)),
            .int => |v| blk: {
                if (v.signedness == .unsigned) {
                    std.debug.print("unsigned integers not yet supported\n", .{});
                    return error.UnsupportedType;
                }
                switch (v.bits) {
                    0...32 => break :blk RawBson.Int32(@intCast(data)),
                    33...64 => break :blk RawBson.Int64(@intCast(data)),
                    else => |otherwise| {
                        std.debug.print("{d} width ints not yet supported\n", .{otherwise});
                        return error.UnsupportedType;
                    },
                }
            },
            .comptime_int => RawBson.Int32(data),
            .comptime_float => RawBson.Double(data),
            .float => |v| blk: {
                switch (v.bits) {
                    1...63 => break :blk RawBson.Double(@floatCast(data)),
                    64 => break :blk RawBson.Double(data),
                    else => |otherwise| {
                        std.debug.print("{d} width floats not yet supported\n", .{otherwise});
                        return error.UnsupportedType;
                    },
                }
            },
            .array => |v| blk: {
                // if array of u8, assume a string
                if (v.child == u8) {
                    break :blk RawBson.String(try owned.arena.allocator().dupe(u8, &data));
                }
                var elements = try owned.arena.allocator().alloc(RawBson, v.len);
                for (data, 0..) |elem, i| {
                    elements[i] = (try from(owned.arena.allocator(), elem)).value;
                }
                break :blk RawBson.Array(elements);
            },
            .pointer => |v| blk: {
                switch (v.size) {
                    //*[]u8 { ... }
                    .slice => {
                        if (v.child == u8) {
                            break :blk RawBson.String(try owned.arena.allocator().dupe(u8, data));
                        }
                        var elements = try std.ArrayList(RawBson).init(owned.arena.allocator());
                        for (data) |elem| {
                            try elements.append((try from(owned.arena.allocator(), elem)).value);
                        }
                        break :blk RawBson.Array(try elements.toOwnedSlice());
                    },
                    .one => break :blk (try from(owned.arena.allocator(), data.*)).value,
                    else => |otherwise| {
                        std.debug.print("{any} pointer types not yet supported\n", .{otherwise});
                        return error.UnsupportedType;
                    },
                }
            },
            // todo: many other types...
            else => |otherwise| {
                std.debug.print("{any} types not yet supported\n", .{otherwise});
                return error.UnsupportedType;
            },
        };
        return owned;
    }

    /// Derives a value of a requested type `T` from a RawBson value. Typically callers request in a struct
    /// type
    ///
    /// When requesting a struct whose fields are not yet represented an IncompatibleBsonType is is returned.
    ///
    /// When requesting a struct whose fields are supported by the underlying RawBson value type doesn't align, an IncompatibleBsonType error is returned
    ///
    /// When a requested struct whose field value is not present in a RawBson document, struct field defaults are supported and recommended
    pub fn into(self: @This(), allocator: std.mem.Allocator, comptime T: type) !Owned(T) {
        var owned = Owned(T){
            .arena = try allocator.create(std.heap.ArenaAllocator),
            .value = undefined,
        };
        owned.arena.* = std.heap.ArenaAllocator.init(allocator);
        // tidy up if an error happens below
        errdefer {
            owned.arena.deinit();
            allocator.destroy(owned.arena);
        }

        // if the provided value already is a raw bson or similar native bson type defined above, simply return it
        switch (T) {
            RawBson => {
                owned.value = try self.dupe(owned.arena.allocator());
                return owned;
            },
            document => switch (self) {
                .document => |v| {
                    owned.value = try v.dupe(owned.arena.allocator());
                    return owned;
                },
                else => return error.IncompatibleBsonType,
            },
            regex => switch (self) {
                .regex => |v| {
                    owned.value = try v.dupe(owned.arena.allocator());
                    return owned;
                },
                else => return error.IncompatibleBsonType,
            },
            decimal128 => switch (self) {
                .decimal128 => |v| {
                    owned.value = try v.dupe(owned.arena.allocator());
                    return owned;
                },
                else => return error.IncompatibleBsonType,
            },
            timestamp => switch (self) {
                .timestamp => |v| {
                    owned.value = try v.dupe(owned.arena.allocator());
                    return owned;
                },
                else => return error.IncompatibleBsonType,
            },
            binary => switch (self) {
                .binary => |v| {
                    owned.value = try v.dupe(owned.arena.allocator());
                    return owned;
                },
                else => return error.IncompatibleBsonType,
            },
            objectId => switch (self) {
                .object_id => |v| {
                    owned.value = v.dupe();
                    return owned;
                },
                else => return error.IncompatibleBsonType,
            },
            datetime => switch (self) {
                .datetime => |v| {
                    owned.value = v.dupe();
                    return owned;
                },
                else => return error.IncompatibleBsonType,
            },
            else => {},
        }

        owned.value = switch (@typeInfo(T)) {
            .@"struct" => |v| blk: {
                switch (self) {
                    .document => |doc| {
                        var parsed: T = undefined;
                        inline for (v.fields) |field| {
                            if (doc.get(field.name)) |value| {
                                const ftype = switch (@typeInfo(field.type)) {
                                    .optional => |o| o.child,
                                    else => field.type,
                                };
                                @field(parsed, field.name) = (try value.into(owned.arena.allocator(), ftype)).value;
                            } else if (field.defaultValue()) |default| {
                                const dvalue_aligned: *align(field.alignment) const anyopaque = @alignCast(default);
                                @field(parsed, field.name) = @as(*const field.type, @ptrCast(dvalue_aligned)).*;
                            } else if (@typeInfo(field.type) == .optional) {
                                @field(parsed, field.name) = null;
                            } else {
                                return error.UnresolvedValue;
                            }
                        }
                        break :blk parsed;
                    },
                    else => return error.IncompatibleBsonType,
                }
            },
            .bool => blk: {
                switch (self) {
                    .boolean => |b| break :blk b,
                    else => return error.IncompatibleBsonType,
                }
            },
            .@"enum" => |v| blk: {
                switch (self) {
                    .string => |s| {
                        inline for (v.fields, 0..) |field, i| {
                            if (std.mem.eql(u8, field.name, s)) {
                                break :blk @enumFromInt(i);
                            }
                        }
                        return error.UnsupportedEnumVariant;
                    },
                    else => return error.IncompatibleBsonType,
                }
            },
            .comptime_int => blk: {
                switch (self) {
                    .int32 => |v| break :blk v.value,
                    else => return error.IncompatibleBsonType,
                }
            },
            .int => |v| blk: {
                if (v.signedness == .unsigned) {
                    std.debug.print("unsigned integers not yet supported\n", .{});
                    break :blk error.UnsupportedType;
                }
                switch (v.bits) {
                    0...32 => {
                        switch (self) {
                            .int32 => |i| break :blk @intCast(i.value),
                            else => return error.IncompatibleBsonType,
                        }
                    },
                    33...64 => {
                        switch (self) {
                            .int64 => |i| break :blk @intCast(i.value),
                            else => return error.IncompatibleBsonType,
                        }
                    },
                    else => |otherwise| {
                        std.debug.print("{d} width ints not yet supported\n", .{otherwise});
                        break :blk error.UnsupportedType;
                    },
                }
            },
            .comptime_float => blk: {
                switch (self) {
                    .double => |d| break :blk @floatCast(d.value),
                    else => return error.IncompatibleBsonType,
                }
            },
            .float => |v| blk: {
                switch (v.bits) {
                    1...63 => {
                        switch (self) {
                            .double => |d| break :blk @floatCast(d.value),
                            else => return error.IncompatibleBsonType,
                        }
                    },
                    64 => {
                        switch (self) {
                            .double => |d| break :blk d.value,
                            else => return error.IncompatibleBsonType,
                        }
                    },
                    else => |otherwise| {
                        std.debug.print("{d} width floats not yet supported\n", .{otherwise});
                        break :blk error.UnsupportedType;
                    },
                }
            },
            .pointer => |v| blk: {
                switch (v.size) {
                    .slice => {
                        if (v.child == u8) {
                            switch (self) {
                                .string => |s| break :blk try owned.arena.allocator().dupe(u8, s),
                                else => return error.IncompatibleBsonType,
                            }
                        }
                        switch (self) {
                            .array => |a| {
                                // alloc an array to capture data
                                var elements = try std.ArrayList(v.child).initCapacity(owned.arena.allocator(), a.len);
                                elements.deinit();
                                for (a) |elem| {
                                    elements.appendAssumeCapacity((try elem.into(owned.arena.allocator(), v.child)).value);
                                }
                                break :blk try elements.toOwnedSlice();
                            },
                            else => return error.IncompatibleBsonType,
                        }
                    },
                    // .One => break :blk (try from(owned.arena.allocator(), data.*, options)).value,
                    else => |otherwise| {
                        std.debug.print("{any} pointer types not yet supported\n", .{otherwise});
                        break :blk error.UnsupportedType;
                    },
                }
            },
            else => |otherwise| {
                std.debug.print("unsupported type {any}\n", .{otherwise});
                return error.UnsupportedType;
            },
        };
        return owned;
    }

    pub fn toType(self: @This()) Type {
        return switch (self) {
            .double => .double,
            .string => .string,
            .document => .document,
            .array => .array,
            .boolean => .boolean,
            .null => .null,
            .regex => .regex,
            .dbPointer => .dbPointer,
            .javascript => .javascript,
            .javascript_with_scope => .javascript_with_scope,
            .int32 => .int32,
            .int64 => .int64,
            .decimal128 => .decimal128,
            .timestamp => .timestamp,
            .binary => .binary,
            .object_id => .object_id,
            .datetime => .datetime,
            .symbol => .symbol,
            .undefined => .undefined,
            .max_key => .max_key,
            .min_key => .min_key,
        };
    }

    pub fn jsonStringify(self: @This(), out: anytype) !void {
        return try switch (self) {
            .double => |v| out.write(v),
            .string => |v| out.write(v),
            .document => |v| out.write(v),
            .array => |v| out.write(v),
            .binary => |v| out.write(v),
            .undefined => out.print("{{\"$undefined\":true}}", .{}),
            .object_id => |v| out.write(v),
            .boolean => |v| out.write(v),
            .datetime => |v| out.write(v),
            .null => out.write(null),
            .regex => |v| out.write(v),
            .javascript => |v| out.write(v),
            .javascript_with_scope => |v| out.write(v),
            .dbPointer => |v| out.write(v),
            .symbol => |v| out.write(v),
            .int32 => |v| out.write(v),
            .timestamp => |v| out.write(v),
            .int64 => |v| out.write(v),
            .decimal128 => |v| out.write(v),
            .min_key => |v| out.write(v),
            .max_key => |v| out.write(v),
        };
    }

    pub fn format(
        self: @This(),
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        var jw = std.json.writeStream(writer, .{});
        defer jw.deinit();
        try jw.write(self);
    }

    pub fn deinit(self: @This(), allocator: std.mem.Allocator) void {
        switch (self) {
            .document => |v| {
                // for (v.elements) |elem| {
                //     elem.v.deinit(allocator);
                // }
                allocator.free(v.elements);
            },
            else => {},
        }
    }
};

test "RawBson.into" {
    const allocator = std.testing.allocator;
    const Enum = enum {
        boom,
        doom,
    };
    var doc = RawBson.Document(
        &.{
            .{ .name = "id", .value = try RawBson.ObjectIdHex("507f1f77bcf86cd799439011") },
            .{ .name = "str", .value = RawBson.String("bar") },
            .{ .name = "enu", .value = RawBson.String("boom") },
            .{ .name = "i32", .value = RawBson.Int32(1) },
            .{ .name = "i64", .value = RawBson.Int64(2) },
            .{ .name = "f64", .value = RawBson.Double(1.5) },
            .{ .name = "bool", .value = RawBson.Boolean(true) },
            .{ .name = "opt_present", .value = RawBson.Boolean(true) },
            .{ .name = "ary", .value = RawBson.Array(&.{ RawBson.Int32(1), RawBson.Int32(2), RawBson.Int32(3) }) },
            .{ .name = "doc", .value = RawBson.Document(
                &.{
                    .{ .name = "foo", .value = RawBson.String("bar") },
                },
            ) },
            .{ .name = "raw", .value = RawBson.String("raw") },
        },
    );
    const T = struct {
        id: objectId,
        str: []const u8,
        enu: Enum,
        i32: i32,
        i64: i64,
        f64: f64,
        bool: bool,
        opt: ?bool,
        opt_present: ?bool,
        ary: []const i32,
        doc: document,
        raw: RawBson,
    };
    var into = try doc.into(allocator, T);
    defer into.deinit();
    try std.testing.expectEqualDeep(T{
        .id = try objectId.fromHex("507f1f77bcf86cd799439011"),
        .str = "bar",
        .enu = .boom,
        .i32 = 1,
        .i64 = 2,
        .f64 = 1.5,
        .bool = true,
        .opt = null,
        .opt_present = true,
        .ary = &.{ 1, 2, 3 },
        .doc = document.init(
            &.{
                .{ .name = "foo", .value = RawBson.String("bar") },
            },
        ),
        .raw = RawBson.String("raw"),
    }, into.value);
}

test "RawBson.from" {
    const allocator = std.testing.allocator;
    const Enum = enum {
        a,
        b,
        c,
    };
    const opt: ?[]const u8 = null;
    const opt_present: ?[]const u8 = "opt_present";
    var doc = try RawBson.from(allocator, .{
        .person = .{
            .str = "test",
            .id = try objectId.fromHex("507f1f77bcf86cd799439011"),
            .opt = opt,
            .opt_present = opt_present,
            .comp_int = 1,
            .i16 = @as(i16, 2),
            .i32 = @as(i32, 2),
            .i64 = @as(i64, 3),
            .ary = [_]i32{ 4, 5, 6 },
            .slice = &[_]i32{ 1, 2, 3 },
            .bool = true,
            .comp_float = 3.2,
            .float32 = @as(f32, 3.2),
            .float64 = @as(f64, 3.2),
            .enu = Enum.a,
        },
    });
    defer doc.deinit();
    // std.debug.print("doc {s}\n", .{doc.value});
    try std.testing.expectEqualDeep(doc.value, RawBson.Document(&.{.{
        .name = "person",
        .value = RawBson.Document(&.{
            .{ .name = "str", .value = RawBson.String("test") },
            .{ .name = "id", .value = try RawBson.ObjectIdHex("507f1f77bcf86cd799439011") },
            .{ .name = "opt", .value = RawBson.Null() },
            .{ .name = "opt_present", .value = RawBson.String("opt_present") },
            .{ .name = "comp_int", .value = RawBson.Int32(1) },
            .{ .name = "i16", .value = RawBson.Int32(2) },
            .{ .name = "i32", .value = RawBson.Int32(2) },
            .{ .name = "i64", .value = RawBson.Int64(3) },
            .{ .name = "ary", .value = RawBson.Array(&[_]RawBson{ RawBson.Int32(4), RawBson.Int32(5), RawBson.Int32(6) }) },
            .{ .name = "slice", .value = RawBson.Array(&[_]RawBson{ RawBson.Int32(1), RawBson.Int32(2), RawBson.Int32(3) }) },
            .{ .name = "bool", .value = RawBson.Boolean(true) },
            .{ .name = "comp_float", .value = RawBson.Double(3.2) },
            .{ .name = "float32", .value = RawBson.Double(@floatCast(@as(f32, 3.2))) },
            .{ .name = "float64", .value = RawBson.Double(3.2) },
            .{ .name = "enu", .value = RawBson.String("a") },
        }),
    }}));
}

test "RawBson.jsonStringify" {
    const allocator = std.testing.allocator;
    const actual = try std.json.stringifyAlloc(allocator, RawBson{
        .min_key = minKey{},
    }, .{});
    defer allocator.free(actual);
    try std.testing.expectEqualStrings(
        \\{"$minKey":1}
    , actual);
}

pub const Type = enum(i8) {
    double = 0x01,
    string = 0x02,
    document = 0x03,
    /// the document for an array is a normal BSON document with integer values for the keys, starting with 0 and continuing sequentially. For example, the array ['red', 'blue'] would be encoded as the document {'0': 'red', '1': 'blue'}. The keys must be in ascending numerical order.
    array = 0x04,
    binary = 0x05,
    /// deprecated
    undefined = 0x06,
    object_id = 0x07,
    boolean = 0x08,
    /// The int64 is UTC milliseconds since the Unix epoch
    datetime = 0x09,
    null = 0x0a,
    regex = 0x0b,
    /// deprecated
    dbPointer = 0x0c,
    javascript = 0x0d,
    /// deprecated
    symbol = 0x0e,
    /// deprecated
    javascript_with_scope = 0x0f,
    int32 = 0x10,
    ///  Special internal type used by MongoDB replication and sharding. First 4 bytes are an increment, second 4 are a timestamp.
    timestamp = 0x11,
    int64 = 0x12,
    decimal128 = 0x13,
    /// Special type which compares lower than all other possible BSON element values.
    min_key = 0xff - 256,
    /// Special type which compares higher than all other possible BSON element values.
    max_key = 0x7f,

    pub fn fromInt(int: i8) @This() {
        return @enumFromInt(int);
    }

    pub fn toInt(self: @This()) i8 {
        return @intFromEnum(self);
    }

    pub fn format(
        self: @This(),
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        try writer.print("{s}", .{@tagName(self)});
    }
};

pub const SubType = enum(u8) {
    /// This is the most commonly used binary subtype and should be the 'default' for drivers and tools.
    binary = 0x00, // todo: rename to generic
    function = 0x01,
    binary_old = 0x02,
    uuid_old = 0x03,
    uuid = 0x04,
    md5 = 0x05,
    encrypted = 0x06,
    /// Compact storage of BSON data. This data type uses delta and delta-of-delta compression and run-length-encoding for efficient element storage. Also has an encoding for sparse arrays containing missing values.
    compact_column = 0x07,
    sensitve = 0x08,
    // 128 - 255
    user_defined = 0x80,

    pub fn fromInt(int: u8) @This() {
        return @enumFromInt(int);
    }

    pub fn toInt(self: @This()) u8 {
        return @intFromEnum(self);
    }

    fn hex(self: @This()) [2]u8 {
        return std.fmt.bytesToHex([_]u8{@intFromEnum(self)}, .lower);
    }

    pub fn format(
        self: @This(),
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        try writer.print("{s}", .{@tagName(self)});
    }
};
