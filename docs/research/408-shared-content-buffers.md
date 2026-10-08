# Shared content buffers for ACP attachments

Research for #408, checked 2026-10-08.

Palantir's generated Conjure Rust `BinaryExample` stores binary data as
`conjure_object::Bytes` and returns a borrowed reference from its accessor
([generated source](https://github.com/palantir/conjure-rust/blob/master/conjure-codegen/src/example_types/objects/product/binary_example.rs)).
`conjure-object` reexports `bytes::Bytes`
([source](https://github.com/palantir/conjure-rust/blob/master/conjure-object/src/lib.rs)).
This is evidence for its public Rust representation, not a claim about
Foundry's internal attachment storage or retention policy.

`Bytes` shares immutable backing storage across cloned handles
([official documentation](https://docs.rs/bytes/1.12.1/bytes/struct.Bytes.html)).
Its `From<Vec<u8>>` implementation transfers ownership of the original buffer
([versioned source](https://docs.rs/bytes/1.12.1/src/bytes/bytes.rs.html)).
Shikigami therefore converts decoded attachments once, stores shared handles,
and returns shared binary payloads from its resolver. HTTP encoding borrows
the resolved slice. Pointer-identity coverage checks that staging, storage,
and repeated resolution retain the original decoded allocation.

The canonical sekai-client protobuf payload still requires `Vec<u8>`, so the
adapter materializes an owned buffer at that transport boundary. This change
does not alter the upstream wire contract or claim zero-copy serialization.
Session storage retains one allocation for follow-up turns and permission
resumption; dropping it after the first call would violate those requirements.
Base64 data URL caching is separate work in #413.
