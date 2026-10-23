# Resolved comments were readable across tenants

Listing comments with `include_resolved = true` skipped the object
visibility check, so a tenant could read another tenant's resolved comments.
Fixed by checking visibility before filtering by state.
