//! Tenant names that stress object-key escaping, shared by key-layout tests.

/// Named tenant strings that a key encoder must keep inside one path segment:
/// separators, traversal, glob and quoting characters, and non-ASCII.
pub(crate) struct AwkwardTenant {
    pub name: &'static str,
    pub tenant: &'static str,
}

pub(crate) const AWKWARD_TENANTS: [AwkwardTenant; 13] = [
    AwkwardTenant {
        name: "plain",
        tenant: "tenant-a",
    },
    AwkwardTenant {
        name: "separator",
        tenant: "a/b",
    },
    AwkwardTenant {
        name: "relative",
        tenant: "..",
    },
    AwkwardTenant {
        name: "current",
        tenant: ".",
    },
    AwkwardTenant {
        name: "traversal",
        tenant: "../../etc",
    },
    AwkwardTenant {
        name: "absolute",
        tenant: "/etc/passwd",
    },
    AwkwardTenant {
        name: "space",
        tenant: "a b",
    },
    AwkwardTenant {
        name: "star",
        tenant: "a*b",
    },
    AwkwardTenant {
        name: "marker",
        tenant: "a!b",
    },
    AwkwardTenant {
        name: "quote",
        tenant: "a'b",
    },
    AwkwardTenant {
        name: "brackets",
        tenant: "(a)",
    },
    AwkwardTenant {
        name: "backslash",
        tenant: "a\\b",
    },
    AwkwardTenant {
        name: "non ASCII",
        tenant: "\u{e9}",
    },
];
